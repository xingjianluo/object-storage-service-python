"""存储服务门面：组合 blob 物理层与 catalog 元数据层。

并发策略（同一台机器上多进程共享同一数据目录 + 单进程多线程）：
- 互斥原语是数据目录内的 fcntl.flock 文件锁（见 locking.LockManager），
  不再只依赖单进程内的 threading 锁；
- 每个变更类操作（put/delete/rollback）按固定顺序加两把锁：
  先持“共享 gc 屏障锁”，再持“该对象的排他对象锁”：
  * 同一对象的所有变更在任意多个进程间严格串行 -> 版本号严格连续、
    不重不跳；一次成功返回后，其 blob 已落盘、meta 已原子发布，
    后续任何进程读到的该版本内容必然可校验，不会被悄悄覆盖
    （blob 内容寻址不可变，历史版本记录只追加不修改）；
  * 不同对象的对象锁互不阻塞，互不影响；
- gc() 持“排他 gc 屏障锁”：必须等所有在途变更结束才开始，期间新变更
  排队等待。因此 GC 看到的“引用集合 ∪ 磁盘集合”是一个无在途写入的
  稳定截面，绝不会删掉任何仍被历史版本引用的内容，也不会把别的进程
  “刚落 blob 还没发 meta”的在途内容误判成垃圾（GC 开始时那种状态
  不可能存在）；
- 读路径（get/stat/list）不持任何锁：blob 与 meta 文件均以
  “临时文件 + fsync + 原子 rename”发布，读者任一时刻拿到的都是
  某个完整版本，GC 也只会删除“任何版本记录都不引用”的 blob。

中断/重启恢复：
- 进程启动时在“排他 gc 屏障锁”保护下清空 staging 暂存目录与
  meta 半成品临时文件；持排他屏障意味着此刻没有任何在途变更，
  清掉的只能是崩溃者的残留，不会误伤运行中进程的文件；
- 锁由内核在进程死亡时自动释放，强杀不会留下死锁；锁文件是可复用
  空文件，不随崩溃累积；
- 先落 blob 并校验、后原子发布 meta；meta 未发布则 blob 不可达，
  不会产生“半成品对象”，这种孤儿 blob 只能由 gc() 显式回收；
- 老数据目录（没有 locks/ 目录）直接打开即可：锁目录按需创建，
  磁盘布局其余部分保持不变，无需人工迁移；
- 读到已引用 blob 若缺失/损坏，明确抛 ContentCorruptedError，不静默吞掉。
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .blobstore import BlobStore
from .catalog import Catalog, ObjectState, Version, now_ts
from .errors import (
    ContentCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    VersionNotFoundError,
)
from .locking import LockManager

logger = logging.getLogger("oss.service")


@dataclass(frozen=True, slots=True)
class PutResult:
    """写入结果：新版本 + 是否复用了已有物理副本（去重）。"""

    version: Version
    deduplicated: bool


class StorageService:
    """对外的对象存储与版本管理门面。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._blobs = BlobStore(self._root)
        self._catalog = Catalog(self._root)
        self._locks = LockManager(self._root)
        # 启动恢复：排他 gc 屏障保证没有别的进程在途变更，
        # 清掉的只会是被强杀进程留下的半成品。
        with self._locks.gc_lock():
            removed_staging = self._blobs.cleanup_staging()
            removed_meta_tmp = self._catalog.sweep_temp_files()
        if removed_staging or removed_meta_tmp:
            logger.warning(
                "启动恢复：清理 %d 个未发布暂存、%d 个元数据半成品",
                removed_staging, removed_meta_tmp,
            )
        logger.info("存储服务已就绪 root=%s pid=%d", self._root.resolve(),
                    os.getpid())

    @property
    def root(self) -> Path:
        return self._root

    @property
    def blob_store(self) -> BlobStore:
        return self._blobs

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    # ---- 写路径 ----
    def put_object(
        self,
        key: str,
        chunks: Iterable[bytes],
        expected_digest: str | None = None,
    ) -> PutResult:
        """写入（覆盖写）对象，返回新版本与去重标志。相同内容复用同一 blob。

        判定依据：内容实际 sha256（若给了 expected_digest 还需一致）。
        """
        with self._locks.mutation_barrier(key):
            stored = self._blobs.put(chunks, expected_digest=expected_digest)
            state = self._catalog.load(key) or ObjectState(key=key)
            version_no = state.next_version_no()
            ver = Version(
                version=version_no,
                digest=stored.digest,
                size=stored.size,
                created_at=now_ts(),
            )
            state.versions.append(ver)
            state.current = version_no
            self._catalog.store(state)
            logger.info(
                "PUT key=%r version=%d digest=%s size=%d deduplicated=%s 依据=内容sha256%s",
                key, version_no, stored.digest, stored.size,
                stored.deduplicated,
                "+客户端校验一致" if expected_digest else "",
            )
            return PutResult(version=ver, deduplicated=stored.deduplicated)

    def delete_object(self, key: str) -> Version:
        """逻辑删除：追加墓碑版本并令当前版本不可见。

        对象不存在时不凭空创建墓碑，直接抛 ObjectNotFoundError；
        对已删除对象重复删除属于幂等成功，返回当前墓碑版本。
        """
        with self._locks.mutation_barrier(key):
            state = self._catalog.load(key)
            if state is None:
                raise ObjectNotFoundError(f"对象不存在，无法删除: key={key!r}")
            visible = state.visible()
            if visible is None:
                # 已删除：幂等返回最近一个墓碑版本
                tombstone = next(
                    (v for v in reversed(state.versions) if v.deleted), None
                )
                logger.info("DELETE key=%r 幂等（已删除） version=%s", key,
                            tombstone.version if tombstone else None)
                if tombstone is not None:
                    return tombstone
            version_no = state.next_version_no()
            ver = Version(version=version_no, digest="", size=0,
                          created_at=now_ts(), deleted=True)
            state.versions.append(ver)
            state.current = None
            self._catalog.store(state)
            logger.info("DELETE key=%r tombstone_version=%d 依据=逻辑墓碑(blob不删)",
                        key, version_no)
            return ver

    def rollback(self, key: str, version: int) -> Version:
        """把当前可见指针切回某个历史（非墓碑）版本，不复制内容、不新建版本。

        目标版本必须存在且不是墓碑。目标即当前版本时为幂等成功。
        """
        with self._locks.mutation_barrier(key):
            state = self._catalog.load(key)
            if state is None:
                raise ObjectNotFoundError(f"对象不存在，无法回滚: key={key!r}")
            target = state.find(version)
            if target is None:
                raise VersionNotFoundError(
                    f"版本不存在: key={key!r} version={version}，"
                    f"现有版本={[v.version for v in state.versions]}"
                )
            if target.deleted:
                raise VersionNotFoundError(
                    f"不能回滚到墓碑版本: key={key!r} version={version}"
                )
            state.current = target.version
            self._catalog.store(state)
            logger.info(
                "ROLLBACK key=%r current_version=%d digest=%s 依据=复用历史版本引用(不复制blob)",
                key, target.version, target.digest,
            )
            return target

    # ---- 读路径 ----
    def _state_or_404(self, key: str) -> ObjectState:
        state = self._catalog.load(key)
        if state is None:
            raise ObjectNotFoundError(f"对象不存在: key={key!r}")
        return state

    def get_object(self, key: str, version: int | None = None) -> tuple[bytes, Version]:
        """读取内容；version=None 读当前版本。

        - 对象从不存在 -> ObjectNotFoundError
        - 当前无可见版本（已删除）-> ObjectDeletedError
        - 指定版本不存在 / 是墓碑 -> VersionNotFoundError
        - 底层 blob 缺失或哈希损坏 -> ContentCorruptedError
        """
        state = self._state_or_404(key)
        if version is None:
            ver = state.visible()
            if ver is None:
                raise ObjectDeletedError(
                    f"对象已删除，无可见版本: key={key!r}，"
                    f"历史版本={[v.version for v in state.versions]}"
                )
        else:
            ver = state.find(version)
            if ver is None:
                raise VersionNotFoundError(
                    f"版本不存在: key={key!r} version={version}，"
                    f"现有版本={[v.version for v in state.versions]}"
                )
            if ver.deleted:
                raise VersionNotFoundError(
                    f"该版本是删除墓碑，无内容: key={key!r} version={version}"
                )
        # 元数据引用了 blob；若物理缺失/损坏必须显式报错。
        # 读不持锁：meta 原子发布、blob 不可变，GC 在排他屏障下只删无引用者。
        data = self._blobs.get(ver.digest)
        logger.info(
            "GET key=%r version=%d digest=%s size=%d 判定=内容sha256校验通过",
            key, ver.version, ver.digest, ver.size,
        )
        return data, ver

    def stat(self, key: str) -> ObjectState:
        """返回对象元数据状态；不存在抛 ObjectNotFoundError。"""
        return self._state_or_404(key)

    def list_versions(self, key: str) -> list[Version]:
        """列出对象全部版本（含墓碑，按版本号升序）。"""
        state = self._state_or_404(key)
        return list(state.versions)

    # ---- 维护 ----
    def gc(self) -> list[str]:
        """删除无任何版本（含历史版本）引用的孤儿 blob，返回被回收的 digest。

        排他 gc 屏障：等待所有在途变更结束后方开始，期间阻塞新变更，
        因此引用集合与磁盘集合构成无在途写入的稳定截面，
        不会误删历史版本引用内容，也不会误删别人刚提交的在途 blob。
        """
        with self._locks.gc_lock():
            referenced = self._catalog.all_referenced_digests()
            on_disk = self._blobs.all_digests()
            garbage = sorted(on_disk - referenced)
            for digest in garbage:
                self._blobs.delete(digest)
            logger.info(
                "GC pid=%d 磁盘blob=%d 被引用=%d 回收=%d 依据=任一版本记录引用即存活",
                os.getpid(),
                len(on_disk), len(referenced), len(garbage),
            )
            return garbage

    def recover(self) -> tuple[int, int]:
        """在排他 gc 屏障下清理暂存/元数据半成品（构造时已自动执行）。

        返回 (清理的 blob 暂存数, 清理的 meta 半成品数)。
        """
        with self._locks.gc_lock():
            return self._blobs.cleanup_staging(), self._catalog.sweep_temp_files()


__all__ = ["StorageService", "PutResult"]

"""存储服务门面：组合 blob 物理层与 catalog 元数据层。

并发策略（单机多进程 + 进程内多线程）：
- 进程内：一把可重入互斥锁 self._rw 串行化本进程所有元数据访问；
- 跨进程：fcntl.flock 文件锁（见 locks.py）——
  * 每个逻辑对象一把排他锁：put/delete/rollback 持锁完成
    “读 meta -> 改 -> 原子发布”，因此同一对象的版本号严格 1..N 不重不跳，
    不同对象用不同锁文件，互不阻塞；
  * 一把 GC 读写锁：写路径（含 blob 落盘 + meta 发布全程）持共享锁，
    GC 持独占锁，保证 GC 标记-清除期间不存在“blob 已提交、meta 未发布”
    的中间态，刚提交的内容不会被误判为垃圾；
- flock 锁随进程死亡由内核自动释放，进程被 kill -9 不会留下死锁；
- 读路径只持进程内锁：meta 是原子发布的不可变快照，版本记录只增不改，
  被任一版本引用的 blob 永不失效，读到的一定是完整内容。

中断/重启恢复（多进程安全）：
- 构造时清理“属主进程已死亡 / 老版本遗留”的暂存半成品（blob tmp 与
  meta tmp 均按 PID 判定），不动其它存活进程正在写的文件；
- 先落 blob 并校验、后原子发布 meta；meta 未发布则 blob 不可达，
  不会产生“半成品对象”，孤儿 blob 由 gc() 回收；
- 读到已引用 blob 若缺失/损坏，明确抛 ContentCorruptedError，不静默吞掉。
"""

from __future__ import annotations

import logging
import os
import threading
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
from .locks import LockManager

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
        self._rw = threading.RLock()  # 进程内互斥；跨进程互斥由 flock 文件锁承担
        removed_tmp, removed_meta = self._recover_staging()
        if removed_tmp or removed_meta:
            logger.warning(
                "启动恢复：清理死亡进程遗留暂存 blob_tmp=%d meta_tmp=%d",
                removed_tmp, removed_meta,
            )
        logger.info("存储服务已就绪 root=%s pid=%d", self._root.resolve(), os.getpid())

    def _recover_staging(self) -> tuple[int, int]:
        """清理死亡进程遗留的暂存文件（不动存活进程的文件），返回两类清理数。"""
        return self._blobs.cleanup_staging(), self._catalog.cleanup_staging()

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

        锁序（全服务统一，避免死锁）：进程内锁 -> GC 共享锁 -> 对象排他锁。
        blob 落盘在对象锁之外（内容寻址、天然幂等），但必须在 GC 共享锁内，
        保证“blob 已提交、meta 未发布”的中间态与 GC 互斥。
        """
        with self._rw, self._locks.gc_lock(exclusive=False):
            stored = self._blobs.put(chunks, expected_digest=expected_digest)
            with self._locks.object_lock(key):
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
                "PUT pid=%d key=%r version=%d digest=%s size=%d deduplicated=%s 依据=内容sha256%s",
                os.getpid(), key, version_no, stored.digest, stored.size,
                stored.deduplicated,
                "+客户端校验一致" if expected_digest else "",
            )
            return PutResult(version=ver, deduplicated=stored.deduplicated)

    def delete_object(self, key: str) -> Version:
        """逻辑删除：追加墓碑版本并令当前版本不可见。

        对象不存在时不凭空创建墓碑，直接抛 ObjectNotFoundError；
        对已删除对象重复删除属于幂等成功，返回当前墓碑版本。
        """
        with self._rw, self._locks.object_lock(key):
            state = self._catalog.load(key)
            if state is None:
                raise ObjectNotFoundError(f"对象不存在，无法删除: key={key!r}")
            visible = state.visible()
            if visible is None:
                # 已删除：幂等返回最近一个墓碑版本
                tombstone = next(
                    (v for v in reversed(state.versions) if v.deleted), None
                )
                logger.info("DELETE pid=%d key=%r 幂等（已删除） version=%s",
                            os.getpid(), key,
                            tombstone.version if tombstone else None)
                if tombstone is not None:
                    return tombstone
            version_no = state.next_version_no()
            ver = Version(version=version_no, digest="", size=0,
                          created_at=now_ts(), deleted=True)
            state.versions.append(ver)
            state.current = None
            self._catalog.store(state)
            logger.info("DELETE pid=%d key=%r tombstone_version=%d 依据=逻辑墓碑(blob不删)",
                        os.getpid(), key, version_no)
            return ver

    def rollback(self, key: str, version: int) -> Version:
        """把当前可见指针切回某个历史（非墓碑）版本，不复制内容、不新建版本。

        目标版本必须存在且不是墓碑。目标即当前版本时为幂等成功。
        """
        with self._rw, self._locks.object_lock(key):
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
                "ROLLBACK pid=%d key=%r current_version=%d digest=%s 依据=复用历史版本引用(不复制blob)",
                os.getpid(), key, target.version, target.digest,
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
        with self._rw:
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
            data = self._blobs.get(ver.digest)
            logger.info(
                "GET pid=%d key=%r version=%d digest=%s size=%d 判定=内容sha256校验通过",
                os.getpid(), key, ver.version, ver.digest, ver.size,
            )
            return data, ver

    def stat(self, key: str) -> ObjectState:
        """返回对象元数据状态；不存在抛 ObjectNotFoundError。"""
        with self._rw:
            return self._state_or_404(key)

    def list_versions(self, key: str) -> list[Version]:
        """列出对象全部版本（含墓碑，按版本号升序）。"""
        with self._rw:
            state = self._state_or_404(key)
            return list(state.versions)

    # ---- 维护 ----
    def gc(self) -> list[str]:
        """删除无任何版本（含历史版本）引用的孤儿 blob，返回被回收的 digest。

        持 GC 独占锁：标记-清除全程与所有进程的写路径（GC 共享锁）互斥，
        因此不会把“刚提交 blob、meta 尚未发布”的内容误判为垃圾；
        被任一版本记录引用的 blob 一律存活。
        """
        with self._rw, self._locks.gc_lock(exclusive=True):
            referenced = self._catalog.all_referenced_digests()
            on_disk = self._blobs.all_digests()
            garbage = sorted(on_disk - referenced)
            for digest in garbage:
                self._blobs.delete(digest)
            logger.info(
                "GC pid=%d 磁盘blob=%d 被引用=%d 回收=%d 依据=任一版本记录引用即存活",
                os.getpid(), len(on_disk), len(referenced), len(garbage),
            )
            return garbage

    def recover(self) -> tuple[int, int]:
        """清理死亡进程遗留的暂存半成品（构造时已自动执行，也可显式调用）。

        返回 (blob 暂存清理数, meta 暂存清理数)；只清理属主进程已死亡或
        老版本遗留的文件，不影响其它存活进程。
        """
        with self._rw:
            return self._recover_staging()


__all__ = ["StorageService", "PutResult"]

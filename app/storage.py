"""逻辑对象服务。

组合内容寻址物理层 (:class:`~app.blob_store.BlobStore`) 与版本元数据层
(:class:`~app.metadata.MetadataStore`)，对外提供：

* 写入去重：物理层按 sha256 去重，逻辑层 append 新版本；
* 版本可见性：``current_version`` 即当前可见版本，删除是一条删除标记，
  覆盖写入/删除/回滚都通过 append 新记录实现，历史永不丢失；
* 引用规则：物理副本只要被任意一条版本记录引用就必须保留，
  其余为孤儿副本，可由 garbage_collect 安全回收；
* 一致性：同一逻辑对象的“读元数据-写物理副本-改元数据”全程持锁，
  物理层与元数据层均原子落盘；先落物理副本再改元数据，
  崩溃后至多留下不被引用的孤儿副本（可回收），绝不会读到半成品。
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import logging
import pathlib
import threading

from app.blob_store import BlobStore
from app.errors import (
    InvalidArgumentError,
    ObjectCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    VersionNotFoundError,
)
from app.metadata import MetadataStore, ObjectMetadata, VersionRecord

logger = logging.getLogger("object_storage")


def _utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _validate_key(key: str) -> None:
    if not isinstance(key, str) or not key:
        raise InvalidArgumentError("对象 key 必须是非空字符串")
    if "\x00" in key:
        raise InvalidArgumentError("对象 key 不能包含 NUL 字符")
    if len(key) > 1024:
        raise InvalidArgumentError("对象 key 长度不能超过 1024")


class _RWLock:
    """写者优先读写锁：对象操作持共享端（彼此仅按 key 锁再串行），
    GC 持排他端。

    写者优先：一旦有 GC 在等待，新的对象操作不再进入共享端，
    避免持续写入导致 GC 饥饿；同时保证 GC 扫描引用集合期间，
    没有“副本已写但引用未挂接”的窗口。
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._active_readers = 0
        self._waiting_writers = 0
        self._writer_active = False

    @contextlib.contextmanager
    def shared(self):
        with self._cond:
            # 有 GC 等待或正在执行时，对象操作在门外排队
            while self._waiting_writers > 0 or self._writer_active:
                self._cond.wait()
            self._active_readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._active_readers -= 1
                if self._active_readers == 0:
                    self._cond.notify_all()

    @contextlib.contextmanager
    def exclusive(self):
        with self._cond:
            self._waiting_writers += 1
            try:
                while self._active_readers > 0 or self._writer_active:
                    self._cond.wait()
                self._writer_active = True
            finally:
                self._waiting_writers -= 1
        try:
            yield
        finally:
            with self._cond:
                self._writer_active = False
                self._cond.notify_all()



class ObjectStorage:
    def __init__(self, root: str | pathlib.Path) -> None:
        self.root = pathlib.Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.blobs = BlobStore(self.root)
        self.meta = MetadataStore(self.root)
        self._gc_lock = _RWLock()
        self._recover_on_start()

    # -- 启动恢复 -------------------------------------------------------------
    def _recover_on_start(self) -> None:
        tmp_removed = self.blobs.recover()
        meta_removed = self.meta.recover()
        if tmp_removed or meta_removed:
            logger.info(
                "启动恢复: 清理物理暂存文件 %d 个 %s；清理元数据临时文件 %d 个 %s",
                len(tmp_removed), tmp_removed, len(meta_removed), meta_removed,
            )

    def recover(self) -> dict[str, list[str]]:
        """手动触发恢复：清理半成品暂存/临时文件。"""
        return {
            "blob_tmp": self.blobs.recover(),
            "meta_tmp": self.meta.recover(),
        }

    # -- 写入 ----------------------------------------------------------------
    def put_object(
        self,
        key: str,
        data: bytes,
        expected_sha256: str | None = None,
    ) -> VersionRecord:
        """覆盖写入：append 一条新版本，返回该版本记录。

        先在物理层原子落盘（失败则拒绝且不留痕），再持 key 锁追加元数据。
        """
        _validate_key(key)
        if not isinstance(data, bytes):
            raise InvalidArgumentError("写入内容必须是 bytes")

        # 共享锁必须覆盖“写物理副本 -> 挂接元数据引用”的全过程，
        # 否则 GC 可能在两者之间把尚无引用的新副本当作孤儿回收。
        with self._gc_lock.shared():
            digest = self.blobs.put(data, expected_sha256)
            with self.meta.lock(key):
                md = self.meta.load(key)
                if md is None:
                    md = ObjectMetadata(key=key, current_version=0, versions=[])
                # 幂等去重：若当前可见内容与本次完全相同，则不产生新版本，
                # 且绝不改动已有对象状态。
                cur = md.current
                if cur is not None and not cur.is_delete and cur.sha256 == digest:
                    logger.info(
                        "key=%s 写入去重命中: sha256=%s 复用版本 v%s，不新增版本",
                        key, digest, cur.version,
                    )
                    return cur
                next_ver = (
                    max((v.version for v in md.versions), default=0) + 1
                )
                rec = VersionRecord(
                    version=next_ver,
                    sha256=digest,
                    size=len(data),
                    operation="put",
                    timestamp=_utcnow_iso(),
                )
                md.versions.append(rec)
                md.current_version = next_ver
                self.meta.save(key, md)
                logger.info(
                    "key=%s 新版本 v%s: sha256=%s size=%s 判定依据=内容寻址物理层",
                    key, next_ver, digest, len(data),
                )
                return rec

    # -- 读取 ----------------------------------------------------------------
    def _resolve(
        self, key: str, version: int | None
    ) -> tuple[ObjectMetadata, VersionRecord]:
        _validate_key(key)
        md = self.meta.load(key)
        if md is None:
            raise ObjectNotFoundError(f"对象不存在: key={key}")
        if version is None:
            rec = md.current
            if rec is None:
                raise ObjectNotFoundError(
                    f"对象无任何版本: key={key}"
                )
        else:
            try:
                rec = md.get(version)
            except KeyError:
                raise VersionNotFoundError(
                    f"版本不存在: key={key} version=v{version}"
                ) from None
        return md, rec

    def head_object(self, key: str, version: int | None = None) -> VersionRecord:
        md, rec = self._resolve(key, version)
        if rec.is_delete:
            raise ObjectDeletedError(
                f"对象已删除，当前无可见内容: key={key} version=v{rec.version}"
            )
        logger.info(
            "key=%s head 命中版本 v%s: sha256=%s 判定依据=current_version=%s",
            key, rec.version, rec.sha256, md.current_version,
        )
        return rec

    def get_object(
        self, key: str, version: int | None = None
    ) -> tuple[bytes, VersionRecord]:
        """读取当前可见版本（或指定版本），返回内容与版本记录。

        读取物理副本时强制重算摘要；副本缺失/损坏抛
        :class:`ObjectCorruptedError`，与 key 不存在、已删除明确区分。
        """
        md, rec = self._resolve(key, version)
        if rec.is_delete:
            raise ObjectDeletedError(
                f"对象已删除，当前无可见内容: key={key} version=v{rec.version}"
            )
        data = self.blobs.read_verified(rec.sha256)
        if len(data) != rec.size:
            raise ObjectCorruptedError(
                f"物理副本大小与元数据不符: key={key} version=v{rec.version} "
                f"期望={rec.size} 实际={len(data)} sha256={rec.sha256}"
            )
        logger.info(
            "key=%s 读取版本 v%s: sha256=%s size=%s 校验=重算摘要一致",
            key, rec.version, rec.sha256, len(data),
        )
        return data, rec

    # -- 删除 ----------------------------------------------------------------
    def delete_object(self, key: str) -> VersionRecord:
        """逻辑删除：append 一条删除标记作为当前可见版本。

        历史版本与其物理引用全部保留（可回滚/可审计）；对不存在或已删除
        的对象重复删除是幂等的：直接返回既有状态，不产生新版本。
        """
        _validate_key(key)
        with self._gc_lock.shared():
            with self.meta.lock(key):
                md = self.meta.load(key)
                if md is None:
                    raise ObjectNotFoundError(f"对象不存在: key={key}")
                cur = md.current
                if cur is not None and cur.is_delete:
                    logger.info(
                        "key=%s 重复删除幂等返回: 复用删除标记 v%s",
                        key, cur.version,
                    )
                    return cur
                next_ver = (
                    max((v.version for v in md.versions), default=0) + 1
                )
                # 删除标记引用被删内容，保证其物理副本不会被 GC 回收，
                # 从而仍可回滚/审计。
                rec = VersionRecord(
                    version=next_ver,
                    sha256=cur.sha256 if cur else "",
                    size=cur.size if cur else 0,
                    operation="delete",
                    timestamp=_utcnow_iso(),
                )
                md.versions.append(rec)
                md.current_version = next_ver
                self.meta.save(key, md)
                logger.info(
                    "key=%s 删除标记 v%s（遮蔽至 v%s）: 被删 sha256=%s",
                    key, next_ver, cur.version if cur else 0,
                    rec.sha256,
                )
                return rec

    # -- 回滚 ----------------------------------------------------------------
    def rollback_object(self, key: str, version: int) -> VersionRecord:
        """回滚：把历史“内容版本”重新作为当前可见版本。

        不修改也不删除任何中间版本，而是 append 一条新的 put 记录，
        其 sha256 指向目标历史内容，保证版本链只增不改、引用清晰。
        """
        _validate_key(key)
        with self._gc_lock.shared():
            with self.meta.lock(key):
                md = self.meta.load(key)
                if md is None:
                    raise ObjectNotFoundError(f"对象不存在: key={key}")
                try:
                    target = md.get(version)
                except KeyError:
                    raise VersionNotFoundError(
                        f"回滚目标版本不存在: key={key} version=v{version}"
                    ) from None
                if target.is_delete:
                    raise InvalidArgumentError(
                        f"回滚目标是删除标记而非内容版本: "
                        f"key={key} version=v{version}"
                    )
                cur = md.current
                if cur is not None and not cur.is_delete and (
                    cur.sha256 == target.sha256
                ):
                    logger.info(
                        "key=%s 回滚幂等: 当前内容已与 v%s 相同 (sha256=%s)",
                        key, target.version, target.sha256,
                    )
                    return cur
                if not self.blobs.exists(target.sha256):
                    raise ObjectCorruptedError(
                        f"回滚目标物理副本缺失: key={key} "
                        f"version=v{target.version} sha256={target.sha256}"
                    )
                next_ver = (
                    max((v.version for v in md.versions), default=0) + 1
                )
                rec = VersionRecord(
                    version=next_ver,
                    sha256=target.sha256,
                    size=target.size,
                    operation="put",
                    timestamp=_utcnow_iso(),
                )
                md.versions.append(rec)
                md.current_version = next_ver
                self.meta.save(key, md)
                logger.info(
                    "key=%s 回滚 v%s -> 新版本 v%s: sha256=%s 判定依据=引用历史摘要",
                    key, target.version, next_ver, target.sha256,
                )
                return rec

    # -- 元信息 / GC ---------------------------------------------------------
    def get_metadata(self, key: str) -> ObjectMetadata:
        _validate_key(key)
        md = self.meta.load(key)
        if md is None:
            raise ObjectNotFoundError(f"对象不存在: key={key}")
        return md

    def garbage_collect(self) -> list[str]:
        """回收不被任何版本记录引用的物理副本，返回被删除的摘要列表。

        引用关系判定：先对所有 key 加锁并汇总“版本记录引用摘要集合”，
        物理层中不在该集合内的副本即孤儿副本，可安全删除。
        扫描期间排他等待所有进行中的写入完成，杜绝误删刚写入、
        尚未挂接到元数据的副本。
        """
        with self._gc_lock.exclusive():
            keys = self.meta.list_keys()
            referenced: set[str] = set()
            locks: list = []
            try:
                for key in keys:
                    cm = self.meta.lock(key)
                    cm.__enter__()
                    locks.append(cm)
                for key in keys:
                    md = self.meta.load(key)
                    if md is not None:
                        referenced |= md.referenced_digests()
            finally:
                for cm in reversed(locks):
                    cm.__exit__(None, None, None)

            removed: list[str] = []
            for digest in self.blobs.list_blobs():
                if digest not in referenced:
                    self.blobs.delete(digest)
                    removed.append(digest)
            logger.info(
                "GC: 扫描对象 %d 个, 存活引用 %d 个, 回收孤儿副本 %d 个: %s",
                len(keys), len(referenced), len(removed), removed,
            )
            return removed

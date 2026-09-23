"""逻辑对象的版本元数据管理。

存储布局::

    <root>/meta/<urlencoded-key>.json   # 每个逻辑对象一份版本链
    <root>/locks/<urlencoded-key>.lock  # 跨进程互斥锁文件（flock）

版本链是一条只追加（append-only）的记录序列，``current_version`` 指向
当前可见版本：

* ``put``    记录：该版本对应一份物理副本（sha256 + size），可见；
* ``delete`` 记录：删除标记，引用被删内容的 sha256 以便审计与回收判定，
  该版本“可见状态”为已删除。

保存采用“写临时文件 + fsync + os.replace”原子替换，因此崩溃后元数据
要么是上一个完整版本，要么是下一个完整版本，绝不会出现半截 JSON。
同一逻辑对象的所有“读改写”都必须持有该对象的互斥锁。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import pathlib
import threading
import uuid
from dataclasses import asdict, dataclass, field
from urllib.parse import quote, unquote

from app.errors import ObjectCorruptedError

META_DIRNAME = "meta"
LOCKS_DIRNAME = "locks"


@dataclass
class VersionRecord:
    version: int
    sha256: str
    size: int
    operation: str  # "put" | "delete"
    timestamp: str

    @property
    def is_delete(self) -> bool:
        return self.operation == "delete"


@dataclass
class ObjectMetadata:
    key: str
    current_version: int
    versions: list[VersionRecord] = field(default_factory=list)

    def get(self, version: int) -> VersionRecord:
        for rec in self.versions:
            if rec.version == version:
                return rec
        raise KeyError(version)

    @property
    def current(self) -> VersionRecord | None:
        if self.current_version == 0:
            return None
        return self.get(self.current_version)

    def referenced_digests(self) -> set[str]:
        """所有版本记录引用过的物理副本摘要（GC 判定依据）。"""
        return {r.sha256 for r in self.versions if r.sha256}


def encode_key(key: str) -> str:
    """把逻辑对象 key 编码为安全的单级文件名（可逆、无路径分隔符）。"""
    return quote(key, safe="")


def decode_key(name: str) -> str:
    return unquote(name)


class MetadataStore:
    def __init__(self, root: str | pathlib.Path) -> None:
        self.root = pathlib.Path(root)
        self.meta_dir = self.root / META_DIRNAME
        self.locks_dir = self.root / LOCKS_DIRNAME
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        self._thread_locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    # -- 锁 ------------------------------------------------------------------
    def _thread_lock(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._thread_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._thread_locks[key] = lock
            return lock

    @contextlib.contextmanager
    def lock(self, key: str):
        """获取某个逻辑对象的跨线程 + 跨进程排他锁。"""
        tlock = self._thread_lock(key)
        tlock.acquire()
        lock_path = self.locks_dir / f"{encode_key(key)}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock_path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()
                tlock.release()

    # -- 路径 ----------------------------------------------------------------
    def _meta_path(self, key: str) -> pathlib.Path:
        return self.meta_dir / f"{encode_key(key)}.json"

    # -- 读写 ----------------------------------------------------------------
    def load(self, key: str) -> ObjectMetadata | None:
        path = self._meta_path(key)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            versions = [VersionRecord(**v) for v in raw["versions"]]
            meta = ObjectMetadata(
                key=raw["key"],
                current_version=raw["current_version"],
                versions=versions,
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ObjectCorruptedError(
                f"元数据损坏，无法解析: key={key} 路径={path} 原因={exc}"
            ) from exc
        if meta.key != key or not (
            meta.current_version == 0 or any(
                v.version == meta.current_version for v in meta.versions
            )
        ):
            raise ObjectCorruptedError(
                f"元数据内部不一致: key={key} current_version="
                f"{meta.current_version}"
            )
        return meta

    def save(self, key: str, meta: ObjectMetadata) -> None:
        """原子替换写盘：tmp + fsync + rename，崩溃后不会出现半成品。"""
        path = self._meta_path(key)
        payload = {
            "key": meta.key,
            "current_version": meta.current_version,
            "versions": [asdict(v) for v in meta.versions],
        }
        tmp = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex}")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            self._fsync_dir(path.parent)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    @staticmethod
    def _fsync_dir(path: pathlib.Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def delete_key(self, key: str) -> None:
        path = self._meta_path(key)
        path.unlink(missing_ok=True)
        self._fsync_dir(path.parent)

    def list_keys(self) -> list[str]:
        result: list[str] = []
        for p in sorted(self.meta_dir.iterdir()):
            # 跳过崩溃可能残留的临时文件
            if p.is_file() and p.suffix == ".json":
                result.append(decode_key(p.stem))
        return result

    def recover(self) -> list[str]:
        """清理元数据目录中的原子写临时文件（rename 前崩溃的残留）。"""
        removed: list[str] = []
        for p in self.meta_dir.iterdir():
            if p.is_file() and ".tmp." in p.name:
                p.unlink(missing_ok=True)
                removed.append(p.name)
        return removed

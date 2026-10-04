"""内容寻址的物理存储层（blob layer）。

布局::

    <root>/blobs/<ab>/<64hex>   不可变内容文件，文件名为内容 sha256
    <root>/tmp/                 写入暂存目录（重启后可安全清空）

关键性质：
- 内容先完整写入 tmp，校验通过后原子 rename 到内容寻址路径，
  因此 blobs/ 下永远不会出现半成品；
- 相同 digest 的目标路径唯一，天然去重；重复写入不覆盖、不破坏已存在副本；
- 已存在副本若意外损坏（哈希不符），会用新的正确副本原子替换修复。

多进程：暂存文件名携带属主 PID（.put-<pid>-*），启动恢复只清理
“属主已死亡 / 老版本遗留”的暂存，绝不动其它存活进程正在写的文件。
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import hashlib

from .errors import ChecksumMismatchError, ContentCorruptedError
from .hashing import CHUNK_SIZE, is_valid_sha256_hex, sha256_chunks
from .procutil import is_stale_staging


@dataclass(frozen=True, slots=True)
class StoredBlob:
    """一次成功落盘的物理内容描述。"""

    digest: str
    size: int
    deduplicated: bool = False  # True 表示复用了已存在的物理副本


def _fsync_dir(path: Path) -> None:
    """fsync 目录，保证其中 rename/unlink 的元数据落盘。"""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class BlobStore:
    """以本地磁盘为介质的内容寻址存储，天然去重。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._blobs = self._root / "blobs"
        self._tmp = self._root / "tmp"
        self._blobs.mkdir(parents=True, exist_ok=True)
        self._tmp.mkdir(parents=True, exist_ok=True)

    @property
    def blobs_dir(self) -> Path:
        return self._blobs

    @property
    def tmp_dir(self) -> Path:
        return self._tmp

    def _path_for(self, digest: str) -> Path:
        if not is_valid_sha256_hex(digest):
            # 非法 digest 视为引用损坏，避免任何路径穿越。
            raise ContentCorruptedError(f"非法的内容标识: {digest!r}")
        return self._blobs / digest[:2] / digest

    def put(self, chunks: Iterable[bytes], expected_digest: str | None = None) -> StoredBlob:
        """原子写入一段内容；相同 digest 只保留一份物理副本。

        expected_digest 非空且与实际不符时抛 ChecksumMismatchError；
        任何中途失败都不会在 blobs/ 下留下文件。
        """
        fd, tmp_name = tempfile.mkstemp(dir=self._tmp, prefix=f".put-{os.getpid()}-")
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as fh:
                h = hashlib.sha256()
                size = 0
                for chunk in chunks:
                    fh.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
                fh.flush()
                os.fsync(fh.fileno())
                digest = h.hexdigest()

            if expected_digest is not None and expected_digest != digest:
                raise ChecksumMismatchError(
                    f"声明的 sha256={expected_digest} 与实际内容 sha256={digest} 不一致，"
                    f"size={size}；写入已拒绝"
                )

            target = self._path_for(digest)
            target.parent.mkdir(parents=True, exist_ok=True)

            if target.exists():
                # 去重路径：已存在则校验旧副本；
                # 完好 -> 直接复用，本次暂存丢弃；
                # 损坏 -> 用新的正确副本原子替换修复（视为新写，不算去重）。
                try:
                    self.verify(digest)
                except ContentCorruptedError:
                    os.replace(tmp_path, target)
                    _fsync_dir(target.parent)
                    return StoredBlob(digest=digest, size=size, deduplicated=False)
                else:
                    tmp_path.unlink(missing_ok=True)
                    return StoredBlob(digest=digest, size=size, deduplicated=True)

            os.replace(tmp_path, target)
            _fsync_dir(target.parent)
            return StoredBlob(digest=digest, size=size, deduplicated=False)
        except ChecksumMismatchError:
            tmp_path.unlink(missing_ok=True)
            raise
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    def verify(self, digest: str) -> int:
        """校验已存在的 blob，返回其字节数；缺失/损坏抛 ContentCorruptedError。"""
        path = self._path_for(digest)
        if not path.is_file():
            raise ContentCorruptedError(f"物理内容缺失: digest={digest}, path={path}")
        h = hashlib.sha256()
        total = 0
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
                total += len(chunk)
        actual = h.hexdigest()
        if actual != digest:
            raise ContentCorruptedError(
                f"物理内容校验失败: 期望 digest={digest}，实际 digest={actual}，"
                f"size={total}，path={path}"
            )
        return total

    def get(self, digest: str) -> bytes:
        """按 digest 读取内容；缺失或损坏抛 ContentCorruptedError。"""
        path = self._path_for(digest)
        if not path.is_file():
            raise ContentCorruptedError(f"物理内容缺失: digest={digest}, path={path}")
        data = path.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        if actual != digest:
            raise ContentCorruptedError(
                f"物理内容校验失败: 期望 digest={digest}，实际 digest={actual}，"
                f"size={len(data)}，path={path}"
            )
        return data

    def exists(self, digest: str) -> bool:
        try:
            return self._path_for(digest).is_file()
        except ContentCorruptedError:
            return False

    def all_digests(self) -> set[str]:
        """枚举磁盘上当前全部 blob 的 digest（仅接受合法命名）。"""
        found: set[str] = set()
        if not self._blobs.is_dir():
            return found
        for shard in self._blobs.iterdir():
            if not shard.is_dir() or len(shard.name) != 2:
                continue
            for entry in shard.iterdir():
                if entry.is_file() and is_valid_sha256_hex(entry.name):
                    found.add(entry.name)
        return found

    def delete(self, digest: str) -> None:
        """物理删除一个 blob（仅 GC 在确认无引用后调用）。"""
        path = self._path_for(digest)
        path.unlink(missing_ok=True)

    def cleanup_staging(self) -> int:
        """清理暂存目录中的残留，返回删除的文件数（启动恢复用）。

        只删除“属主进程已死亡”或“老版本遗留（文件名无 PID）”的暂存文件；
        其它存活进程正在写入的暂存文件保持不动（多进程安全）。
        """
        count = 0
        if not self._tmp.is_dir():
            return count
        for entry in self._tmp.iterdir():
            if not (entry.is_file() or entry.is_symlink()):
                continue
            if not is_stale_staging(entry.name, "put"):
                continue
            entry.unlink()
            count += 1
        return count


__all__ = ["StoredBlob", "BlobStore"]

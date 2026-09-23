"""内容寻址的物理对象层。

存储布局::

    <root>/blobs/<ab>/<ab...64hex>   # 以摘要前两位分桶的不可变物理副本
    <root>/tmp/<uuid>                # 写入暂存区，原子提升后即消失

规则：
* 物理副本以内容 sha256 命名，天然去重：相同内容只保留一份。
* 写入先落 tmp，fsync 后再 ``os.replace`` 原子提升到最终路径；
  最终路径已存在时直接丢弃暂存文件，重复写入绝不触碰已有副本。
* 副本一旦写入即不可变；任何读取都重新计算摘要并与文件名比对，
  缺失/损坏抛出 :class:`ObjectCorruptedError`。
"""

from __future__ import annotations

import os
import pathlib
import uuid

from app.errors import (
    ChecksumMismatchError,
    InvalidArgumentError,
    ObjectCorruptedError,
)
from app.hashing import is_valid_sha256, sha256_bytes, sha256_file

BLOBS_DIRNAME = "blobs"
TMP_DIRNAME = "tmp"


class BlobStore:
    """以 sha256 内容摘要寻址的物理副本存储。"""

    def __init__(self, root: str | pathlib.Path) -> None:
        self.root = pathlib.Path(root)
        self.blobs_dir = self.root / BLOBS_DIRNAME
        self.tmp_dir = self.root / TMP_DIRNAME
        self.blobs_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    # -- 路径 ----------------------------------------------------------------
    def _validate_digest(self, digest: str) -> str:
        if not is_valid_sha256(digest):
            raise InvalidArgumentError(f"非法的 sha256 摘要: {digest!r}")
        return digest.lower()

    def get_path(self, digest: str) -> pathlib.Path:
        digest = self._validate_digest(digest)
        return self.blobs_dir / digest[:2] / digest

    # -- 写入 ----------------------------------------------------------------
    def put(self, data: bytes, expected_sha256: str | None = None) -> str:
        """原子写入内容，返回内容 sha256。

        :raises ChecksumMismatchError: 客户端声明的 ``expected_sha256``
            与实际内容摘要不一致，此时不会留下任何物理副本。
        """
        digest = sha256_bytes(data)
        if expected_sha256 is not None:
            expected = expected_sha256.lower()
            if not is_valid_sha256(expected):
                raise InvalidArgumentError(
                    f"非法的 expected_sha256: {expected_sha256!r}"
                )
            if expected != digest:
                raise ChecksumMismatchError(
                    f"内容校验失败: 声明={expected} 实际={digest} "
                    f"字节数={len(data)}"
                )

        final = self.get_path(digest)
        if final.exists():
            # 去重命中：物理副本已存在，绝不重写，直接返回其标识。
            return digest

        final.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.tmp_dir / uuid.uuid4().hex
        try:
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            # 提升前再确认一次目标，避免并发下对同路径做无意义替换。
            if not final.exists():
                os.replace(tmp, final)
                self._fsync_dir(final.parent)
            else:
                tmp.unlink(missing_ok=True)
        except BaseException:
            # 写入中途失败：暂存文件保留在 tmp/，绝不暴露半成品，
            # 等待下次启动 recover() 清理。
            tmp.unlink(missing_ok=True)
            raise
        return digest

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

    # -- 读取 ----------------------------------------------------------------
    def read_verified(self, digest: str) -> bytes:
        """读取并校验物理副本。

        :raises ObjectCorruptedError: 副本缺失或实际摘要与文件名不一致
            （内容损坏），这与“校验和不匹配的写入被拒绝”明确区分。
        """
        digest = self._validate_digest(digest)
        path = self.get_path(digest)
        if not path.exists():
            raise ObjectCorruptedError(f"物理副本缺失: sha256={digest}")
        actual = sha256_file(path)
        if actual != digest:
            raise ObjectCorruptedError(
                f"物理副本损坏: 期望={digest} 实际={actual} 路径={path}"
            )
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ObjectCorruptedError(
                f"物理副本不可读: sha256={digest} 原因={exc}"
            ) from exc

    def exists(self, digest: str) -> bool:
        return self.get_path(digest).exists()

    def delete(self, digest: str) -> None:
        path = self.get_path(digest)
        path.unlink(missing_ok=True)
        self._fsync_dir(path.parent)

    def list_blobs(self) -> list[str]:
        result: list[str] = []
        for shard in sorted(self.blobs_dir.iterdir()):
            if not shard.is_dir():
                continue
            for p in sorted(shard.iterdir()):
                if p.is_file() and is_valid_sha256(p.name):
                    result.append(p.name)
        return result

    # -- 崩溃恢复 -------------------------------------------------------------
    def recover(self) -> list[str]:
        """清理暂存区残留（写入中途崩溃 / 进程被杀留下的半成品）。

        暂存文件从未被原子提升，任何元数据都不会引用它，直接删除即可。
        返回被清理的暂存文件名列表。
        """
        removed: list[str] = []
        for p in self.tmp_dir.iterdir():
            if p.is_file():
                p.unlink(missing_ok=True)
                removed.append(p.name)
        return removed

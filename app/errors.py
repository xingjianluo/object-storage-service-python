"""可区分的存储错误类型。"""


class StorageError(Exception):
    """所有存储相关错误的基类，携带稳定的错误码。"""

    code: str = "storage_error"


class ChecksumMismatchError(StorageError):
    """客户端声明的校验值与实际内容不一致，写入被拒绝。"""

    code = "checksum_mismatch"


class ContentCorruptedError(StorageError):
    """物理内容读取后校验不通过（磁盘上的 blob 损坏）。"""

    code = "content_corrupted"


class ObjectNotFoundError(StorageError):
    """逻辑对象不存在，或已被删除（无任何可见版本）。"""

    code = "object_not_found"


class ObjectDeletedError(StorageError):
    """对象存在但当前处于已删除状态（墓碑之后无可见版本）。"""

    code = "object_deleted"


class VersionNotFoundError(StorageError):
    """对象存在但请求的具体版本不存在。"""

    code = "version_not_found"


__all__ = [
    "StorageError",
    "ChecksumMismatchError",
    "ContentCorruptedError",
    "ObjectNotFoundError",
    "ObjectDeletedError",
    "VersionNotFoundError",
]

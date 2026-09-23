"""可区分的错误类型定义。"""


class StorageError(Exception):
    """存储层错误基类。"""


class ChecksumMismatchError(StorageError):
    """写入内容与客户端声明的校验值不一致。"""


class ObjectCorruptedError(StorageError):
    """物理内容损坏（实际字节摘要与元数据不一致或缺失）。"""


class ObjectNotFoundError(StorageError):
    """逻辑对象不存在，或当前没有可见版本（已删除）。"""


class VersionNotFoundError(StorageError):
    """指定的历史版本不存在。"""


class InvalidArgumentError(StorageError):
    """请求参数非法。"""


class ObjectDeletedError(StorageError):
    """对象存在但当前无可见内容（被删除标记覆盖）。

    与 :class:`ObjectNotFoundError` 区分：前者说明该 key 有历史版本链，
    只是当前版本是删除标记；后者说明 key 从未出现过。
    """

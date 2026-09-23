"""内容标识（摘要）工具。

所有物理副本以其内容的 sha256 摘要作为全局唯一标识，
相同内容必然得到相同标识，这是去重与损坏检测的判定依据。
"""

from __future__ import annotations

import hashlib
import pathlib

CHUNK_SIZE = 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    """计算字节串的 sha256 十六进制摘要。"""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | pathlib.Path) -> str:
    """分块计算磁盘文件的 sha256 摘要。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def is_valid_sha256(value: str) -> bool:
    """判断字符串是否为合法的 64 位十六进制 sha256 摘要。"""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value.lower())
    )

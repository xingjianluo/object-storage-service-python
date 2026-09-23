"""内容标识（SHA-256）相关原语。"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

DIGEST_HEX_LEN = 64
CHUNK_SIZE = 1024 * 1024


def sha256_hex(data: bytes) -> str:
    """计算单块字节内容的 sha256 十六进制摘要。"""
    return hashlib.sha256(data).hexdigest()


def sha256_chunks(chunks: Iterable[bytes]) -> tuple[str, int]:
    """对流式分块内容计算 sha256 摘要，同时返回总字节数。"""
    h = hashlib.sha256()
    total = 0
    for chunk in chunks:
        if not isinstance(chunk, bytes | bytearray | memoryview):
            raise TypeError(f"chunk 必须是 bytes-like，实际为 {type(chunk).__name__}")
        h.update(chunk)
        total += len(chunk)
    return h.hexdigest(), total


def is_valid_sha256_hex(value: str) -> bool:
    """判断字符串是否为合法的 64 位小写十六进制 sha256。"""
    return (
        isinstance(value, str)
        and len(value) == DIGEST_HEX_LEN
        and all(c in "0123456789abcdef" for c in value)
    )


__all__ = ["DIGEST_HEX_LEN", "CHUNK_SIZE", "sha256_hex", "sha256_chunks", "is_valid_sha256_hex"]

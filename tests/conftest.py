"""pytest 公共夹具与日志配置。

所有单测统一打印：内容标识(sha256)、版本号、判定依据，
并在关键断言处通过 ``verify_log`` 风格的辅助方法输出判定信息。
"""

from __future__ import annotations

import logging
import sys

import pytest

from app.storage import ObjectStorage

# 让存储层日志在单测中可见（内容标识 / 版本 / 判定依据）
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
)
logger = logging.getLogger("object_storage")
logger.setLevel(logging.INFO)
logger.handlers = [_handler]
logger.propagate = False


@pytest.fixture()
def storage(tmp_path):
    root = tmp_path / "store"
    s = ObjectStorage(root)
    print(f"\n[fixture] 存储根目录: {root}")
    yield s


def digest(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()

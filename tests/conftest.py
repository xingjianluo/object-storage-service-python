"""pytest 公共夹具与日志配置。

日志强制打印：内容标识(digest)、版本(version)、判定依据(reason)，
即使 pytest 捕获输出也会在失败时随用例展示；加 -s 可实时查看。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

from app.service import StorageService

# 让业务日志在单测中可见（打印 digest / version / 判定依据）
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    force=True,
)


@pytest.fixture()
def tmp_root(tmp_path: Path) -> Path:
    return tmp_path / "store"


@pytest.fixture()
def service(tmp_root: Path) -> StorageService:
    return StorageService(tmp_root)


def log_case(title: str, **fields) -> None:
    """统一格式打印用例判定信息：标识/版本/依据。"""
    detail = " ".join(f"{k}={v}" for k, v in fields.items())
    logging.getLogger("oss.test").info("【%s】%s", title, detail)

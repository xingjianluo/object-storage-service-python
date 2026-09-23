"""逻辑对象 -> 版本列表 的元数据目录（meta layer）。

布局::

    <root>/meta/<shard>/<urlencoded-key>.json   每个逻辑对象一个元数据文件

分片规则：对 key 做 sha256，取前 2 个十六进制字符作为分片目录，
避免单目录文件过多；文件名使用 URL 安全编码，避免路径穿越与分隔符冲突。

原子性：元数据以“写临时文件 + fsync + os.replace 原子改名”发布，
崩溃后读到的要么是上一个完整版本，要么是新的完整版本，绝不会是半成品。
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, unquote

SCHEMA_VERSION = 1


@dataclass(slots=True)
class Version:
    """一个不可变的历史版本记录。"""

    version: int           # 单调递增的版本号，从 1 开始
    digest: str            # 内容 sha256；墓碑版本为空串
    size: int              # 内容字节数；墓碑为 0
    created_at: float      # 提交时间戳（Unix 秒）
    deleted: bool = False  # True 表示删除墓碑


@dataclass(slots=True)
class ObjectState:
    """一个逻辑对象的完整元数据。"""

    key: str
    versions: list[Version] = field(default_factory=list)
    current: int | None = None  # 当前可见版本号；None 表示无可见版本

    def visible(self) -> Version | None:
        """返回当前可见版本；不可见时返回 None。"""
        if self.current is None:
            return None
        return self.find(self.current)

    def find(self, version: int) -> Version | None:
        """按版本号查找记录（含墓碑），找不到返回 None。"""
        for v in self.versions:
            if v.version == version:
                return v
        return None

    def next_version_no(self) -> int:
        """下一个版本号（始终单调递增，删除也占用版本号）。"""
        if not self.versions:
            return 1
        return max(v.version for v in self.versions) + 1

    def to_dict(self) -> dict:
        return {
            "schema": SCHEMA_VERSION,
            "key": self.key,
            "current": self.current,
            "versions": [
                {
                    "version": v.version,
                    "digest": v.digest,
                    "size": v.size,
                    "created_at": v.created_at,
                    "deleted": v.deleted,
                }
                for v in self.versions
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ObjectState":
        if data.get("schema") != SCHEMA_VERSION:
            raise ValueError(f"不支持的元数据 schema: {data.get('schema')!r}")
        state = cls(key=str(data["key"]), current=data.get("current"))
        for item in data.get("versions", []):
            state.versions.append(
                Version(
                    version=int(item["version"]),
                    digest=str(item.get("digest", "")),
                    size=int(item.get("size", 0)),
                    created_at=float(item["created_at"]),
                    deleted=bool(item.get("deleted", False)),
                )
            )
        return state


def _shard_of(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:2]


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Catalog:
    """对象元数据的持久化目录。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._meta = self._root / "meta"
        self._meta.mkdir(parents=True, exist_ok=True)

    @property
    def meta_dir(self) -> Path:
        return self._meta

    def _path_for(self, key: str) -> Path:
        if not isinstance(key, str) or not key:
            raise ValueError("对象 key 必须是非空字符串")
        if "\x00" in key:
            raise ValueError("对象 key 不能包含 NUL 字符")
        shard = _shard_of(key)
        # safe='' 使 '/' 也被编码，杜绝任何路径穿越。
        return self._meta / shard / (quote(key, safe="") + ".json")

    def load(self, key: str) -> ObjectState | None:
        """加载对象元数据；从无元数据文件返回 None。文件损坏抛 ValueError/OSError。"""
        path = self._path_for(key)
        if not path.is_file():
            return None
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        state = ObjectState.from_dict(data)
        if state.key != key:
            raise ValueError(f"元数据 key 不一致: 文件内={state.key!r} 请求={key!r}")
        return state

    def store(self, state: ObjectState) -> None:
        """原子发布对象元数据（临时文件 + fsync + rename）。"""
        path = self._path_for(state.key)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state.to_dict(), ensure_ascii=False, sort_keys=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=".meta-", suffix=".tmp")
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)
            _fsync_dir(path.parent)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    def all_referenced_digests(self) -> set[str]:
        """枚举全部版本记录引用的 blob digest（GC 的可达集合，含历史版本）。"""
        referenced: set[str] = set()
        for key in self.all_keys():
            state = self.load(key)
            if state is None:
                continue
            for v in state.versions:
                if not v.deleted and v.digest:
                    referenced.add(v.digest)
        return referenced

    def all_keys(self) -> list[str]:
        """枚举所有拥有元数据文件的逻辑对象 key（含已删除）。"""
        keys: list[str] = []
        if not self._meta.is_dir():
            return keys
        for shard in sorted(self._meta.iterdir()):
            if not shard.is_dir():
                continue
            for entry in sorted(shard.iterdir()):
                if entry.suffix != ".json" or entry.name.startswith("."):
                    continue
                keys.append(unquote(entry.stem))
        return keys


def now_ts() -> float:
    """当前 Unix 时间戳（秒），便于测试替身。"""
    return time.time()


__all__ = ["Version", "ObjectState", "Catalog", "now_ts", "SCHEMA_VERSION"]

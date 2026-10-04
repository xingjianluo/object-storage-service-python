"""跨进程文件锁（fcntl.flock），支撑单机多进程共享同一数据目录。

锁布局::

    <root>/locks/gc.lock                 GC 与写入变更的互斥锁（写=共享，GC=独占）
    <root>/locks/obj/<shard>/<key>.lock  每个逻辑对象一把排他锁（版本号严格连续的依据）

设计要点：
- flock 是“劝告锁”，锁随 fd 关闭/进程死亡由内核自动释放，
  进程被 kill -9 也不会留下死锁，无需清理锁文件；
- 锁文件创建后不再删除：数量以对象数为界，不会随运行时间无限累积；
- 进程内另有一把 threading.RLock 串行化本进程线程，flock 只解决跨进程互斥；
- 一致性承诺仅限同一台机器上的本地文件系统（ext4/xfs/tmpfs 等）；
  NFS 等网络盘的 flock 语义依赖服务端配置，不在支持范围内。
"""

from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
from typing import Self
from urllib.parse import quote


def _shard_of(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:2]


class FileLock:
    """对单个锁文件的 flock 包装（上下文管理器，可重入需自行保证）。"""

    def __init__(self, path: str | Path, *, exclusive: bool = True) -> None:
        self._path = Path(path)
        self._exclusive = exclusive
        self._fd: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeError(f"锁已持有: {self._path}")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # 锁文件只增不删；O_CREAT 保证首次使用即存在。
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            op = fcntl.LOCK_EX if self._exclusive else fcntl.LOCK_SH
            fcntl.flock(fd, op)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class LockManager:
    """数据目录级别的锁工厂：对象锁 + GC 互斥锁。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._locks = self._root / "locks"
        (self._locks / "obj").mkdir(parents=True, exist_ok=True)

    def object_lock(self, key: str) -> FileLock:
        """某逻辑对象的排他锁：串行化该对象的版本变更（put/delete/rollback）。

        不同 key 落到不同锁文件，因此不同对象之间的并发操作互不阻塞。
        """
        if not isinstance(key, str) or not key:
            raise ValueError("对象 key 必须是非空字符串")
        shard = _shard_of(key)
        path = self._locks / "obj" / shard / (quote(key, safe="") + ".lock")
        return FileLock(path, exclusive=True)

    def gc_lock(self, *, exclusive: bool) -> FileLock:
        """GC 互斥锁：写入/删除/回滚持共享锁(exclusive=False)，GC 持独占锁。

        保证 GC 做“标记-清除”的全程没有任何进程处于
        “blob 已提交、meta 未发布”的中间态，刚提交的内容不会被误判为垃圾。
        """
        return FileLock(self._locks / "gc.lock", exclusive=exclusive)


__all__ = ["FileLock", "LockManager"]

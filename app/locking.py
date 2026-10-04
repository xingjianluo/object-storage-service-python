"""跨进程文件锁（单机多进程共享同一数据目录时的互斥原语）。

锁文件位于 <root>/locks/ 下，用 fcntl.flock 对整文件加锁：
- 每次获取都重新 open() 一个独立的 open file description，因此同进程内
  多个线程的并发获取也严格按“排他互斥、共享共存”语义排队，
  不再依赖只在单进程内有效的 threading 锁；
- 锁由内核持有，进程被 SIGKILL 后内核自动释放，不存在“死锁残留”，
  锁文件本身是空文件、可重复复用，不随崩溃累积；
- 锁顺序约定（全局唯一，杜绝死锁）：
  1) gc 屏障锁 locks/gc.lock：变更类操作持共享锁，GC 持排他锁；
  2) 对象锁 locks/obj/<shard>/<sha256(key)>.lock：同一对象的变更串行化。
  嵌套顺序固定为“先 gc 屏障、后对象锁”，代码中不存在反向嵌套。

适用介质：提供 POSIC flock 语义的文件系统。本机本地盘/ext4/xfs/tmpfs 可靠；
NFS 仅 NFSv4 且挂载点 flock 语义可靠时可用（详见 README）。
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import time
from collections.abc import Iterator
from pathlib import Path

DEFAULT_TIMEOUT = 30.0
_POLL_INTERVAL = 0.01


class LockAcquireTimeout(TimeoutError):
    """在限定时间内未能获得文件锁。"""


class _FileLock:
    """单个锁文件上的共享/排他 flock（非重入；本服务不存在嵌套获取同名锁）。"""

    def __init__(self, path: Path, exclusive: bool, timeout: float | None) -> None:
        self._path = path
        self._exclusive = exclusive
        self._timeout = DEFAULT_TIMEOUT if timeout is None else timeout
        self._fd: int | None = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # 独立 open()：不同进程、同进程不同线程拿到的都是独立的锁持有者。
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        base = fcntl.LOCK_EX if self._exclusive else fcntl.LOCK_SH
        try:
            if self._timeout is None:
                fcntl.flock(fd, base)  # PEP 475：被信号中断时自动重试
            else:
                deadline = time.monotonic() + max(0.0, self._timeout)
                while True:
                    try:
                        fcntl.flock(fd, base | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise LockAcquireTimeout(
                                f"获取锁超时({self._timeout}s): {self._path}"
                            ) from None
                        time.sleep(_POLL_INTERVAL)
            self._fd = fd
        except BaseException:
            os.close(fd)
            raise

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def __enter__(self) -> "_FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()


class LockManager:
    """数据目录内全部跨进程锁的入口。"""

    def __init__(self, root: str | Path) -> None:
        self._locks = Path(root) / "locks"
        self._objects = self._locks / "obj"
        self._locks.mkdir(parents=True, exist_ok=True)
        self._objects.mkdir(parents=True, exist_ok=True)
        self._gc_lock = self._locks / "gc.lock"

    @property
    def locks_dir(self) -> Path:
        return self._locks

    @staticmethod
    def _object_lock_path(objects_dir: Path, key: str) -> Path:
        if not isinstance(key, str) or not key or "\x00" in key:
            raise ValueError("对象 key 必须是非空且不含 NUL 的字符串")
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return objects_dir / digest[:2] / (digest + ".lock")

    def object_lock(self, key: str, timeout: float | None = DEFAULT_TIMEOUT) -> _FileLock:
        """某逻辑对象的排他锁（不同对象互不阻塞）。"""
        return _FileLock(self._object_lock_path(self._objects, key), True, timeout)

    @contextlib.contextmanager
    def mutation_barrier(self, key: str,
                         timeout: float | None = DEFAULT_TIMEOUT) -> Iterator[None]:
        """变更类操作的标准加锁顺序：先共享 gc 屏障，再排他对象锁。

        - 与 GC 互斥（GC 拿排他 gc 屏障，必须等在途变更全部结束）；
        - 不同对象之间只在 gc 屏障上共享共存，对象锁互不阻塞；
        - 同一对象的所有变更严格串行，版本号因此连续不重不跳。
        """
        with _FileLock(self._gc_lock, False, timeout):
            with self.object_lock(key, timeout):
                yield

    @contextlib.contextmanager
    def gc_lock(self, timeout: float | None = DEFAULT_TIMEOUT) -> Iterator[None]:
        """GC 排他屏障：持有期间不存在任何在途变更。"""
        with _FileLock(self._gc_lock, True, timeout):
            yield


__all__ = ["LockManager", "LockAcquireTimeout", "DEFAULT_TIMEOUT"]

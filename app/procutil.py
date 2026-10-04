"""进程存活判定与暂存文件归属解析（单机多进程恢复用）。

暂存文件命名约定：``.<prefix>-<pid>-<random>``（pid 为创建进程号）。
恢复时只清理“属主进程已死亡”或“老版本遗留（命名中无 pid）”的暂存文件，
绝不触碰其它存活进程正在写入的暂存文件。
"""

from __future__ import annotations

import os
import re


def pid_alive(pid: int) -> bool:
    """本机上该 PID 是否有存活进程（kill(pid,0) 探测）。"""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # 存在但无权发信号，视为存活
    return True


def staging_owner_pid(name: str, prefix: str) -> int | None:
    """解析暂存文件名中的属主 PID。

    新格式 ``.{prefix}-<pid>-<random>`` 返回 pid；
    老格式（无 pid 段，mkstemp 随机串不含 '-'）返回 None。
    """
    m = re.fullmatch(rf"\.{re.escape(prefix)}-(\d+)-.+", name)
    if m:
        return int(m.group(1))
    return None


def is_stale_staging(name: str, prefix: str) -> bool:
    """判断暂存文件是否为可安全清理的残留。

    - 老格式（无 pid）：只可能是已崩溃的旧版本进程遗留 -> 可清理；
    - 新格式且属主 PID 已死 -> 可清理；
    - 新格式且属主存活 -> 其它进程正在写，绝不可动。
    """
    pid = staging_owner_pid(name, prefix)
    if pid is None:
        return True
    return not pid_alive(pid)


__all__ = ["is_stale_staging", "pid_alive", "staging_owner_pid"]

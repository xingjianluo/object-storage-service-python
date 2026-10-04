"""多进程并发与崩溃恢复测试：同一台机器多个服务进程共享同一数据目录。

覆盖：
1. 多进程并发写同一逻辑对象：版本号严格 1..N 不重不跳，
   每次成功返回的版本都能读到其对应内容（sha256 自洽）；
2. 多进程混合操作（写/删/回滚）：版本序列仍连续，已提交内容不丢；
3. 写入与 GC 并发：GC 永不回收仍被引用的内容，写入方数据完好；
4. 进程被 kill -9 后的重启恢复：版本连续、内容可校验、
   死亡进程暂存残留被清理、恢复后立刻可读写。

日志均打印 进程号(pid)/内容标识(digest)/版本(version)/判定依据/结果。
子进程用 spawn 启动，各自独立构造 StorageService（等价于独立服务进程）。
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import sys
import time
from pathlib import Path

import pytest

from app.hashing import sha256_hex
from app.service import StorageService

from .conftest import log_case

_MP = multiprocessing.get_context("spawn")


def _child_logging() -> None:
    """子进程内重新配置日志（spawn 不继承父进程 logging 配置）。"""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


def _join_all(procs: list[multiprocessing.Process], timeout: float = 60.0) -> None:
    """带超时地回收子进程；卡死即强杀并失败，避免测试悬挂。"""
    deadline = time.monotonic() + timeout
    for p in procs:
        p.join(max(0.0, deadline - time.monotonic()))
    for p in procs:
        if p.is_alive():
            p.kill()
            p.join()
            pytest.fail(f"子进程 pid={p.pid} 未在 {timeout}s 内退出（疑似死锁）")
        assert p.exitcode == 0, f"子进程 pid={p.pid} 异常退出 exitcode={p.exitcode}"


# ---- 子进程入口（必须模块级，spawn 才能导入） ----

def _put_worker(root: str, key: str, count: int, barrier, q) -> None:
    """每个进程独占一个 StorageService，对同一 key 连写 count 个版本。"""
    _child_logging()
    pid = os.getpid()
    svc = StorageService(root)
    barrier.wait()  # 尽量对齐起跑线，制造真实竞争
    for i in range(count):
        content = f"writer-pid-{pid}-seq-{i}".encode()
        result = svc.put_object(key, [content], expected_digest=sha256_hex(content))
        ver = result.version
        logging.getLogger("oss.test.mp").info(
            "【并发写-子进程】pid=%d key=%r seq=%d -> version=%d digest=%s 结果=成功",
            pid, key, i, ver.version, ver.digest,
        )
        q.put((pid, i, ver.version, ver.digest, content))


def _churn_worker(root: str, key: str, count: int, barrier, q) -> None:
    """混合操作：写 / 删 / 回滚到 v1 交替进行。"""
    _child_logging()
    pid = os.getpid()
    svc = StorageService(root)
    barrier.wait()
    for i in range(count):
        op = i % 3
        if op == 0:
            content = f"churn-pid-{pid}-seq-{i}".encode()
            r = svc.put_object(key, [content])
            q.put(("put", r.version.version, r.version.digest, content))
            logging.getLogger("oss.test.mp").info(
                "【混合-写】pid=%d seq=%d version=%d digest=%s", pid, i,
                r.version.version, r.version.digest,
            )
        elif op == 1:
            v = svc.delete_object(key)
            q.put(("del", v.version, "", b""))
            logging.getLogger("oss.test.mp").info(
                "【混合-删】pid=%d seq=%d tombstone_version=%d", pid, i, v.version,
            )
        else:
            v = svc.rollback(key, 1)
            q.put(("rollback", v.version, v.digest, b""))
            logging.getLogger("oss.test.mp").info(
                "【混合-回滚】pid=%d seq=%d current->v%d", pid, i, v.version,
            )


def _gc_worker(root: str, stop, q) -> None:
    """循环执行 GC，直到 stop 置位；每次有回收就上报 digest 列表。"""
    _child_logging()
    pid = os.getpid()
    svc = StorageService(root)
    rounds = 0
    while not stop.is_set():
        collected = svc.gc()
        rounds += 1
        if collected:
            logging.getLogger("oss.test.mp").info(
                "【GC-子进程】pid=%d round=%d collected=%s", pid, rounds, collected,
            )
            q.put(collected)
        time.sleep(0.005)
    logging.getLogger("oss.test.mp").info("【GC-子进程】pid=%d 共执行 %d 轮", pid, rounds)


def _victim_worker(root: str, key: str, q) -> None:
    """被强杀目标：无限循环写同一 key，每成功一次上报一次。"""
    _child_logging()
    pid = os.getpid()
    svc = StorageService(root)
    q.put(("ready", pid))
    i = 0
    while True:
        content = f"victim-pid-{pid}-seq-{i}".encode()
        r = svc.put_object(key, [content])
        q.put(("committed", i, r.version.version, r.version.digest, content))
        i += 1


# ---- 用例 ----

def test_multiprocess_concurrent_put_same_object(tmp_root: Path):
    """4 进程 x 8 次并发写同一对象：版本 1..32 不重不跳，逐版本内容可校验。"""
    procs_n, per_proc = 4, 8
    barrier = _MP.Barrier(procs_n)
    q = _MP.Queue()
    procs = [
        _MP.Process(target=_put_worker, args=(str(tmp_root), "hot", per_proc, barrier, q))
        for _ in range(procs_n)
    ]
    for p in procs:
        p.start()
    _join_all(procs)

    reported = [q.get(timeout=10) for _ in range(procs_n * per_proc)]
    svc = StorageService(tmp_root)
    versions = svc.list_versions("hot")
    nums = sorted(v.version for v in versions)
    log_case(
        "多进程并发写同一对象",
        进程数=procs_n, 每进程写入=per_proc,
        版本序列=f"{nums[0]}..{nums[-1]}", 版本总数=len(nums),
        依据="对象排他锁串行化读-改-发,版本号必须1..N不重不跳",
    )
    assert nums == list(range(1, procs_n * per_proc + 1))

    # 每一次成功返回的 (version, digest) 都必须能读到其对应内容
    for pid, seq, ver_no, digest, content in reported:
        data, ver = svc.get_object("hot", version=ver_no)
        assert ver.digest == digest, f"pid={pid} seq={seq} version={ver_no} digest 漂移"
        assert data == content and sha256_hex(data) == digest
    log_case(
        "多进程并发写-逐版本校验",
        校验条数=len(reported),
        结果="全部成功返回的版本读回内容均与写入一致(sha256自洽)",
        依据="成功返回即承诺可读;不允许被别的进程悄悄覆盖",
    )


def test_multiprocess_mixed_ops_same_object(tmp_root: Path):
    """多进程混合 写/删/回滚：版本序列连续，已提交内容均可读回。"""
    svc0 = StorageService(tmp_root)
    svc0.put_object("mix", [b"seed-v1"])  # 保证 rollback/delete 有目标
    procs_n, per_proc = 3, 15
    barrier = _MP.Barrier(procs_n)
    q = _MP.Queue()
    procs = [
        _MP.Process(target=_churn_worker, args=(str(tmp_root), "mix", per_proc, barrier, q))
        for _ in range(procs_n)
    ]
    for p in procs:
        p.start()
    _join_all(procs)

    events = [q.get(timeout=10) for _ in range(procs_n * per_proc)]
    svc = StorageService(tmp_root)
    nums = sorted(v.version for v in svc.list_versions("mix"))
    log_case(
        "多进程混合操作",
        操作总数=len(events), 版本序列=f"1..{nums[-1]}", 版本数=len(nums),
        依据="写/删/回滚互斥串行,版本号只增不跳",
    )
    assert nums == list(range(1, nums[-1] + 1))

    puts = [e for e in events if e[0] == "put"]
    for _, ver_no, digest, content in puts:
        data, ver = svc.get_object("mix", version=ver_no)
        assert ver.digest == digest and data == content
    log_case(
        "多进程混合-内容校验",
        写入事件=len(puts), 结果="每个put版本读回内容一致",
        依据="墓碑与回滚不影响历史版本内容可达性",
    )


def test_multiprocess_gc_concurrent_with_writes(tmp_root: Path):
    """写入与 GC 并发：GC 不得回收任何被引用内容，写入结果全部完好。"""
    # 预置一个“blob 已落、meta 未发布”的孤儿，验证并发 GC 能且仅能回收它
    planter = StorageService(tmp_root)
    orphan = planter.blob_store.put([b"orphan-never-referenced"])
    del planter

    procs_n, per_proc = 3, 10
    barrier = _MP.Barrier(procs_n)
    q = _MP.Queue()
    stop = _MP.Event()
    writers = [
        _MP.Process(
            target=_put_worker,
            args=(str(tmp_root), f"gc-key-{w}", per_proc, barrier, q),
        )
        for w in range(procs_n)
    ]
    collected_q = _MP.Queue()
    gc_proc = _MP.Process(target=_gc_worker, args=(str(tmp_root), stop, collected_q))
    gc_proc.start()
    for p in writers:
        p.start()
    _join_all(writers)
    stop.set()
    _join_all([gc_proc])

    reported = [q.get(timeout=10) for _ in range(procs_n * per_proc)]
    collected: set[str] = set()
    while not collected_q.empty():
        collected.update(collected_q.get())

    svc = StorageService(tmp_root)
    referenced = svc.catalog.all_referenced_digests()
    log_case(
        "写入与GC并发-引用判定",
        写入版本数=len(reported), 被引用blob=len(referenced),
        GC回收=len(collected),
        交集=len(collected & referenced),
        依据="GC独占锁与写路径共享锁互斥;任一版本引用即存活",
    )
    assert not (collected & referenced), "GC 回收了仍被引用的内容!"
    # 预置孤儿必须已被并发中的 GC 精确回收（证明 GC 确实在写入期间工作过）
    assert orphan.digest in collected, "并发 GC 未回收预置孤儿"
    assert not svc.blob_store.exists(orphan.digest)

    # 每个成功写入的版本：blob 仍在且 sha256 校验通过
    for pid, seq, ver_no, digest, content in reported:
        assert svc.blob_store.verify(digest) == len(content)
    # 收尾再做一次 GC，磁盘上不应再有任何被引用内容缺失
    svc.gc()
    for key in (f"gc-key-{w}" for w in range(procs_n)):
        for v in svc.list_versions(key):
            data, _ = svc.get_object(key, version=v.version)
            assert sha256_hex(data) == v.digest
    log_case(
        "写入与GC并发-存活校验",
        结果="全部版本内容读回且sha256自洽,无丢失无误报缺失",
        依据="被引用blob永不回收;读不到半成品",
    )


def test_multiprocess_kill9_then_recover(tmp_root: Path):
    """写入进程被 kill -9：重启后版本连续、内容可校验、残留被清理、立即可读写。"""
    q = _MP.Queue()
    victim = _MP.Process(target=_victim_worker, args=(str(tmp_root), "victim", q))
    victim.start()
    kind, victim_pid = q.get(timeout=10)
    assert kind == "ready"

    committed: list[tuple] = []
    first = q.get(timeout=10)  # 至少等到 1 次成功提交
    committed.append(first)
    time.sleep(0.3)  # 让它持续写，制造“写到一半被杀”的窗口
    os.kill(victim_pid, signal.SIGKILL)
    victim.join(timeout=10)
    assert victim.exitcode == -signal.SIGKILL
    while not q.empty():
        committed.append(q.get())
    log_case(
        "强杀写入进程",
        victim_pid=victim_pid, 已提交次数=len(committed),
        依据="flock随进程死亡由内核释放,不会死锁",
    )

    # 模拟崩溃残留：以死亡进程 PID 命名暂存文件（等价于被杀瞬间的半成品）
    tmp_dir = tmp_root / "tmp"
    meta_shard = next((tmp_root / "meta").iterdir())
    stale_blob_tmp = tmp_dir / f".put-{victim_pid}-residue"
    stale_meta_tmp = meta_shard / f".meta-{victim_pid}-residue.tmp"
    stale_blob_tmp.write_bytes(b"half-written")
    stale_meta_tmp.write_text("{}")
    assert stale_blob_tmp.exists() and stale_meta_tmp.exists()

    # 重启（新进程等价于重新构造服务）：恢复 + 立即可读写
    svc = StorageService(tmp_root)
    assert not stale_blob_tmp.exists(), "死亡进程的 blob 暂存残留未被清理"
    assert not stale_meta_tmp.exists(), "死亡进程的 meta 暂存残留未被清理"
    log_case(
        "重启恢复-残留清理",
        victim_pid=victim_pid,
        结果="死亡进程暂存已清理,存活进程文件不受影响",
        依据="暂存文件名携带属主PID,仅清理PID已死亡者",
    )

    versions = svc.list_versions("victim")
    nums = [v.version for v in versions]
    assert nums == list(range(1, len(nums) + 1)), f"版本不连续: {nums}"
    assert len(nums) >= len(committed) - 1  # 上报后可能又提交了 1 次
    reported_by_ver = {c[2]: c for c in committed}
    for c in committed:
        _, _, ver_no, digest, content = c
        data, ver = svc.get_object("victim", version=ver_no)
        assert ver.digest == digest and data == content
    log_case(
        "重启恢复-版本与内容",
        版本序列=f"1..{nums[-1]}", 校验提交数=len(reported_by_ver),
        结果="版本连续,已提交内容全部可读且sha256自洽",
        依据="先落blob后原子发meta;崩溃只留孤儿,不留半成品",
    )

    # 恢复后立刻正常读写；显式 recover 幂等且无残留累积
    r = svc.put_object("victim", [b"after-recovery"])
    assert svc.get_object("victim")[0] == b"after-recovery"
    assert r.version.version == nums[-1] + 1
    cleaned = svc.recover()
    assert cleaned == (0, 0), f"恢复后仍有残留累积: {cleaned}"
    assert list(tmp_dir.iterdir()) == []
    log_case(
        "重启恢复-服务可用",
        新版本=r.version.version, 残留=cleaned,
        结果="恢复后读写正常,无残留随运行累积",
        依据="孤儿blob由GC回收;暂存按PID精确清理",
    )

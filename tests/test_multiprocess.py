"""多进程并发一致性测试（真正的独立 OS 进程，spawn 方式启动）。

覆盖：
1. 多进程并发写/删/回滚/读同一对象：版本号严格连续、不重不跳，
   每个成功返回对应的内容随时可按版本读回且 sha256 自洽；
2. 不同对象互不影响：各对象各自 1..N 连续，各自内容独立可校验；
3. GC 与多进程写入并发：存活内容（含历史版本引用）一个不丢，
   在途提交不会被当成垃圾；
4. 进程在写入过程中被 SIGKILL：重启恢复后版本仍连续、存活数据不丢、
   暂存/meta 半成品无残留且不累积，服务立刻可正常读写。

日志：worker 进程把“进程pid / 操作 / key / 版本 / 内容digest / 判定依据 /
结果”追加进共享日志文件，父进程在用例末尾整文件转储到 pytest 输出。
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import signal
import time
from pathlib import Path

import pytest

from app.hashing import sha256_hex
from app.service import StorageService

from .conftest import log_case

_CTX = mp.get_context("spawn")
_JOIN_TIMEOUT = 30.0


# ---------------------------------------------------------------------------
# worker 侧日志：结构化 JSON Lines，跨 spawn 进程安全（append 小行，O_APPEND）
# ---------------------------------------------------------------------------
def _wlog(log_path: str | None, op: str, **fields) -> None:
    if not log_path:
        return
    rec = {"ts": round(time.time(), 6), "pid": os.getpid(), "op": op, **fields}
    try:
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _dump_worker_log(log_path: Path) -> None:
    logger = logging.getLogger("oss.test.mp")
    if not log_path.is_file():
        return
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        logger.info("worker-log %s", rec)


def _join_all(procs: list[mp.Process], timeout: float = _JOIN_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    for p in procs:
        remaining = max(1.0, deadline - time.monotonic())
        p.join(remaining)
    alive = [p.pid for p in procs if p.is_alive()]
    if alive:
        for p in procs:
            if p.is_alive():
                p.kill()
        raise AssertionError(f"worker 进程超时未退出: {alive}")


# ---------------------------------------------------------------------------
# worker 入口（spawn 要求模块级可 pickle）
# ---------------------------------------------------------------------------
def _worker_pure_puts(root: str, key: str, pid_idx: int, rounds: int,
                      log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    for i in range(rounds):
        content = f"p{pid_idx}-c{i}".encode()
        digest = sha256_hex(content)
        result = svc.put_object(key, [content], expected_digest=digest)
        q.put(("put", pid_idx, result.version.version, digest, len(content)))
        _wlog(log_path, "put", key=key, version=result.version.version,
              digest=digest[:12], size=len(content),
              reason="内容sha256一致;对象锁串行化->分配到唯一连续版本", result="ok")


# ---------------------------------------------------------------------------
# 用例
# ---------------------------------------------------------------------------
def test_mp_skeleton_pure_puts(tmp_root: Path):
    """骨架冒烟：4 个独立进程各写 20 次同一对象，版本必须 1..80 连续。"""
    log_path = tmp_root.parent / "mp-skeleton.log.jsonl"
    n_proc, rounds = 4, 20
    q = _CTX.Queue()
    procs = [
        _CTX.Process(target=_worker_pure_puts,
                     args=(str(tmp_root), "hot", i, rounds, str(log_path), q))
        for i in range(n_proc)
    ]
    for p in procs:
        p.start()
    _join_all(procs)
    rows = []
    while not q.empty():
        rows.append(q.get())

    versions = sorted(r[2] for r in rows)
    log_case("多进程并发写-骨架", 进程数=n_proc, 每进程写=rounds,
             成功提交=len(rows), 版本范围=(versions[0], versions[-1]),
             依据="flock对象锁跨进程串行化,版本1..N不重不跳")
    assert len(rows) == n_proc * rounds
    assert versions == list(range(1, n_proc * rounds + 1))
    assert len(set(versions)) == len(versions)
    _dump_worker_log(log_path)


def _worker_mixed(root: str, key: str, pid_idx: int, rounds: int,
                  log_path: str | None, q) -> None:
    """写/读/删/回滚混合：每轮写后立即按精确版本读回；每 3 轮再删+回滚。"""
    from app.errors import ObjectDeletedError
    from app.service import StorageService
    svc = StorageService(root)
    for i in range(rounds):
        content = f"p{pid_idx}-i{i}".encode()
        digest = sha256_hex(content)
        r = svc.put_object(key, [content], expected_digest=digest)
        v = r.version.version
        q.put(("put", pid_idx, v, digest))
        _wlog(log_path, "put", key=key, version=v, digest=digest[:12],
              reason="对象锁串行化分配连续版本;blob先落盘meta后发布", result="ok")

        # 不变量 A：刚成功返回的精确版本立刻可被（任意进程视角）读到且内容自洽
        data, ver = svc.get_object(key, version=v)
        assert ver.version == v and sha256_hex(data) == digest, (v, digest)
        _wlog(log_path, "get@version", key=key, version=v, digest=digest[:12],
              reason="历史版本不可变,内容sha256必须等于版本记录digest", result="ok")

        # 读当前版本（可能正被别的进程删除）：要么内容自洽，要么明确已删除
        try:
            cur_data, cur = svc.get_object(key)
            assert sha256_hex(cur_data) == cur.digest
        except ObjectDeletedError:
            pass

        if i % 3 == 2:
            tv = svc.delete_object(key)
            q.put(("delete", pid_idx, tv.version))
            _wlog(log_path, "delete", key=key, tombstone=tv.version,
                  reason="墓碑占版本号,blob保留", result="ok")
            # 回滚到刚写的版本：该版本不可变，回滚记录的 digest 必须与内容一致
            rb = svc.rollback(key, v)
            assert rb.digest == digest
            rb_data, rb_ver = svc.get_object(key, version=rb.version)
            assert rb_ver.version == v and sha256_hex(rb_data) == digest
            q.put(("rollback", pid_idx, v))
            _wlog(log_path, "rollback", key=key, target=v, digest=digest[:12],
                  reason="复用历史引用,目标版本内容sha256自洽", result="ok")


def test_mp_mixed_put_delete_rollback_read(tmp_root: Path):
    log_path = tmp_root.parent / "mp-mixed.log.jsonl"
    n_proc, rounds = 5, 18
    # 父进程先建种子版本，保证 delete 永远不会面对“从不存在”的对象
    from app.service import StorageService
    seed = StorageService(tmp_root).put_object("mixed", [b"seed"]).version
    n_seed = 1

    q = _CTX.Queue()
    procs = [
        _CTX.Process(target=_worker_mixed,
                     args=(str(tmp_root), "mixed", i, rounds, str(log_path), q))
        for i in range(n_proc)
    ]
    for p in procs:
        p.start()
    _join_all(procs)
    puts, deletes, rollbacks = [], [], []
    while not q.empty():
        kind, idx, v, *rest = q.get()
        if kind == "put":
            puts.append((v, rest[0]))
        elif kind == "delete":
            deletes.append(v)
        else:
            rollbacks.append(v)

    svc = StorageService(tmp_root)
    state = svc.stat("mixed")
    nums = [v.version for v in state.versions]
    expected_total = n_seed + len(puts) + len(deletes)
    log_case(
        "多进程混合写删回滚读",
        进程数=n_proc, 每进程轮次=rounds,
        成功写=len(puts), 墓碑=len(deletes), 回滚=len(rollbacks),
        版本范围=(min(nums), max(nums)), 版本总数=len(nums),
        依据="同一对象全部变更跨进程串行->版本连续;回滚不占版本号",
    )
    # 版本号严格连续（写与墓碑都占号；回滚不占号）
    assert nums == list(range(1, expected_total + 1))
    assert len(set(nums)) == len(nums)

    # 每次成功写的版本，其最终元数据 digest 必须就是当时那份内容
    for v, digest in puts:
        rec = state.find(v)
        assert rec is not None and not rec.deleted
        assert rec.digest == digest, (v, rec.digest, digest)

    # 所有非墓碑版本：物理 blob 存在、按版本读回 sha256 自洽（历史不被覆盖）
    for rec in state.versions:
        if rec.deleted:
            continue
        assert svc.blob_store.verify(rec.digest) == rec.size
        data, got = svc.get_object("mixed", version=rec.version)
        assert got.digest == rec.digest and sha256_hex(data) == rec.digest

    # 每个墓碑记录都在连续版本序列中、无内容
    for v in deletes:
        rec = state.find(v)
        assert rec is not None and rec.deleted and rec.digest == ""
    _dump_worker_log(log_path)


def _worker_disjoint_keys(root: str, pid_idx: int, rounds: int,
                          log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    key = f"obj-{pid_idx}"
    for i in range(rounds):
        content = f"key{pid_idx}-v{i}".encode()
        r = svc.put_object(key, [content], expected_digest=sha256_hex(content))
        q.put((key, r.version.version, sha256_hex(content)))
        _wlog(log_path, "put", key=key, version=r.version.version,
              digest=r.version.digest[:12], reason="不同对象锁互不阻塞", result="ok")


def test_mp_distinct_objects_do_not_interfere(tmp_root: Path):
    log_path = tmp_root.parent / "mp-disjoint.log.jsonl"
    n_proc, rounds = 6, 25
    q = _CTX.Queue()
    procs = [
        _CTX.Process(target=_worker_disjoint_keys,
                     args=(str(tmp_root), i, rounds, str(log_path), q))
        for i in range(n_proc)
    ]
    for p in procs:
        p.start()
    _join_all(procs)
    by_key: dict[str, list[tuple[int, str]]] = {}
    while not q.empty():
        key, v, digest = q.get()
        by_key.setdefault(key, []).append((v, digest))

    svc = StorageService(tmp_root)
    log_case(
        "多进程不同对象互不影响",
        进程数=n_proc, 对象数=len(by_key), 每对象版本=rounds,
        依据="对象锁按key哈希分文件,不同key并发不串行",
    )
    assert set(by_key) == {f"obj-{i}" for i in range(n_proc)}
    for key, rows in by_key.items():
        nums = sorted(v for v, _ in rows)
        assert nums == list(range(1, rounds + 1)), key
        # 每个对象的内容按版本读回必须精确等于该对象自己写入的内容
        for v, digest in rows:
            data, got = svc.get_object(key, version=v)
            assert got.version == v and sha256_hex(data) == digest
    _dump_worker_log(log_path)


def _worker_gc_loop(root: str, stop_path: str, log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    n_runs = 0
    while not os.path.exists(stop_path):
        collected = svc.gc()  # 排他 gc 屏障：与全部变更互斥
        n_runs += 1
        q.put(("gc-run", os.getpid(), len(collected)))
        _wlog(log_path, "gc", collected=len(collected), run=n_runs,
              reason="排他gc屏障->无在途提交,任一版本引用即存活", result="ok")
        time.sleep(0.002)
    q.put(("gc-done", os.getpid(), n_runs))


def _worker_gc_writers(root: str, pid_idx: int, rounds: int,
                       log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    for i in range(rounds):
        # 同一批内容在不同 key/版本间共享，制造“历史引用”存活集合
        content = f"shared-{i % 7}-from-{pid_idx}".encode()
        r = svc.put_object(f"gc-key-{pid_idx % 3}", [content])
        q.put((f"gc-key-{pid_idx % 3}", r.version.version, r.version.digest))
        _wlog(log_path, "put", key=f"gc-key-{pid_idx % 3}",
              version=r.version.version, digest=r.version.digest[:12],
              reason="共享gc屏障:GC必须等在途提交结束", result="ok")


def test_mp_gc_concurrent_with_writes(tmp_root: Path):
    log_path = tmp_root.parent / "mp-gc.log.jsonl"
    stop_path = tmp_root.parent / "mp-gc-stop"
    n_writers, rounds = 6, 30
    q = _CTX.Queue()
    gc_proc = _CTX.Process(target=_worker_gc_loop,
                           args=(str(tmp_root), str(stop_path), str(log_path), q))
    writers = [
        _CTX.Process(target=_worker_gc_writers,
                     args=(str(tmp_root), i, rounds, str(log_path), q))
        for i in range(n_writers)
    ]
    gc_proc.start()
    for p in writers:
        p.start()
    _join_all(writers)
    Path(stop_path).write_text("stop")
    _join_all([gc_proc])

    events = []
    while not q.empty():
        events.append(q.get())
    puts = [e for e in events if e[0].startswith("gc-key-")]
    gc_runs = [e for e in events if e[0] == "gc-run"]

    svc = StorageService(tmp_root)
    # 核心不变量：每个已提交版本引用的 blob 必须仍在磁盘上且内容自洽
    missing = []
    per_key: dict[str, list[int]] = {}
    for key, v, digest in puts:
        per_key.setdefault(key, []).append(v)
        if not svc.blob_store.exists(digest):
            missing.append((key, v, digest))
        else:
            assert svc.blob_store.verify(digest) >= 0
    log_case(
        "GC与多进程写入并发",
        gc执行次数=len(gc_runs), 成功提交=len(puts),
        被误删的存活blob=len(missing),
        依据="gc排他屏障与变更互斥;被任一历史版本引用即存活",
    )
    assert missing == []
    # 每个对象版本各自连续
    for key, nums in per_key.items():
        assert sorted(nums) == list(range(1, len(nums) + 1)), key

    # 全部提交内容按精确版本读回 sha256 自洽（读者从未拿到残缺/误报缺失）
    for key, v, digest in puts:
        data, got = svc.get_object(key, version=v)
        assert got.digest == digest and sha256_hex(data) == digest

    # 收敛：并发结束后做一次 GC，磁盘上只应留下被引用内容（垃圾不累积）
    final_garbage = svc.gc()
    on_disk = svc.blob_store.all_digests()
    referenced = svc.catalog.all_referenced_digests()
    log_case(
        "GC并发-收敛无残留",
        最后一轮回收=len(final_garbage), 磁盘blob=len(on_disk),
        被引用blob=len(referenced),
        依据="孤儿blob最终被gc回收;存活集合与磁盘一致",
    )
    assert on_disk == referenced
    _dump_worker_log(log_path)


# ---------------------------------------------------------------------------
# 强杀恢复场景
# ---------------------------------------------------------------------------
def _worker_crash_writers(root: str, pid_idx: int, duration: float,
                          log_path: str | None, q) -> None:
    """持续写同一热点对象 + 自己的私有对象，直到被父进程 SIGKILL。"""
    import queue as _queue
    from app.service import StorageService
    svc = StorageService(root)
    deadline = time.monotonic() + duration
    i = 0

    def report(item) -> None:
        try:
            q.put(item, timeout=1)
        except (_queue.Full, OSError, EOFError):
            pass

    while time.monotonic() < deadline:
        # 较大且变化的载荷，拉长 tmp 写入窗口，使强杀更可能落在不同阶段
        content = f"hot-p{pid_idx}-i{i}-".encode() + b"z" * (4096 * (i % 8 + 1))
        r = svc.put_object("crash-hot", [content])
        report(("put", "crash-hot", r.version.version, r.version.digest))
        _wlog(log_path, "put", key="crash-hot", version=r.version.version,
              digest=r.version.digest[:12], size=r.version.size,
              reason="持对象锁分配版本并原子发布;被杀则锁由内核释放", result="ok")
        if i % 4 == 0:
            c2 = f"own{pid_idx}-seq{i}".encode()
            r2 = svc.put_object(f"crash-own-{pid_idx}", [c2])
            report(("put", f"crash-own-{pid_idx}", r2.version.version, r2.version.digest))
        i += 1


def _worker_fresh_process_rw(root: str, expected_max: int,
                             log_path: str | None, q) -> None:
    """重启后由一个全新进程验证：可立刻读、可立刻写，版本接着连续。"""
    from app.service import StorageService
    svc = StorageService(root)
    state = svc.stat("crash-hot")
    nums = [v.version for v in state.versions]
    contiguous = nums == list(range(1, len(nums) + 1))
    data, _ = svc.get_object("crash-hot")  # 立刻读当前
    r = svc.put_object("crash-hot", [b"fresh-process-after-reboot"])
    new_data, new_ver = svc.get_object("crash-hot")
    q.put((
        contiguous,
        sha256_hex(data) == state.visible().digest if state.visible() else True,
        r.version.version == expected_max + 1,
        sha256_hex(new_data) == new_ver.digest,
    ))
    _wlog(log_path, "reboot-rw", expected_next=expected_max + 1,
          got=r.version.version, reason="恢复后全新进程立即可读可写且版本接续",
          result="ok" if r.version.version == expected_max + 1 else "bad")


def _drain_queue(q) -> list:
    import queue as _queue
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except (_queue.Empty, OSError, EOFError):
            break
    return items


def test_mp_sigkill_then_restart_recovery(tmp_root: Path):
    import random
    log_path = tmp_root.parent / "mp-crash.log.jsonl"
    rng = random.Random(20261004)

    svc = StorageService(tmp_root)
    # 种子对象：跨所有崩溃轮次必须始终完好可读
    for k in range(3):
        svc.put_object(f"seed-{k}", [(f"seed-content-{k}" * 100).encode()])

    committed: list[tuple[str, int, str]] = []
    rounds = 3
    n_writers = 4
    hot_max = 0

    for rnd in range(1, rounds + 1):
        q = _CTX.Queue()
        procs = [
            _CTX.Process(target=_worker_crash_writers,
                         args=(str(tmp_root), i, 2.0, str(log_path), q))
            for i in range(n_writers)
        ]
        for p in procs:
            p.start()
        time.sleep(rng.uniform(0.15, 0.4))  # 让各进程处在不同写入阶段
        for p in procs:
            p.kill()  # SIGKILL：不给清理机会，最严苛的强杀
        _join_all(procs)
        killed = [(-p.exitcode if p.exitcode is not None and p.exitcode < 0 else p.exitcode)
                  for p in procs]
        for item in _drain_queue(q):
            if item[0] == "put":
                committed.append((item[1], item[2], item[3]))
            else:
                raise AssertionError(f"worker 非预期上报: {item}")
        assert all(code == int(signal.SIGKILL) for code in killed), killed

        # —— “重启”：在同一数据目录上新构造服务（构造即在排他 gc 屏障下恢复）——
        svc = StorageService(tmp_root)

        staging_left = list(svc.blob_store.tmp_dir.iterdir())
        meta_tmp_left = list(tmp_root.glob("meta/**/.meta-*.tmp"))
        log_case(
            f"第{rnd}轮强杀-重启恢复无残留",
            被杀进程=killed, 已确认提交数=len(committed),
            tmp暂存残留=len(staging_left), meta半成品残留=len(meta_tmp_left),
            依据="启动时持排他gc屏障清扫tmp与.meta-*.tmp;被杀者锁由内核释放",
        )
        assert staging_left == []
        assert meta_tmp_left == []

        # 热点对象版本严格连续（被杀在临界区里也不跳号：锁释放后别人重算版本号）
        state = svc.stat("crash-hot")
        nums = [v.version for v in state.versions]
        assert nums == list(range(1, len(nums) + 1)), nums[:5]
        # 被杀前必须确实发生过提交，否则“连续”断言没有意义
        assert len(nums) > hot_max, (rnd, len(nums), hot_max)
        hot_max = len(nums)

        # 截至本轮，所有“成功返回过”的提交必须还在、且按版本读回内容自洽
        for key, v, digest in committed:
            st = svc.stat(key)
            rec = st.find(v)
            assert rec is not None and rec.digest == digest, (key, v)
            data, got = svc.get_object(key, version=v)
            assert got.digest == digest and sha256_hex(data) == digest

        # 每个非墓碑版本物理 blob 完好；种子对象完好
        for rec in state.versions:
            assert svc.blob_store.verify(rec.digest) == rec.size
        for k in range(3):
            data, ver = svc.get_object(f"seed-{k}")
            assert sha256_hex(data) == ver.digest

        # 恢复后立刻写：版本必须接在 max 之后，且立刻可读
        post = svc.put_object("crash-hot", [f"post-recovery-round-{rnd}".encode()])
        assert post.version.version == hot_max + 1
        data, got = svc.get_object("crash-hot", version=post.version.version)
        assert sha256_hex(data) == got.digest
        hot_max += 1
        committed.append(("crash-hot", post.version.version, post.version.digest))

        # 为下一轮确定性地植入两类半成品，验证下一次启动能“发现并清理”
        if rnd < rounds:
            (svc.blob_store.tmp_dir / ".put-zombie").write_bytes(b"deadbeef" * 64)
            any_shard = svc.catalog.meta_dir / "99"
            any_shard.mkdir(parents=True, exist_ok=True)
            (any_shard / ".meta-zombie.tmp").write_text('{"broken":', encoding="utf-8")

    # 末轮：由一个全新 spawn 进程做“重启后立即可读可写”的最终证明
    q = _CTX.Queue()
    verifier = _CTX.Process(
        target=_worker_fresh_process_rw,
        args=(str(tmp_root), hot_max, str(log_path), q),
    )
    verifier.start()
    _join_all([verifier])
    verdict = _drain_queue(q)
    assert verdict == [(True, True, True, True)], verdict
    log_case(
        "强杀恢复-全新进程立即可读可写",
        接续版本=hot_max + 1,
        判定=verdict,
        依据="版本连续/当前内容自洽/新版本接续/新内容可校验 四项全True",
    )

    # 收敛：被杀在“blob已落、meta未发”之间产生的孤儿由 GC 回收，磁盘==引用集
    svc = StorageService(tmp_root)
    orphans = svc.gc()
    on_disk = svc.blob_store.all_digests()
    referenced = svc.catalog.all_referenced_digests()
    log_case(
        "强杀恢复-孤儿收敛且存活数据不丢",
        孤儿回收=len(orphans), 磁盘blob=len(on_disk), 存活引用=len(referenced),
        依据="孤儿=任何版本都不引用;存活集合与磁盘一致后种子仍可读",
    )
    assert on_disk == referenced
    for k in range(3):
        data, ver = svc.get_object(f"seed-{k}")
        assert sha256_hex(data) == ver.digest
    assert list(svc.blob_store.tmp_dir.iterdir()) == []
    assert list(tmp_root.glob("meta/**/.meta-*.tmp")) == []
    _dump_worker_log(log_path)

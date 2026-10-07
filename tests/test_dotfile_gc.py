"""点号开头（及各种特殊字符）对象名与垃圾回收的回归用例。

历史缺陷：元数据文件名是 quote(key)+".json"，而 quote 不编码 '.'，
因此 .env / .gitignore / .config/settings.json 这类对象的元数据文件名
本身以点号开头；Catalog.all_keys() 旧实现按“文件名以点号开头即隐藏文件”
跳过它们，导致 GC 的可达集合漏掉这些对象的全部版本引用，
一次 gc() 就把仍被版本引用的 blob 删掉，而版本列表里版本还在，
随后读当前/历史版本统一报 content_corrupted（内容缺失）。

本文件覆盖：
1. 点号开头（含带路径的嵌套名）及斜杠/百分号/中文等特殊名字对象：
   多版本写入 -> GC 前后每个版本都能按版本读回、sha256 一致、版本连续；
   真正无引用的孤儿仍被回收；连续多次 GC 结果一致收敛；重启后依旧成立；
   旧版本写出的数据目录（磁盘布局完全一致，无迁移）直接打开即可读、可回收。
2. GC 与多个独立进程同时写同一批特殊名字对象并发：
   成功提交的版本内容在 GC 前后均可按版本读回、校验一致、版本连续，
   存活 blob 零误删，结束后 GC 收敛（磁盘集合 == 引用集合）且多次一致。

日志（-s 实时可见，失败时 pytest 也会带出）：每个对象、每个版本、
每份内容的 digest、存活/回收的判定依据与结果均逐行打印。
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from pathlib import Path

from app.hashing import sha256_hex
from app.service import StorageService

from .conftest import log_case

# 点号开头（含嵌套路径）为主，兼顾其余特殊字符：斜杠、百分号、中文、
# 连续点号等；与普通名字一视同仁。
DOT_KEYS: list[str] = [
    ".env",
    ".gitignore",
    ".config/settings.json",
    ".a/.b/..hidden",
    "normal.txt",
    "dir/sub/object.bin",
    "100%done.txt",
    "中文/配置.env",
    ".百 分%/x",
]


def test_dotfile_named_objects_survive_gc(tmp_root: Path) -> None:
    """点号/特殊名字对象多版本写入后反复 GC：内容按版本不丢、垃圾仍被回收。"""
    svc = StorageService(tmp_root)

    # key -> {版本号: (内容, digest)}，作为后续一切校验的期望值基准
    expected: dict[str, dict[int, tuple[bytes, str]]] = {k: {} for k in DOT_KEYS}

    n_versions = 3
    for round_no in range(1, n_versions + 1):
        for key in DOT_KEYS:
            content = f"内容@{key}#v{round_no}".encode()
            digest = sha256_hex(content)
            result = svc.put_object(key, [content], expected_digest=digest)
            v = result.version.version
            expected[key][v] = (content, digest)
            log_case(
                "写入特殊名字对象",
                对象=key, 版本=v, 内容digest=digest[:12], size=len(content),
                判定依据="内容sha256与客户端声明一致;版本号在对象锁下连续分配",
                结果="ok",
            )

    # 真正的垃圾：直接写进 blob 层、不被任何版本引用的孤儿
    orphan_digests = {
        svc.blob_store.put([b"orphan-no-reference-1"]).digest,
        svc.blob_store.put([b"orphan-no-reference-2"]).digest,
    }
    log_case(
        "植入无引用孤儿blob",
        孤儿=[d[:12] for d in sorted(orphan_digests)],
        判定依据="仅落blobs/、无任何meta版本记录引用->应被gc回收",
    )

    # GC 前的基线：所有对象都被枚举、所有版本引用都进入可达集合
    listed = set(svc.catalog.all_keys())
    assert listed == set(DOT_KEYS), sorted(set(DOT_KEYS) ^ listed)
    referenced_before = svc.catalog.all_referenced_digests()
    assert orphan_digests.isdisjoint(referenced_before)

    def _assert_every_version_readable(service: StorageService, tag: str) -> None:
        """逐对象逐版本读回，断言内容 sha256 一致、版本 1..N 连续。"""
        for key in DOT_KEYS:
            state = service.stat(key)
            nums = [v.version for v in state.versions]
            assert nums == list(range(1, n_versions + 1)), (tag, key, nums)
            log_case(
                f"{tag}-版本连续",
                对象=key, 版本序列=nums,
                判定依据="同对象变更跨进程/多轮严格串行,版本不重不跳",
            )
            for v, (content, digest) in expected[key].items():
                data, got = service.get_object(key, version=v)
                assert got.version == v and got.digest == digest, (tag, key, v)
                assert sha256_hex(data) == digest and data == content, (tag, key, v)
                assert service.blob_store.verify(digest) == len(content), (tag, key, v)
                log_case(
                    f"{tag}-按版本读回校验",
                    对象=key, 版本=v, 内容digest=digest[:12],
                    size=len(content), 判定依据="重算sha256==版本记录digest且字节相等",
                    结果="存活可读",
                )

    _assert_every_version_readable(svc, "GC前")

    # 第 1 次 GC：只应回收两个孤儿，点号对象的所有版本内容一个都不能少
    collected_first = set(svc.gc())
    log_case(
        "第1次GC",
        回收=[d[:12] for d in sorted(collected_first)],
        回收数=len(collected_first),
        判定依据="任一版本记录(含历史)引用即存活;无任何引用才删除",
    )
    assert collected_first == orphan_digests
    _assert_every_version_readable(svc, "GC后")

    # 连续回收必须稳定收敛：后续 GC 不再删除任何东西，磁盘集合 == 引用集合
    later_runs = [set(svc.gc()) for _ in range(3)]
    for i, collected in enumerate(later_runs, start=2):
        log_case(
            f"第{i}次GC",
            回收数=len(collected),
            判定依据="上一轮已收敛;存活集合不随枚举顺序/时点变化",
            结果="收敛" if not collected else "未收敛",
        )
        assert collected == set()
    on_disk = svc.blob_store.all_digests()
    referenced = svc.catalog.all_referenced_digests()
    assert on_disk == referenced
    log_case(
        "GC收敛一致",
        磁盘blob数=len(on_disk), 被引用blob数=len(referenced),
        判定依据="磁盘集合==任一版本引用集合;孤儿已清空且存活内容零误删",
    )

    # 回滚到点号对象的历史版本后再 GC：历史内容仍须存活可读
    svc.rollback(".env", 1)
    assert svc.gc() == []
    data, ver = svc.get_object(".env")
    assert ver.version == 1 and data == expected[".env"][1][0]
    log_case(
        "回滚历史版本后GC",
        对象=".env", 当前版本=ver.version, 内容digest=ver.digest[:12],
        判定依据="回滚仅切current指针;历史版本引用仍在->blob存活",
        结果="ok",
    )

    # 重启：全新服务打开同一数据目录，旧数据直接可读、可回收，无需改名/迁移
    rebooted = StorageService(tmp_root)
    assert rebooted.gc() == []
    assert set(rebooted.catalog.all_keys()) == set(DOT_KEYS)
    _assert_every_version_readable(rebooted, "重启后")
    log_case(
        "重启兼容",
        对象数=len(DOT_KEYS),
        判定依据="磁盘布局未变(quote(key).json),旧目录直接打开即可读可回收",
        结果="ok",
    )


# ---------------------------------------------------------------------------
# 多进程并发：GC 与多个独立进程同时写同一批点号/特殊名字对象
# 与 test_multiprocess.py 同款约定：spawn 真进程 + JSONL 共享日志 + Queue 汇总
# ---------------------------------------------------------------------------
_CTX = mp.get_context("spawn")

# 同一批并发写的对象：全部点号开头（含嵌套路径），外加带百分号/中文的名字
MP_DOT_KEYS: list[str] = [
    ".env",
    ".gitignore",
    ".config/settings.json",
    ".secret/nested/file.cfg",
    "100%点号/.env中文",
]


def _wlog(log_path: str | None, op: str, **fields) -> None:
    """worker 侧结构化日志（O_APPEND 小行，跨 spawn 进程安全）。"""
    if not log_path:
        return
    rec = {"ts": round(time.time(), 6), "pid": os.getpid(), "op": op, **fields}
    try:
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _dump_worker_log(log_path: Path) -> None:
    import logging

    if not log_path.is_file():
        return
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        logging.getLogger("oss.test.mp.dotfile").info("worker-log %s", rec)


def _join_all(procs: list, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    for p in procs:
        p.join(max(1.0, deadline - time.monotonic()))
    alive = [p.pid for p in procs if p.is_alive()]
    if alive:
        for p in procs:
            if p.is_alive():
                p.kill()
        raise AssertionError(f"worker 进程超时未退出: {alive}")


def _worker_dotfile_writers(root: str, pid_idx: int, rounds: int,
                            log_path: str | None, q) -> None:
    """每个 writer 对同一批点号对象各写 rounds 个互不相同的新版本。"""
    from app.hashing import sha256_hex
    from app.service import StorageService

    svc = StorageService(root)
    for i in range(rounds):
        for key in MP_DOT_KEYS:
            content = f"dotfile-key={key}|pid={pid_idx}|seq={i}".encode()
            digest = sha256_hex(content)
            result = svc.put_object(key, [content], expected_digest=digest)
            v = result.version.version
            q.put((key, v, digest, len(content)))
            _wlog(
                log_path, "put", key=key, version=v, digest=digest[:12],
                size=len(content),
                reason="共享gc屏障:GC必须等本次提交结束;对象锁串行化->版本连续",
                result="ok",
            )
            # 刚成功返回的版本立刻按版本读回，读者绝不能拿到残缺/误删内容
            data, got = svc.get_object(key, version=v)
            assert got.digest == digest and sha256_hex(data) == digest
            _wlog(
                log_path, "get@version", key=key, version=v,
                digest=digest[:12],
                reason="被任一版本引用的blob在并发GC下必须存活;sha256自洽",
                result="ok",
            )


def _worker_dotfile_gc_loop(root: str, stop_path: str,
                            log_path: str | None, q) -> None:
    """与写入并发地反复回收：排他屏障保证不会误删在途/被引用内容。"""
    from app.service import StorageService

    svc = StorageService(root)
    n_runs = 0
    while not os.path.exists(stop_path):
        collected = svc.gc()
        n_runs += 1
        q.put(("gc-run", len(collected)))
        _wlog(
            log_path, "gc", run=n_runs, collected=len(collected),
            reason="排他gc屏障->无在途提交;点号对象元数据也在可达集合内",
            result="ok",
        )
        time.sleep(0.002)
    q.put(("gc-done", n_runs))


def test_dotfile_named_objects_survive_concurrent_gc(tmp_root: Path) -> None:
    """GC 与多进程同时写同一批点号对象：版本连续、内容不丢、收敛且幂等。"""
    log_path = tmp_root.parent / "dotfile-gc.log.jsonl"
    stop_path = tmp_root.parent / "dotfile-gc-stop"

    # 父进程先建种子版本并植入无引用孤儿：种子在并发 GC 下必须始终存活，
    # 孤儿与任何点号对象无关、不被引用，并发期可被任意一轮 GC 回收，
    # 最终收敛时磁盘上不得残留。
    seed = StorageService(tmp_root)
    seed_digests: dict[str, str] = {}
    for key in MP_DOT_KEYS:
        content = f"seed-content-of-{key}".encode()
        r = seed.put_object(key, [content])
        seed_digests[key] = r.version.digest
        log_case(
            "并发前种子版本",
            对象=key, 版本=r.version.version, 内容digest=r.version.digest[:12],
            判定依据="种子版本在整个并发GC期间被引用->必须存活",
        )
    orphan_digests = {
        seed.blob_store.put([b"dotfile-test-orphan-A"]).digest,
        seed.blob_store.put([b"dotfile-test-orphan-B"]).digest,
    }
    log_case(
        "并发前植入孤儿",
        孤儿=[d[:12] for d in sorted(orphan_digests)],
        判定依据="无任何版本引用->并发期/最终GC必须回收干净",
    )

    n_writers, rounds = 4, 6
    q: mp.Queue = _CTX.Queue()
    gc_procs = [
        _CTX.Process(
            target=_worker_dotfile_gc_loop,
            args=(str(tmp_root), str(stop_path), str(log_path), q),
        )
        for _ in range(2)
    ]
    writers = [
        _CTX.Process(
            target=_worker_dotfile_writers,
            args=(str(tmp_root), i, rounds, str(log_path), q),
        )
        for i in range(n_writers)
    ]
    for p in gc_procs:
        p.start()
    for p in writers:
        p.start()
    _join_all(writers)
    Path(stop_path).write_text("stop", encoding="utf-8")
    _join_all(gc_procs)

    events: list = []
    while not q.empty():
        events.append(q.get())
    commits = [e for e in events if e[0] != "gc-run" and e[0] != "gc-done"]
    gc_runs = [e for e in events if e[0] == "gc-run"]
    log_case(
        "并发阶段结束",
        writer进程=n_writers, 每进程每对象写=rounds,
        成功提交=len(commits), gc执行轮次=len(gc_runs),
        判定依据="flock屏障+对象锁:提交与GC互不穿透",
    )
    assert len(commits) == n_writers * rounds * len(MP_DOT_KEYS)

    svc = StorageService(tmp_root)

    # 1) 每个对象版本严格 1..N 连续（种子占 v1，其余为并发提交）
    per_key: dict[str, dict[int, str]] = {k: {1: seed_digests[k]} for k in MP_DOT_KEYS}
    for key, v, digest, size in commits:
        assert key in per_key
        assert v not in per_key[key], (key, v, "重复版本号")
        per_key[key][v] = digest
        # 成功提交时引用的 blob 必须物理存活且完好（并发 GC 零误删的核心断言）
        assert svc.blob_store.exists(digest), (key, v, digest)
        assert svc.blob_store.verify(digest) == size, (key, v, digest)

    for key, versions in per_key.items():
        nums = sorted(versions)
        assert nums == list(range(1, len(nums) + 1)), (key, nums)
        state = svc.stat(key)
        assert [x.version for x in state.versions] == nums, key
        log_case(
            "并发后版本连续",
            对象=key, 版本数=len(nums), 版本范围=(nums[0], nums[-1]),
            判定依据="同对象所有变更跨进程串行;GC不占版本号",
        )

    # 2) 每个已提交版本（含种子与全部历史版本）按版本读回，内容 sha256 一致
    for key, versions in per_key.items():
        for v, digest in versions.items():
            data, got = svc.get_object(key, version=v)
            assert got.version == v and got.digest == digest, (key, v)
            assert sha256_hex(data) == digest, (key, v, digest)
            log_case(
                "并发后按版本读回",
                对象=key, 版本=v, 内容digest=digest[:12],
                判定依据="版本记录digest==重算内容sha256;存活判定含历史版本",
                结果="内容一致",
            )

    # 3) 真正无引用内容被回收：最终 GC 后磁盘集合 == 全量版本引用集合
    final_collected = set(svc.gc())
    on_disk = svc.blob_store.all_digests()
    referenced = svc.catalog.all_referenced_digests()
    log_case(
        "并发后最终GC",
        本轮回收=[d[:12] for d in sorted(final_collected)],
        磁盘blob数=len(on_disk), 被引用blob数=len(referenced),
        判定依据="枚举含点号开头对象->任一版本引用即存活;孤儿必被回收",
    )
    assert on_disk == referenced
    assert orphan_digests.isdisjoint(on_disk)
    for digest in referenced:
        assert digest in on_disk and svc.blob_store.exists(digest)

    # 4) 连续回收稳定一致：再来两次，结果都必须是空集且集合不再变化
    assert svc.gc() == []
    assert svc.gc() == []
    assert svc.blob_store.all_digests() == referenced
    log_case(
        "并发后连续GC收敛",
        判定依据="磁盘==引用后重复GC恒为空;不随时点/枚举顺序抖动",
        结果="稳定收敛",
    )

    _dump_worker_log(log_path)

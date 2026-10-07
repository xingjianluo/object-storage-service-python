"""点号开头（含嵌套路径）对象的回收存活回归测试。

背景缺陷：Catalog.all_keys() 曾按“文件名以点号开头”跳过元数据文件，
而 quote() 不会编码 '.'，导致 .env / .config/settings.json 这类对象的
元数据文件恰好以点号开头、被枚举漏掉 -> GC 可达集合漏掉其引用 ->
存活 blob 被误回收，之后按当前/指定版本读都报内容缺失。

覆盖：
1. 点号开头（含 .config/settings.json 这类嵌套名）对象写入后回收：
   回收前后每个版本都可读回、sha256 自洽、版本号连续；
2. 斜杠/点号/百分号/中文等特殊字符名字一视同仁；
3. 重启（重新打开同一数据目录）+ 反复回收后仍不丢内容；
4. 真正无引用的孤儿 blob 仍被回收干净，连续回收结果一致收敛；
5. 旧格式数据目录（手工放置 meta/blob、无 locks 目录）直接打开即可读、
   可正常参与回收，无需改名或迁移；
6. GC 与多个进程同时写同一批点号对象并发：存活内容一个不丢、版本连续、
   回收结果收敛。

日志：每个断言点都打印 对象key / 版本version / 内容digest / 判定依据 / 结果；
多进程用例中 worker 以 JSON Lines 写共享日志，父进程末尾统一转储。
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import time
from pathlib import Path

import pytest

from app.catalog import Catalog
from app.hashing import sha256_hex
from app.service import StorageService

from .conftest import log_case

_CTX = mp.get_context("spawn")
_JOIN_TIMEOUT = 60.0

# 点号开头（含嵌套）+ 各类特殊字符名字，必须与普通名字一视同仁
DOT_KEYS = [
    ".env",
    ".gitignore",
    ".config/settings.json",
    ".config/.secret",
    "dir/.hidden",
]
SPECIAL_KEYS = [
    "百分号%25对象.txt",
    "中文目录/配置.文件.json",
    "a/b/c/deep.txt",
    "normal",
]


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _put_versions(svc: StorageService, key: str, contents: list[bytes]) -> list[str]:
    """依次写入若干版本，返回各版本内容 digest；并打印每版判定日志。"""
    digests = []
    for content in contents:
        digest = sha256_hex(content)
        result = svc.put_object(key, [content], expected_digest=digest)
        digests.append(digest)
        log_case("写入版本", key=key, version=result.version.version,
                 digest=digest[:12], 依据="内容sha256与客户端声明一致", 结果="ok")
    return digests


def _assert_all_versions_readable(svc: StorageService, key: str,
                                  contents: list[bytes], digests: list[str],
                                  阶段: str) -> None:
    """逐版本读回并校验：版本连续、digest 与元数据一致、内容 sha256 自洽。"""
    state = svc.stat(key)
    nums = [v.version for v in state.versions]
    assert nums == list(range(1, len(contents) + 1)), (key, nums)
    for i, (content, digest) in enumerate(zip(contents, digests, strict=True)):
        version = i + 1
        rec = state.find(version)
        assert rec is not None and not rec.deleted and rec.digest == digest, (
            key, version)
        data, ver = svc.get_object(key, version=version)
        assert ver.version == version and ver.digest == digest
        assert data == content and sha256_hex(data) == digest
        log_case("按版本读回校验", 阶段=阶段, key=key, version=version,
                 digest=digest[:12], 依据="读回内容sha256==版本记录digest==写入内容",
                 结果="ok")
    # 当前版本也必须可读且等于最后一次写入
    cur_data, cur = svc.get_object(key)
    assert cur.version == len(contents) and cur_data == contents[-1]
    log_case("读当前版本", 阶段=阶段, key=key, version=cur.version,
             digest=cur.digest[:12], 依据="current指向最后写入版本", 结果="ok")


# ---------------------------------------------------------------------------
# 1. 单元级：all_keys 必须枚举点号开头与特殊字符名字，且不漏枚举/不误枚举
# ---------------------------------------------------------------------------
def test_catalog_all_keys_includes_dot_and_special_names(tmp_root: Path):
    catalog = Catalog(tmp_root)
    svc = StorageService(tmp_root)
    keys = DOT_KEYS + SPECIAL_KEYS
    for key in keys:
        svc.put_object(key, [f"content-of-{key}".encode()])

    # 制造一个未发布的半成品临时文件：必须不被枚举
    shard = catalog.meta_dir / "ab"
    shard.mkdir(parents=True, exist_ok=True)
    (shard / ".meta-halfdone.tmp").write_text('{"broken":', encoding="utf-8")

    found = catalog.all_keys()
    log_case("all_keys枚举", 期望=sorted(keys), 实际=sorted(found),
             依据="点号开头/斜杠/百分号/中文名字与普通名字同等枚举;"
                  ".meta-*.tmp半成品只靠后缀过滤", 结果="ok" if sorted(found) == sorted(keys) else "bad")
    assert sorted(found) == sorted(keys)
    assert not any(name.startswith(".meta-") for name in found)


# ---------------------------------------------------------------------------
# 2. 点号开头对象写入后回收：存活内容不丢、垃圾仍被回收、连续回收收敛、重启无恙
# ---------------------------------------------------------------------------
def test_dotkey_survives_gc_restart_and_repeated_gc(tmp_root: Path, caplog):
    svc = StorageService(tmp_root)
    keys = DOT_KEYS + SPECIAL_KEYS
    # 每个对象写 3 个版本，制造“历史版本引用”的存活集合
    written: dict[str, tuple[list[bytes], list[str]]] = {}
    for key in keys:
        contents = [f"{key}-v{i}-payload".encode() for i in range(1, 4)]
        digests = _put_versions(svc, key, contents)
        written[key] = (contents, digests)

    # 植入一份真正无引用的孤儿 blob（模拟“blob 已落盘、meta 未发布”的崩溃残留）
    orphan_content = b"orphan-blob-no-version-references-me"
    orphan = svc.blob_store.put([orphan_content])
    log_case("植入孤儿blob", digest=orphan.digest[:12],
             依据="任何版本记录都不引用它,必须可被回收", 结果="ok")

    with caplog.at_level(logging.INFO, logger="oss.service"):
        collected = svc.gc()
    referenced = svc.catalog.all_referenced_digests()
    log_case("第一次GC", 回收数=len(collected), 磁盘blob=len(svc.blob_store.all_digests()),
             被引用=len(referenced),
             依据="任一版本(含历史)引用即存活;孤儿=磁盘集合-引用集合", 结果="ok")
    # 孤儿被回收，且只回收了孤儿；点号对象的每份内容都活着
    assert collected == [orphan.digest]
    assert not svc.blob_store.exists(orphan.digest)
    for key, (_, digests) in written.items():
        for digest in digests:
            assert svc.blob_store.exists(digest), (key, digest)
    # GC 日志里能看到判定依据
    assert any("GC" in r.getMessage() and "依据" in r.getMessage()
               for r in caplog.records), "GC 日志缺少判定依据"

    # 回收后：每个点号/特殊名字的每个版本都可读回且内容一致
    for key, (contents, digests) in written.items():
        _assert_all_versions_readable(svc, key, contents, digests, 阶段="首次GC后")

    # 重启（同一数据目录重新打开服务）后再反复回收：不丢内容、结果收敛
    svc = StorageService(tmp_root)
    for round_no in (2, 3):
        again = svc.gc()
        on_disk = svc.blob_store.all_digests()
        referenced = svc.catalog.all_referenced_digests()
        log_case(f"重启后第{round_no}次GC", 本轮回收=len(again),
                 磁盘blob=len(on_disk), 被引用=len(referenced),
                 依据="稳定截面下磁盘集合==引用集合,连续回收结果一致收敛",
                 结果="ok" if again == [] and on_disk == referenced else "bad")
        assert again == [], f"第{round_no}次回收仍回收到内容: {again}"
        assert on_disk == referenced
    for key, (contents, digests) in written.items():
        _assert_all_versions_readable(svc, key, contents, digests, 阶段="重启+反复GC后")


# ---------------------------------------------------------------------------
# 3. 兼容性：旧数据目录（手工放置 meta/blob、无 locks/）直接打开即可读、可回收
# ---------------------------------------------------------------------------
def test_legacy_dir_with_dotkey_opens_and_gc_directly(tmp_root: Path):
    # 模拟旧版本留下的数据目录：只有 blobs/ 与 meta/，没有 locks/
    catalog = Catalog(tmp_root)
    from app.catalog import ObjectState, Version
    from app.blobstore import BlobStore

    blobs = BlobStore(tmp_root)
    legacy: dict[str, tuple[bytes, str]] = {}
    for key in (".env", ".config/settings.json", "中文/旧.配置"):
        content = f"legacy-{key}".encode()
        stored = blobs.put([content])
        state = ObjectState(key=key)
        state.versions.append(Version(version=1, digest=stored.digest,
                                      size=stored.size, created_at=1.0))
        state.current = 1
        catalog.store(state)
        legacy[key] = (content, stored.digest)
    assert not (tmp_root / "locks").exists()

    # 直接打开：无需改名/迁移，能读、能写、能参与回收
    svc = StorageService(tmp_root)
    for key, (content, digest) in legacy.items():
        data, ver = svc.get_object(key, version=1)
        assert data == content and sha256_hex(data) == digest
        log_case("旧目录直接打开读", key=key, version=ver.version,
                 digest=digest[:12], 依据="旧meta文件名原样识别,无需迁移", 结果="ok")
    orphan = svc.blob_store.put([b"legacy-orphan"])
    collected = svc.gc()
    assert collected == [orphan.digest]
    for key, (content, digest) in legacy.items():
        assert svc.blob_store.exists(digest)
        data, _ = svc.get_object(key)
        assert data == content
    # 旧对象上继续写新版本：版本接续连续
    r = svc.put_object(".env", [b"legacy-env-v2"])
    assert r.version.version == 2
    assert svc.gc() == []
    log_case("旧目录参与回收并续写", key=".env", 新版本=r.version.version,
             孤儿回收=collected == [orphan.digest],
             依据="旧目录打开后锁目录按需创建,回收/续写行为与新目录一致", 结果="ok")


# ---------------------------------------------------------------------------
# 4. GC 与多进程并发写同一批点号对象
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
    logger = logging.getLogger("oss.test.dotkey.mp")
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
        p.join(max(1.0, deadline - time.monotonic()))
    alive = [p.pid for p in procs if p.is_alive()]
    if alive:
        for p in procs:
            if p.is_alive():
                p.kill()
        raise AssertionError(f"worker 进程超时未退出: {alive}")


def _worker_dotkey_writer(root: str, keys: list[str], pid_idx: int, rounds: int,
                          log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    for i in range(rounds):
        for key in keys:
            content = f"p{pid_idx}-{key}-round{i}".encode()
            digest = sha256_hex(content)
            r = svc.put_object(key, [content], expected_digest=digest)
            q.put((key, r.version.version, digest))
            _wlog(log_path, "put", key=key, version=r.version.version,
                  digest=digest[:12],
                  reason="对象锁跨进程串行->版本连续;共享gc屏障->GC等在途提交结束",
                  result="ok")


def _worker_dotkey_gc_loop(root: str, stop_path: str, log_path: str | None, q) -> None:
    from app.service import StorageService
    svc = StorageService(root)
    n_runs = 0
    while not os.path.exists(stop_path):
        collected = svc.gc()
        n_runs += 1
        q.put(("gc-run", len(collected)))
        _wlog(log_path, "gc", run=n_runs, collected=len(collected),
              reason="排他gc屏障->无在途变更;任一版本引用即存活", result="ok")
        time.sleep(0.002)
    q.put(("gc-done", n_runs))


@pytest.mark.parametrize("keys", [DOT_KEYS[:3]], ids=["dotkeys"])
def test_dotkey_gc_concurrent_with_multiprocess_writes(tmp_root: Path,
                                                       keys: list[str]):
    log_path = tmp_root.parent / "mp-dotkey-gc.log.jsonl"
    stop_path = tmp_root.parent / "mp-dotkey-gc-stop"
    n_writers, rounds = 4, 12

    q = _CTX.Queue()
    gc_proc = _CTX.Process(target=_worker_dotkey_gc_loop,
                           args=(str(tmp_root), str(stop_path), str(log_path), q))
    writers = [
        _CTX.Process(target=_worker_dotkey_writer,
                     args=(str(tmp_root), keys, i, rounds, str(log_path), q))
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
    puts = [e for e in events if isinstance(e[0], str) and e[0] in keys]
    gc_runs = [e for e in events if e[0] == "gc-run"]
    assert gc_runs, "GC 进程应至少跑过一轮"

    svc = StorageService(tmp_root)
    per_key: dict[str, list[tuple[int, str]]] = {k: [] for k in keys}
    for key, version, digest in puts:
        per_key[key].append((version, digest))

    # 每个点号对象：版本 1..N 连续不重不跳
    for key, rows in per_key.items():
        nums = sorted(v for v, _ in rows)
        log_case("并发写-版本连续", key=key, 版本数=len(nums),
                 版本范围=(nums[0], nums[-1]),
                 依据="同一对象全部变更跨进程串行", 结果="ok")
        assert nums == list(range(1, n_writers * rounds + 1)), key

    # 每个已提交版本：元数据 digest 一致、按版本读回内容 sha256 自洽（GC 没误删）
    missing = []
    for key, rows in per_key.items():
        state = svc.stat(key)
        for version, digest in rows:
            rec = state.find(version)
            assert rec is not None and rec.digest == digest, (key, version)
            if not svc.blob_store.exists(digest):
                missing.append((key, version, digest))
                continue
            data, ver = svc.get_object(key, version=version)
            assert ver.digest == digest and sha256_hex(data) == digest
    log_case("GC并发-存活判定", gc执行次数=len(gc_runs), 成功提交=len(puts),
             被误删的存活blob=len(missing),
             依据="任一版本(含历史)引用即存活;点号对象与普通对象同等枚举",
             结果="ok" if not missing else "bad")
    assert missing == [], missing

    # 收敛：并发结束后再连续两次 GC，结果一致且磁盘==引用集合
    first = svc.gc()
    second = svc.gc()
    on_disk = svc.blob_store.all_digests()
    referenced = svc.catalog.all_referenced_digests()
    log_case("GC并发-收敛", 第一次回收=len(first), 第二次回收=len(second),
             磁盘blob=len(on_disk), 被引用=len(referenced),
             依据="无在途变更后连续回收结果一致;垃圾不累积、存活不丢失",
             结果="ok" if second == [] and on_disk == referenced else "bad")
    assert second == []
    assert on_disk == referenced
    _dump_worker_log(log_path)

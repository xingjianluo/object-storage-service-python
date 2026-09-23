"""中断与重启恢复测试。

场景：
1. 写入在“落 blob 之前/过程中”中断：只留下 tmp 暂存半成品，
   重启构造服务时清空暂存，逻辑对象不可见（无半成品被读到）。
2. 写入在“blob 已提交、meta 未发布”之间中断：留下孤儿 blob，
   对象仍不可见；孤儿由 GC 回收，不影响任何对象。
3. 正常提交后重启：元数据与内容完整可读，版本可见性保持。

判定依据：发布顺序 = 先 blob（内容寻址原子 rename）后 meta（原子 rename）；
崩溃点之前没有任何指针能指向半成品。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.errors import ObjectNotFoundError
from app.service import StorageService

from .conftest import log_case


def test_restart_cleans_staging_half_writes(tmp_root: Path):
    # 预构造一个“写了一半”的暂存文件（模拟进程在 tmp 阶段崩溃）
    svc = StorageService(tmp_root)
    tmp_dir = svc.blob_store.tmp_dir
    (tmp_dir / ".put-crashed-1").write_bytes(b"half" * 1000)
    (tmp_dir / ".put-crashed-2").write_bytes(b"written")
    assert len(list(tmp_dir.iterdir())) == 2

    # 重新启动（新进程等价：重新构造服务即做恢复）
    svc2 = StorageService(tmp_root)
    leftovers = list(svc2.blob_store.tmp_dir.iterdir())
    log_case(
        "恢复-清理暂存半成品",
        staging_left=len(leftovers),
        依据="构造时清空tmp;暂存内容从未进入blobs,无对象引用",
    )
    assert leftovers == []
    assert svc2.blob_store.all_digests() == set()


def test_crash_between_blob_and_meta_leaves_only_garbage(service, tmp_root):
    # 正常对象先建立一个，确认不受影响
    good = service.put_object("good", [b"good-data"]).version

    # 模拟崩溃点：blob 已原子发布，但元数据尚未 store
    orphan_blob = service.blob_store.put([b"committed-but-unreferenced"])
    # 此时直接换一个“新进程”的服务实例，不发布任何 meta
    svc2 = StorageService(tmp_root)

    # 半成品对象不可见、不可读
    with pytest.raises(ObjectNotFoundError) as ei:
        svc2.get_object("half-object")
    log_case(
        "恢复-blob已落meta未发",
        orphan_digest=orphan_blob.digest[:12],
        code=ei.value.code,
        good_version=good.version,
        依据="无meta指针->对象not_found;孤儿blob仅能被GC触及",
    )
    # 已提交对象完好
    assert svc2.get_object("good")[0] == b"good-data"
    # 孤儿 blob 存在但无引用，GC 精确回收
    assert orphan_blob.digest in svc2.blob_store.all_digests()
    collected = svc2.gc()
    assert orphan_blob.digest in collected
    assert good.digest in svc2.blob_store.all_digests()
    assert svc2.get_object("good")[0] == b"good-data"


def test_state_survives_restart_with_versions_and_tombstone(service, tmp_root):
    v1 = service.put_object("k", [b"one"]).version
    v2 = service.put_object("k", [b"two"]).version
    service.put_object("other", [b"two"])  # 与 v2 共享 blob
    service.delete_object("k")

    svc2 = StorageService(tmp_root)
    state = svc2.stat("k")
    log_case(
        "重启-版本与墓碑保持",
        key="k",
        versions=[v.version for v in state.versions],
        current=state.current,
        shared_digest=v2.digest[:12],
        依据="meta原子落盘;删除为墓碑;共享blob引用仍完整",
    )
    assert [v.version for v in state.versions] == [1, 2, 3]
    assert state.current is None
    assert svc2.get_object("k", version=1)[0] == b"one"
    assert svc2.get_object("other")[0] == b"two"
    # 重启后回滚恢复
    svc2.rollback("k", 2)
    assert svc2.get_object("k")[0] == b"two"

    # 再重启确认回滚后的 current 也持久化
    svc3 = StorageService(tmp_root)
    assert svc3.stat("k").current == 2
    assert svc3.get_object("k")[0] == b"two"
    log_case(
        "重启-回滚状态持久",
        current=svc3.stat("k").current,
        依据="current指针随meta原子发布持久化",
    )


def test_meta_temp_files_never_visible(tmp_root: Path):
    svc = StorageService(tmp_root)
    svc.put_object("k", [b"payload"])
    # meta 目录下任何时刻都不应残留 .meta-*.tmp 半成品
    leftovers = list(tmp_root.glob("meta/**/.meta-*.tmp"))
    log_case(
        "元数据-无半成品文件",
        meta_tmp_left=len(leftovers),
        依据="store走临时文件+fsync+os.replace,失败即清理",
    )
    assert leftovers == []

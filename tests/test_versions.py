"""多版本可见性语义：覆盖写、逻辑删除、墓碑、回滚。

判定依据：current 指针、版本号单调性、墓碑可见性规则。
"""

from __future__ import annotations

import pytest

from app.errors import (
    ObjectDeletedError,
    ObjectNotFoundError,
    VersionNotFoundError,
)

from .conftest import log_case


def test_overwrite_versions_monotonic(service):
    v1 = service.put_object("k", [b"a"]).version
    v2 = service.put_object("k", [b"bb"]).version
    v3 = service.put_object("k", [b"ccc"]).version
    log_case(
        "覆盖写-版本单调",
        key="k",
        versions=(v1.version, v2.version, v3.version),
        digests=(v1.digest[:12], v2.digest[:12], v3.digest[:12]),
        依据="版本号仅在PUT/DELETE时+1,当前=最新",
    )
    assert (v1.version, v2.version, v3.version) == (1, 2, 3)
    data, cur = service.get_object("k")
    assert data == b"ccc" and cur.version == 3
    assert service.stat("k").current == 3


def test_delete_is_logical_and_history_stays_readable(service):
    service.put_object("k", [b"a"])
    service.put_object("k", [b"bb"])
    tomb = service.delete_object("k")

    log_case(
        "删除-逻辑墓碑",
        key="k",
        tombstone_version=tomb.version,
        digest=tomb.digest or "<empty>",
        依据="DELETE追加deleted版本且current=None,blob不删除",
    )
    assert tomb.deleted is True and tomb.version == 3 and tomb.digest == ""
    # 当前不可见 -> object_deleted
    with pytest.raises(ObjectDeletedError) as ei:
        service.get_object("k")
    assert ei.value.code == "object_deleted"
    # 历史版本仍可读（回滚的依据）
    assert service.get_object("k", version=1)[0] == b"a"
    assert service.get_object("k", version=2)[0] == b"bb"
    # 墓碑版本不提供内容
    with pytest.raises(VersionNotFoundError):
        service.get_object("k", version=3)


def test_delete_nonexistent_raises_not_found(service):
    with pytest.raises(ObjectNotFoundError) as ei:
        service.delete_object("ghost")
    log_case(
        "删除-不存在对象",
        key="ghost",
        code=ei.value.code,
        依据="无元数据文件->object_not_found,不凭空造墓碑",
    )
    assert ei.value.code == "object_not_found"


def test_idempotent_repeat_delete(service):
    service.put_object("k", [b"a"])
    t1 = service.delete_object("k")
    t2 = service.delete_object("k")  # 已删除 -> 幂等
    log_case(
        "删除-幂等",
        versions=(t1.version, t2.version),
        依据="已删除再删返回最近墓碑,不新增版本",
    )
    assert t1.version == t2.version
    assert [v.version for v in service.list_versions("k")] == [1, 2]


def test_rollback_reuses_reference_without_copy(service):
    v1 = service.put_object("k", [b"a"]).version
    v2 = service.put_object("k", [b"bb"]).version
    service.delete_object("k")  # v3 墓碑
    assert service.stat("k").current is None

    restored = service.rollback("k", 1)
    data, cur = service.get_object("k")
    log_case(
        "回滚-复用历史引用",
        target_version=1,
        current=cur.version,
        digest=cur.digest[:12],
        依据="仅移动current指针,不新建版本/不复制blob",
        versions_now=[v.version for v in service.list_versions("k")],
    )
    assert restored.version == 1 and restored.digest == v1.digest
    assert data == b"a"
    # 回滚不产生新版本号
    assert [v.version for v in service.list_versions("k")] == [1, 2, 3]
    assert cur.version == 1

    # 回滚到不存在版本 / 墓碑 -> 可区分错误
    with pytest.raises(VersionNotFoundError):
        service.rollback("k", 99)
    with pytest.raises(VersionNotFoundError):
        service.rollback("k", 3)  # 墓碑
    with pytest.raises(ObjectNotFoundError):
        service.rollback("ghost", 1)


def test_rewrite_after_delete_continues_version_number(service):
    service.put_object("k", [b"a"])
    service.delete_object("k")
    v3 = service.put_object("k", [b"resurrected"]).version
    log_case(
        "删除后重写",
        new_version=v3.version,
        digest=v3.digest[:12],
        依据="版本号跨墓碑继续单调递增",
    )
    assert v3.version == 3
    assert service.get_object("k")[0] == b"resurrected"

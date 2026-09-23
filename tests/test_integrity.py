"""内容完整性测试：写入校验拒绝、读取损坏/缺失检测。

错误必须可区分：
- checksum_mismatch：客户端声明 digest 与实际内容不符（写入侧拒绝，422）；
- content_corrupted：磁盘 blob 内容哈希不符或物理缺失（读取侧，500）；
- object_not_found / object_deleted / version_not_found：元数据层语义。
判定依据：sha256(实际字节) 与 声明/引用 digest 是否相等。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.errors import (
    ChecksumMismatchError,
    ContentCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    VersionNotFoundError,
)
from app.hashing import sha256_hex

from .conftest import log_case


def test_writes_with_wrong_declared_digest_are_rejected(service, tmp_root):
    content = b"actual-bytes"
    wrong = "f" * 64
    with pytest.raises(ChecksumMismatchError) as ei:
        service.put_object("k", [content], expected_digest=wrong)
    log_case(
        "写入-校验不符拒绝",
        declared=wrong[:12],
        actual=sha256_hex(content)[:12],
        code=ei.value.code,
        依据="sha256(实际字节)!=声明值->拒绝,不发布meta不引用blob",
    )
    assert ei.value.code == "checksum_mismatch"
    # 被拒绝的内容不产生对象、不产生 blob、不留暂存
    with pytest.raises(ObjectNotFoundError):
        service.stat("k")
    assert service.blob_store.all_digests() == set()
    assert list(service.blob_store.tmp_dir.iterdir()) == []


def test_writes_with_correct_declared_digest_accepted(service):
    content = b"verified-bytes"
    digest = sha256_hex(content)
    result = service.put_object("k", [content], expected_digest=digest)
    log_case(
        "写入-校验一致接受",
        digest=digest[:12],
        version=result.version.version,
        依据="sha256(实际字节)==声明值->接受",
    )
    assert result.version.digest == digest


def test_corrupted_blob_detected_on_read(service, tmp_root):
    ver = service.put_object("k", [b"important"]).version
    path = service.blob_store._path_for(ver.digest)
    path.write_bytes(b"tampered-bytes")  # 模拟磁盘位腐/被篡改

    # 当前版本读 -> content_corrupted
    with pytest.raises(ContentCorruptedError) as ei:
        service.get_object("k")
    # 指定历史版本同样检测
    with pytest.raises(ContentCorruptedError):
        service.get_object("k", version=1)
    log_case(
        "读取-损坏检测",
        expected_digest=ver.digest[:12],
        actual_digest=sha256_hex(b"tampered-bytes")[:12],
        code=ei.value.code,
        依据="读取后重算sha256与引用digest不符",
    )
    assert ei.value.code == "content_corrupted"
    assert "校验失败" in str(ei.value)

    # verify 也独立检测
    with pytest.raises(ContentCorruptedError):
        service.blob_store.verify(ver.digest)


def test_missing_blob_detected_on_read(service, tmp_root):
    ver = service.put_object("k", [b"here-today"]).version
    service.blob_store._path_for(ver.digest).unlink()

    with pytest.raises(ContentCorruptedError) as ei:
        service.get_object("k")
    log_case(
        "读取-缺失检测",
        missing_digest=ver.digest[:12],
        code=ei.value.code,
        依据="元数据有引用但物理文件缺失",
    )
    assert ei.value.code == "content_corrupted"
    assert "缺失" in str(ei.value)


def test_rewrite_repairs_corrupted_blob(service):
    content = b"repairable-content"
    digest = sha256_hex(content)
    ver = service.put_object("k", [content]).version
    path = service.blob_store._path_for(digest)
    path.write_bytes(b"broken")

    # 用相同正确内容重写：物理副本被原子修复，且产生新版本
    result = service.put_object("k", [content])
    data, cur = service.get_object("k")
    log_case(
        "损坏-正确内容重写修复",
        digest=digest[:12],
        repaired_version=cur.version,
        dedup=result.deduplicated,
        依据="put发现已存在副本损坏->用校验通过的新临时文件原子替换",
    )
    assert data == content
    assert sha256_hex(data) == digest
    assert result.deduplicated is False  # 损坏替换不算去重复用


def test_distinct_error_codes_are_discriminable(service):
    service.put_object("k", [b"v1"])
    service.delete_object("k")

    cases: list[tuple[str, type[Exception], str]] = []

    def probe(label: str, exc_type, code: str, fn):
        with pytest.raises(exc_type) as ei:
            fn()
        assert ei.value.code == code
        cases.append((label, exc_type, code))

    probe("不存在对象", ObjectNotFoundError, "object_not_found",
          lambda: service.get_object("ghost"))
    probe("已删除对象", ObjectDeletedError, "object_deleted",
          lambda: service.get_object("k"))
    # 建一个存在对象再探版本不存在
    service.put_object("m", [b"m"])
    probe("存在对象但版本号缺失", VersionNotFoundError, "version_not_found",
          lambda: service.get_object("m", version=99))
    probe("墓碑版本", VersionNotFoundError, "version_not_found",
          lambda: service.get_object("k", version=2))
    probe("声明校验失败", ChecksumMismatchError, "checksum_mismatch",
          lambda: service.put_object("x", [b"z"], expected_digest="0" * 64))
    ver = service.put_object("c", [b"c"]).version
    service.blob_store._path_for(ver.digest).write_bytes(b"!")
    probe("物理损坏", ContentCorruptedError, "content_corrupted",
          lambda: service.get_object("c"))

    log_case(
        "错误码可区分",
        codes={label: code for label, _, code in cases},
        依据="异常类型+稳定code字符串双重区分",
    )
    codes = {code for _, _, code in cases}
    assert codes == {
        "object_not_found",
        "object_deleted",
        "version_not_found",
        "checksum_mismatch",
        "content_corrupted",
    }

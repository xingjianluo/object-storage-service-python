"""HTTP API 集成测试：状态码、错误体 code、去重标志与版本头。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.hashing import sha256_hex

from .conftest import log_case


@pytest.fixture()
def client(tmp_root) -> TestClient:
    return TestClient(create_app(tmp_root))


def test_put_get_dedup_header_and_versioning(client):
    r1 = client.put("/objects/a/b.txt", content=b"abc")
    r2 = client.put("/objects/c.txt", content=b"abc")
    r3 = client.put("/objects/a/b.txt", content=b"abcd")
    log_case(
        "HTTP-写入去重",
        statuses=(r1.status_code, r2.status_code, r3.status_code),
        versions=(r1.json()["version"], r2.json()["version"], r3.json()["version"]),
        dedup=(r1.json()["deduplicated"], r2.json()["deduplicated"], r3.json()["deduplicated"]),
        依据="同sha256第二次写deduplicated=true",
    )
    assert r1.status_code == r2.status_code == r3.status_code == 200
    assert r1.json()["deduplicated"] is False
    assert r2.json()["deduplicated"] is True
    assert [r1.json()["version"], r2.json()["version"], r3.json()["version"]] == [1, 1, 2]

    g = client.get("/objects/a/b.txt")
    assert g.content == b"abcd"
    assert g.headers["X-Object-Version"] == "2"
    assert g.headers["X-Content-SHA256"] == sha256_hex(b"abcd")

    g1 = client.get("/objects/a/b.txt", params={"version": 1})
    assert g1.content == b"abc"


def test_error_status_and_body_codes(client):
    # 422 checksum_mismatch
    r = client.put("/objects/bad", content=b"x", headers={"X-Content-SHA256": "0" * 64})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "checksum_mismatch"

    # 404 object_not_found
    r = client.get("/objects/nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "object_not_found"

    # 410 object_deleted
    client.put("/objects/k", content=b"v")
    client.delete("/objects/k")
    r = client.get("/objects/k")
    assert r.status_code == 410
    assert r.json()["error"]["code"] == "object_deleted"

    # 404 version_not_found
    client.put("/objects/m", content=b"m")
    r = client.get("/objects/m", params={"version": 42})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "version_not_found"
    log_case(
        "HTTP-错误码映射",
        mapping={422: "checksum_mismatch", 404: "not_found/version", 410: "deleted"},
        依据="异常code->HTTP状态码稳定映射",
    )


def test_versions_rollback_and_gc(client):
    client.put("/objects/k", content=b"v1")
    client.put("/objects/k", content=b"v2-longer")
    d = client.delete("/objects/k")
    assert d.json()["deleted"] is True and d.json()["version"] == 3

    listing = client.get("/objects/k/versions").json()
    assert [v["version"] for v in listing["versions"]] == [1, 2, 3]
    assert listing["current"] is None

    r = client.post("/objects/k/rollback", json={"version": 2})
    assert r.status_code == 200 and r.json()["version"] == 2
    assert client.get("/objects/k").content == b"v2-longer"

    gc = client.post("/gc").json()
    assert "collected" in gc and gc["count"] == len(gc["collected"])
    log_case(
        "HTTP-删除回滚GC",
        versions=3,
        gc_collected=gc["count"],
        current=2,
        依据="墓碑后current为空;回滚切指针;GC只收无引用blob",
    )


def test_corrupted_object_returns_500_distinct_code(client, tmp_root):
    client.put("/objects/k", content=b"payload")
    # 直接破坏物理文件
    blob = next(tmp_root.glob("blobs/*/*"))
    blob.write_bytes(b"corrupt")
    r = client.get("/objects/k")
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "content_corrupted"
    log_case(
        "HTTP-损坏500",
        code=r.json()["error"]["code"],
        依据="读取重算sha256失败->content_corrupted与422写入拒绝区分",
    )

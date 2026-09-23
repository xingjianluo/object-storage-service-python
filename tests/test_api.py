"""HTTP 层：端到端接口与可区分错误码。"""

from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient

from app.api import create_app


@pytest.fixture()
def client(tmp_path):
    return TestClient(create_app(str(tmp_path / "data")))


def test_put_get_dedup_and_version_headers(client):
    body = b"http-body"
    d = hashlib.sha256(body).hexdigest()
    r = client.put("/objects/logs/a.log", content=body,
                   headers={"x-content-sha256": d})
    assert r.status_code == 200
    assert r.json()["version"]["version"] == 1

    r2 = client.put("/objects/logs/a.log", content=body,
                    headers={"x-content-sha256": d})
    assert r2.json()["version"]["version"] == 1  # 幂等不新增版本

    g = client.get("/objects/logs/a.log")
    assert g.status_code == 200 and g.content == body
    assert g.headers["x-content-sha256"] == d
    assert g.headers["x-object-version"] == "1"
    print(f"[判定] HTTP 写入 sha256={d[:12]} 去重幂等；响应头携带版本与内容标识")


def test_checksum_mismatch_returns_422_distinct(client):
    r = client.put("/objects/k", content=b"abc",
                   headers={"x-content-sha256": "0" * 64})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "checksum_mismatch"
    print(f"[判定] 校验不通过 HTTP 422 code=checksum_mismatch，原因={r.json()['error']['message'][:40]}")


def test_not_found_vs_deleted_codes(client):
    assert client.get("/objects/ghost").status_code == 404
    assert client.get("/objects/ghost").json()["error"]["code"] == "not_found"

    client.put("/objects/k", content=b"x")
    client.delete("/objects/k")
    r = client.get("/objects/k")
    assert r.status_code == 410
    assert r.json()["error"]["code"] == "object_deleted"
    print("[判定] 从未存在=404/not_found；删除后读=410/object_deleted，可区分")


def test_corrupted_object_returns_500_with_distinct_code(client, tmp_path):
    client.put("/objects/k", content=b"original")
    # 直接篡改物理副本
    blob = next(p for p in (tmp_path / "data" / "blobs").rglob("*")
                if p.is_file())
    blob.write_bytes(b"corrupt")
    r = client.get("/objects/k")
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "object_corrupted"
    print(f"[判定] 损坏对象 HTTP 500 code=object_corrupted，与 4xx 语义错误区分")


def test_rollback_and_metadata_endpoint(client):
    client.put("/objects/k", content=b"v1")
    client.put("/objects/k", content=b"v2")
    client.delete("/objects/k")
    r = client.post("/objects/k/rollback", params={"version": 1})
    assert r.status_code == 200 and r.json()["version"]["version"] == 4
    assert client.get("/objects/k").content == b"v1"

    md = client.get("/objects/k", params={"metadata": True}).json()
    assert md["current_version"] == 4
    assert [v["version"] for v in md["versions"]] == [1, 2, 3, 4]
    assert [v["operation"] for v in md["versions"]] == [
        "put", "put", "delete", "put"
    ]
    print(f"[判定] 版本链={[v['operation'] for v in md['versions']]} v4 回滚引用 v1")


def test_gc_and_recover_admin_endpoints(client):
    client.put("/objects/k", content=b"keep")
    r = client.post("/admin/gc")
    assert r.status_code == 200 and r.json()["count"] == 0
    r2 = client.post("/admin/recover")
    assert r2.status_code == 200 and "cleaned" in r2.json()
    print("[判定] 管理端点 GC/恢复可用")

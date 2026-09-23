"""FastAPI HTTP 层。

路由（注册顺序保证 {key:path} 不会吞掉子路径）：
- GET   /healthz                       存活检查
- POST  /gc                            回收无引用 blob
- GET   /objects/{key}/versions        列出版本（含墓碑）
- POST  /objects/{key}/rollback        {"version": N} 回滚可见指针
- PUT   /objects/{key}                 覆盖写入；X-Content-SHA256 可选校验
- GET   /objects/{key}                 读取当前版本（?version=N 读指定版本）
- HEAD  /objects/{key}                 当前版本元信息（?version=N）
- DELETE /objects/{key}                逻辑删除（墓碑）

错误响应统一为 {"error": {"code", "message"}}，code 稳定可区分：
checksum_mismatch(422) / content_corrupted(500) / object_not_found(404) /
object_deleted(410) / version_not_found(404)。
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from .errors import (
    ChecksumMismatchError,
    ContentCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    StorageError,
    VersionNotFoundError,
)
from .service import StorageService

_ERROR_STATUS = {
    ChecksumMismatchError: 422,
    ContentCorruptedError: 500,
    ObjectNotFoundError: 404,
    ObjectDeletedError: 410,
    VersionNotFoundError: 404,
}


def _version_dict(v) -> dict:
    return {
        "version": v.version,
        "digest": v.digest,
        "size": v.size,
        "created_at": v.created_at,
        "deleted": v.deleted,
    }


def create_app(root: str | Path = "./data") -> FastAPI:
    """构造 FastAPI 应用并绑定一个本地存储服务。"""
    app = FastAPI(title="local-object-storage", version="0.1.0")
    service = StorageService(root)
    app.state.service = service

    @app.exception_handler(StorageError)
    async def _on_storage_error(_: Request, exc: StorageError) -> JSONResponse:
        status = _ERROR_STATUS.get(type(exc), 500)
        return JSONResponse(
            status_code=status,
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok", "root": str(service.root)}

    @app.post("/gc")
    async def gc() -> dict:
        collected = service.gc()
        return {"collected": collected, "count": len(collected)}

    # ---- 子路径路由必须在通用 {key:path} 路由之前 ----

    @app.get("/objects/{key:path}/versions")
    async def list_versions(key: str) -> dict:
        versions = service.list_versions(key)
        state = service.stat(key)
        return {
            "key": key,
            "current": state.current,
            "versions": [_version_dict(v) for v in versions],
        }

    @app.post("/objects/{key:path}/rollback")
    async def rollback(key: str, payload: dict) -> dict:
        version = int(payload["version"])
        ver = service.rollback(key, version)
        return {"key": key, **_version_dict(ver)}

    # ---- 通用对象路由 ----

    @app.put("/objects/{key:path}")
    async def put_object(key: str, request: Request) -> dict:
        expected = request.headers.get("x-content-sha256")
        body = await request.body()
        result = service.put_object(key, [body], expected_digest=expected or None)
        return {
            "key": key,
            **_version_dict(result.version),
            "deduplicated": result.deduplicated,
        }

    @app.get("/objects/{key:path}")
    async def get_object(key: str, version: int | None = None) -> Response:
        data, ver = service.get_object(key, version=version)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Object-Version": str(ver.version),
                "X-Content-SHA256": ver.digest,
                "X-Content-Length": str(ver.size),
            },
        )

    @app.head("/objects/{key:path}")
    async def head_object(key: str, version: int | None = None) -> Response:
        _data, ver = service.get_object(key, version=version)
        return Response(
            media_type="application/octet-stream",
            headers={
                "X-Object-Version": str(ver.version),
                "X-Content-SHA256": ver.digest,
                "Content-Length": str(ver.size),
            },
        )

    @app.delete("/objects/{key:path}")
    async def delete_object(key: str) -> dict:
        ver = service.delete_object(key)
        return {"key": key, **_version_dict(ver)}

    return app


__all__ = ["create_app"]

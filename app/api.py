"""FastAPI HTTP 接口。

错误码（``error.code``）可区分：

* ``checksum_mismatch``  写入内容与声明摘要不符（422）
* ``object_corrupted``   物理副本/元数据损坏或缺失（500 数据面错误）
* ``not_found``          key 从未存在或指定版本不存在（404）
* ``object_deleted``     key 有版本链但当前是删除标记（410）
* ``invalid_argument``   参数非法（400）
"""

from __future__ import annotations

import os
from dataclasses import asdict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.errors import (
    ChecksumMismatchError,
    InvalidArgumentError,
    ObjectCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    StorageError,
    VersionNotFoundError,
)
from app.storage import ObjectStorage

DEFAULT_STORAGE_ROOT = os.environ.get(
    "OBJECT_STORAGE_ROOT", os.path.join(os.getcwd(), "data")
)


def create_app(storage_root: str | None = None) -> FastAPI:
    app = FastAPI(title="本地对象存储与版本管理服务", version="0.1.0")
    storage = ObjectStorage(storage_root or DEFAULT_STORAGE_ROOT)
    app.state.storage = storage

    def _error(code: str, message: str, status: int, **extra) -> JSONResponse:
        body: dict = {"error": {"code": code, "message": message}}
        body["error"].update(extra)
        return JSONResponse(status_code=status, content=body)

    async def _on_storage_error(_: Request, exc: StorageError) -> JSONResponse:
        if isinstance(exc, ChecksumMismatchError):
            return _error("checksum_mismatch", str(exc), 422)
        if isinstance(exc, ObjectDeletedError):
            return _error("object_deleted", str(exc), 410)
        if isinstance(exc, (ObjectNotFoundError, VersionNotFoundError)):
            return _error("not_found", str(exc), 404)
        if isinstance(exc, InvalidArgumentError):
            return _error("invalid_argument", str(exc), 400)
        if isinstance(exc, ObjectCorruptedError):
            return _error("object_corrupted", str(exc), 500)
        return _error("storage_error", str(exc), 500)

    app.add_exception_handler(StorageError, _on_storage_error)

    @app.get("/health")
    def health():
        return {"status": "ok", "storage_root": str(storage.root)}

    @app.put("/objects/{key:path}")
    async def put_object(key: str, request: Request):
        data = await request.body()
        expected = request.headers.get("x-content-sha256")
        rec = storage.put_object(key, data, expected or None)
        return {"key": key, "version": asdict(rec)}

    @app.get("/objects/{key:path}")
    def get_object(
        key: str, version: int | None = None, metadata: bool = False
    ):
        if metadata:
            md = storage.get_metadata(key)
            return {
                "key": md.key,
                "current_version": md.current_version,
                "versions": [asdict(v) for v in md.versions],
            }
        data, rec = storage.get_object(key, version)
        # 以不可变副本的 sha256 作为 ETag，并回传判定依据
        headers = {
            "ETag": f'"{rec.sha256}"',
            "X-Object-Version": str(rec.version),
            "X-Content-SHA256": rec.sha256,
        }
        from fastapi.responses import Response

        return Response(content=data, media_type="application/octet-stream",
                        headers=headers)

    @app.delete("/objects/{key:path}")
    def delete_object(key: str):
        rec = storage.delete_object(key)
        return {"key": key, "deleted_version": asdict(rec)}

    @app.post("/objects/{key:path}/rollback")
    def rollback_object(key: str, version: int):
        rec = storage.rollback_object(key, version)
        return {"key": key, "version": asdict(rec)}

    @app.post("/admin/gc")
    def garbage_collect():
        removed = storage.garbage_collect()
        return {"removed": removed, "count": len(removed)}

    @app.post("/admin/recover")
    def recover():
        result = storage.recover()
        return {"cleaned": result}

    return app

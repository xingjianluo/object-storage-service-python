# 本地对象存储与版本管理服务（FastAPI）

以本地磁盘为对象层，提供内容去重、逻辑对象多版本、删除/回滚与并发一致性。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 默认存储于 ./data，可用 OBJECT_STORAGE_ROOT 覆盖
# 交互式 API 文档：http://127.0.0.1:8000/docs
```

## 测试

```bash
uv run pytest
```

## 文档

详见 [docs/storage.md](docs/storage.md)：存储布局、版本与引用规则、
并发/崩溃一致性保证、可区分错误码与验证方法。

## 代码结构

```
app/
├── errors.py      # 可区分错误类型
├── hashing.py     # sha256 内容标识
├── blob_store.py  # 内容寻址物理层（去重/原子写/读校验/恢复）
├── metadata.py    # 版本链元数据（flock 跨进程锁 + 原子替换）
├── storage.py     # 逻辑对象服务（版本/回滚/GC/一致性）
└── api.py         # FastAPI HTTP 层
tests/             # 物理层/元数据/逻辑层/并发中断/HTTP 测试
```

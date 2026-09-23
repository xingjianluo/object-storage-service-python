# 本地对象存储与版本管理

以本地磁盘为对象层的 FastAPI 服务，提供**内容寻址去重**、**逻辑对象多版本管理**、
**覆盖/删除/回滚的明确可见性语义**，以及**并发与崩溃场景下的一致性保证**。

---

## 1. 存储布局

所有数据位于存储根目录（环境变量 `OBJECT_STORAGE_ROOT`，默认 `./data`）：

```
<root>/
├── blobs/
│   └── <ab>/                      # 以 sha256 前两位分桶
│       └── <64hex>                # 不可变物理副本，文件名=内容 sha256
├── tmp/
│   └── <uuid>                     # 物理写入暂存区，原子提升后消失
├── meta/
│   └── <urlencoded-key>.json      # 每个逻辑对象一条版本链
│   └── <name>.json.tmp.<uuid>     # 元数据原子写暂存（正常情况不存在）
└── locks/
    └── <urlencoded-key>.lock      # 每逻辑对象的跨进程互斥锁文件（flock）
```

### 物理层（`app/blob_store.py`）

* 物理副本以**内容 sha256** 命名并按前两位分桶；相同内容的摘要必然相同，
  因此天然全局去重：**相同内容在磁盘上只保留一份**，即使跨不同逻辑对象。
* 副本**只增不改**（immutable）：目标路径已存在时绝不重写，直接复用，
  重复写入不可能破坏已有对象。
* 写入流程：`tmp/<uuid>` 写入并 `fsync` → `os.replace` 原子提升到
  `blobs/<ab>/<digest>` → `fsync` 目录。任何时刻 `blobs/` 下要么没有该文件，
  要么是完整文件，不存在“写一半”的副本。
* 每次读取都对磁盘字节**重算 sha256** 并与文件名比对，同时核对大小，
  任何静默损坏（bit-rot）/缺失都会被检出。

### 元数据层（`app/metadata.py`）

每个逻辑对象一份 JSON，结构为**只追加（append-only）的版本记录链**：

```json
{
  "key": "logs/a.log",
  "current_version": 3,
  "versions": [
    {"version": 1, "sha256": "…", "size": 120, "operation": "put",    "timestamp": "…"},
    {"version": 2, "sha256": "…", "size": 240, "operation": "put",    "timestamp": "…"},
    {"version": 3, "sha256": "…", "size": 240, "operation": "delete", "timestamp": "…"}
  ]
}
```

* `version` 单调递增、永不复用；`current_version` 指向**当前可见版本**。
* `operation` 为 `put`（内容版本）或 `delete`（删除标记）。
* 落盘同样是 `tmp + fsync + os.replace` 原子替换，崩溃后不会出现半截 JSON；
  加载时校验结构与 `current_version` 引用的有效性，损坏即报 `object_corrupted`。

---

## 2. 版本与引用规则

### 可见性语义

| 操作 | 语义 |
|------|------|
| 首次写入 | 追加 `put v1`，`current_version=1` |
| 覆盖写入不同内容 | 追加新的 `put` 版本；旧版本保留，可按版本号读取 |
| 覆盖写入**相同内容** | **幂等**：不产生新版本，当前版本保持不变 |
| 删除 | 追加一条 `delete` 标记作为当前版本；默认读取返回“已删除” |
| 重复删除 | 幂等：已有删除标记则直接返回，不新增版本 |
| 回滚到 `vN` | 追加一条新的 `put` 记录，其 `sha256` 指向 `vN` 的内容；不修改/删除任何中间版本 |

* 默认读取（不带版本号）永远以 `current_version` 为准。
* 删除后仍可**显式按版本号**读取历史内容；删除标记保留被删内容的 `sha256`。
* 回滚目标必须是**内容版本**（不能是删除标记），且其物理副本必须存在。
* 一个 key 从未出现 → `not_found`（404）；key 有版本链但当前是删除标记
  → `object_deleted`（410）。二者明确区分。

### 引用与垃圾回收（GC）

* **引用判定**：一个物理副本只要被**任意一条版本记录**（含历史版本与删除标记
  所引用的被删内容）引用，就必须保留；物理层中不被任何记录引用的副本为**孤儿**，
  可由 `POST /admin/gc` 安全删除。
* 因为历史版本和删除标记都记录了 `sha256`，正常情况下 GC 不会回收任何
  仍可回滚/审计的内容；只有“写了物理副本却从未成功挂接元数据”的残留才会成为孤儿。

---

## 3. 并发与中断一致性

### 锁层级

1. **GC 读写锁（进程内，写者优先）**
   普通对象操作（put/delete/rollback）持**共享端**，可整体并发；
   GC 持**排他端**。写者优先避免持续写入下 GC 饥饿。
2. **逻辑对象锁（进程内 `threading.Lock` + 文件 `flock`）**
   同一 key 的“读元数据→改版本链→原子写回”全程持锁，
   不同 key 互不阻塞；`flock` 同时提供跨进程互斥。

### 关键不变量

* **无“未挂接引用”窗口**：写入时共享锁覆盖“物理副本落盘 → 元数据追加引用”
  的全过程。因此 GC 排他运行时，绝不会把“刚写完、尚未挂接元数据”的新副本
  误判为孤儿删除（这是对象存储常见的丢数据竞态，见
  `test_gc_concurrent_with_put_never_loses_live_blob`）。
* **并发覆盖写可线性化**：同 key 并发写被串行化，版本号连续、无重复、无缺口；
  每个版本都能按其记录的 sha256 校验读出，绝不出现半成品。
* **并发读/删**：读者只能观察到“完整的旧内容”或“删除标记”，不会读到中间态。
* **崩溃恢复**：
  * 物理写入在 `tmp/` 暂存，未 `rename` 成功则任何元数据都不引用它；
    进程重启时 `ObjectStorage` 自动清理 `tmp/` 与 `meta/*.tmp.*` 残留
    （也可 `POST /admin/recover` 手动触发）。
  * 物理副本与元数据均为原子替换：重启后要么是上一完整状态，要么是下一完整状态。
  * 崩溃后至多留下**不被引用的孤儿副本**，可由 GC 回收，不会造成半成品被读取。
* **损坏/缺失检测**：读取时重算摘要 + 核对大小；物理副本缺失或摘要不符均抛
  `object_corrupted`，与“写入时校验不通过（`checksum_mismatch`）”区分。

### 可区分的失败原因

| 场景 | 异常 | HTTP |
|------|------|------|
| 写入内容与 `X-Content-SHA256` 声明不符 | `ChecksumMismatchError` | 422 `checksum_mismatch` |
| 声明摘要格式非法 | `InvalidArgumentError` | 400 `invalid_argument` |
| key 从未存在 / 版本号不存在 | `ObjectNotFoundError` / `VersionNotFoundError` | 404 `not_found` |
| 当前版本是删除标记 | `ObjectDeletedError` | 410 `object_deleted` |
| 回滚目标是删除标记 | `InvalidArgumentError` | 400 `invalid_argument` |
| 物理副本缺失 / 摘要损坏 / 大小不符 / 元数据损坏 | `ObjectCorruptedError` | 500 `object_corrupted` |

被校验拒绝的写入**不会留下任何物理副本或元数据**。

---

## 4. HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| `PUT` | `/objects/{key}` | 写入/覆盖；请求头 `X-Content-SHA256` 可选，服务端强制核对 |
| `GET` | `/objects/{key}` | 读取当前可见版本；`?version=N` 读历史版本；`?metadata=true` 看版本链 |
| `DELETE` | `/objects/{key}` | 逻辑删除（打标记，可回滚） |
| `POST` | `/objects/{key}/rollback?version=N` | 回滚到历史内容版本 |
| `POST` | `/admin/gc` | 回收孤儿物理副本，返回删除摘要列表 |
| `POST` | `/admin/recover` | 清理暂存/临时残留 |
| `GET` | `/health` | 健康检查 |

读取成功响应头携带 `ETag`（即内容 sha256）、`X-Object-Version`、
`X-Content-SHA256`。

快速手工验证：

```bash
uv run uvicorn main:app --reload
curl -s -XPUT localhost:8000/objects/greet --data-binary 'hello' | jq
curl -s localhost:8000/objects/greet | jq -Rs .
curl -s -XDELETE localhost:8000/objects/greet | jq
curl -s -XPOST 'localhost:8000/objects/greet/rollback?version=1' | jq
```

---

## 5. 验证方法

```bash
uv sync
uv run pytest                      # 全部 44 个用例
uv run pytest -k concurrency       # 并发/中断场景
uv run pytest -k "corrupt or checksum or missing"  # 可区分失败原因
```

测试覆盖（每个用例均打印内容标识 sha256、版本号与判定依据）：

* **去重**：同内容重复写物理只留一份且不新增版本；跨 key 共享副本；
  重复写不修改既有文件（mtime 不变、内容可读）。
* **并发写同一逻辑对象**：版本连续无缺口、每个历史版本可校验读取；
  同内容并发写只产生一个版本；不同 key 并发互不可见干扰。
* **并发读/删、GC 与写入并发**：读者只见完整态或删除态；GC 零误删存活副本。
* **中断恢复**：物理暂存与元数据临时文件残留，重启后自动清理、已提交版本完好。
* **校验失败**：声明摘要与实际不符 → 写入被拒绝且零残留。
* **损坏/缺失对象**：篡改物理字节、删除物理副本、破坏大小元数据、
  破坏元数据 JSON，均被检出并返回可区分错误。

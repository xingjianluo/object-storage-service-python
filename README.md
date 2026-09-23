# 本地对象存储与版本管理服务（FastAPI）

以**本地磁盘**为对象层，提供：

- **内容寻址 + 去重**：相同内容（SHA-256 相同）全局只保留一份物理副本；
- **写入校验**：可携带 `X-Content-SHA256`，声明值与实际不符的写入被拒绝，且不留任何残留；
- **多版本管理**：覆盖写、逻辑删除（墓碑）、回滚的可见性语义明确；
- **并发 / 中断一致性**：写锁串行化元数据变更，物理层与元数据层均“临时文件 + fsync + 原子改名”，崩溃不产生半成品；
- **损坏可检测**：读取时重算哈希，物理缺失或损坏返回与其他错误可区分的 `content_corrupted`。

## 1. 存储布局

```
<root>/
├── blobs/
│   └── <ab>/                     # 按内容 sha256 前 2 位分桶
│       └── <64hex>               # 不可变内容文件，文件名即内容 sha256
├── tmp/
│   └── .put-*                    # 写入暂存；进程启动时整体清空
└── meta/
    └── <ab>/                     # 按 sha256(key) 前 2 位分桶
        └── <urlencoded-key>.json # 每个逻辑对象一个元数据文件
```

- **Blob 文件名 = 内容 SHA-256**。路径天然唯一，因此“同内容 = 同路径 = 单副本”，去重不需要额外索引。
- key 经 URL 安全编码（`/` 也被编码）落盘，杜绝路径穿越；对象自身再带 `key` 字段，加载时校验一致。
- 元数据 JSON 结构（schema=1）：

```json
{
  "schema": 1,
  "key": "dir/a.txt",
  "current": 2,
  "versions": [
    {"version": 1, "digest": "6bb1...", "size": 7,  "created_at": 1727.0, "deleted": false},
    {"version": 2, "digest": "e8af...", "size": 14, "created_at": 1728.0, "deleted": false},
    {"version": 3, "digest": "",       "size": 0,  "created_at": 1729.0, "deleted": true}
  ]
}
```

## 2. 版本与引用规则

### 版本号
- 版本号在每个逻辑对象内**单调递增，从 1 开始**。
- `PUT`（覆盖写）与 `DELETE` 都会占用一个新版本号；**回滚不占用版本号**。
- 版本记录一经写入不可变，只追加、不改写历史。

### 可见性（`current` 指针）
| 操作 | 行为 | current |
| --- | --- | --- |
| `PUT key` | 追加一个内容版本 | 指向新版本 |
| `DELETE key` | 追加一个**墓碑**版本（`deleted=true`，digest 为空），**不删 blob** | `null`（当前不可见） |
| `POST rollback {version}` | 仅把 `current` 切回某个**历史非墓碑**版本，**不复制 blob、不新建版本** | 指向目标版本 |

- `GET key` 读 `current`；`current=null` 即“已删除”，返回 `410 object_deleted`。
- `GET key?version=N` 可读任意历史内容版本；读墓碑版本返回 `404 version_not_found`。
- 删除后重新 `PUT`：版本号跨墓碑继续递增（例如 1、2、墓碑3、新内容4）。
- 对不存在的对象 `DELETE` 返回 `404 object_not_found`，**不会凭空造墓碑**；对已删除对象重复 `DELETE` 幂等，返回当前墓碑。

### 引用与垃圾回收（GC）
- **存活判定（标记）**：扫描所有对象的**全部版本记录（含历史版本，不含墓碑）**，凡被任一记录引用的 digest 即为存活。
- **清除（sweep）**：`POST /gc` 删除“磁盘存在但无任何版本引用”的 blob。
- 因此：
  - 覆盖写后旧版本 blob **不会**被回收（历史版本/回滚仍引用它）；
  - 多对象共享同一内容时，只要还有一个版本引用，blob 就存活；
  - 写入中断留下的“blob 已落、meta 未发布”孤儿不会被任何对象读到，由 GC 精确回收；
  - 已被元数据引用但物理缺失/损坏的 blob **不会**被 GC 当作垃圾删除（引用仍在），读取时报 `content_corrupted`。

## 3. 并发一致性保证

### 单进程多线程
- 服务内用一把可重入锁串行化所有元数据变更（`PUT/DELETE/ROLLBACK/GC`），读操作取元数据时短暂持锁。
- 因此并发写同一逻辑对象时：版本号必然是 `1..N` 不重复不跳号；`current` 最终精确指向某一个已完整提交的版本。
- `GC` 与一切变更互斥，杜绝“正在引用判定时新版本写入/删除”的竞争。

### 物理层原子性（崩溃安全）
写入一个 blob 的顺序固定为：

1. 在 `tmp/` 建临时文件，流式写入并同步计算 SHA-256，`fsync` 文件；
2. 若客户端给了期望值，先比对，不符即删临时文件并抛 `checksum_mismatch`；
3. 校验通过后 `os.replace` 原子改名到 `blobs/<ab>/<digest>`，并 `fsync` 目录。

`blobs/` 下永远只会出现“内容完整且哈希自洽”的文件，不存在半成品。

### 两阶段发布顺序（中断恢复）
```
先：blob 原子提交（内容寻址）
后：meta  原子发布（tmp + fsync + os.replace）
```
崩溃点分析：

- **崩在 blob 之前 / 之中**：只有 `tmp/` 暂存。重启构造服务时清空 `tmp/`，对象从未存在，读不到半成品。
- **崩在 blob 之后、meta 之前**：有孤儿 blob，但没有任何元数据指针指向它 → 对象返回 `object_not_found`；孤儿由 GC 回收。
- **崩在 meta 发布期间**：`os.replace` 原子性保证元数据要么是旧完整版本、要么是新完整版本。

读取路径在返回前对 blob **重算 SHA-256**：哈希不符 → `content_corrupted`（校验失败）；文件缺失 → `content_corrupted`（内容缺失）。绝不会把损坏字节当成功结果返回。

### 重复写不破坏已存在对象
目标 blob 已存在时，先校验现有副本：完好则丢弃本次暂存直接复用；万一现有副本已损坏，则用本次校验通过的新临时文件**原子替换**修复（此时不计为去重）。

## 4. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /healthz` | 健康检查 |
| `PUT /objects/{key}` | 覆盖写；请求头 `X-Content-SHA256` 可选校验；返回版本号与 `deduplicated` |
| `GET /objects/{key}?version=N` | 读当前/指定版本；响应头 `X-Object-Version`、`X-Content-SHA256` |
| `HEAD /objects/{key}` | 仅取版本与摘要头 |
| `DELETE /objects/{key}` | 逻辑删除（墓碑） |
| `POST /objects/{key}/rollback` | body `{"version": N}`，回滚可见指针 |
| `GET /objects/{key}/versions` | 列出全部版本（含墓碑）与 `current` |
| `POST /gc` | 回收无引用 blob，返回 `collected` digest 列表 |

错误响应统一为：

```json
{"error": {"code": "<稳定错误码>", "message": "<含 digest/版本/原因>"}}
```

| code | HTTP | 含义与判定依据 |
| --- | --- | --- |
| `checksum_mismatch` | 422 | 写入侧：实际 SHA-256 ≠ 客户端声明值，写入被拒绝 |
| `object_not_found` | 404 | 从无该对象（无元数据文件） |
| `object_deleted` | 410 | 对象存在但当前无可见版本（墓碑之后） |
| `version_not_found` | 404 | 对象存在但该版本号不存在，或该版本是墓碑 |
| `content_corrupted` | 500 | 读取侧：被引用 blob 物理缺失，或重算哈希不符 |

## 5. 运行与验证

```bash
# 安装依赖（需 Python >= 3.14）
uv sync

# 启动服务（数据目录可用 OSS_ROOT 覆盖，默认 ./data）
uv run uvicorn main:app --reload

# 全量测试（-s 可实时查看含 digest/版本/判定依据的日志）
uv run pytest -q
uv run pytest -s
```

### 手工验证示例

```bash
# 写入（带内容校验）
printf 'hello' | curl -sS -X PUT --data-binary @- \
  -H "X-Content-SHA256: $(printf 'hello' | sha256sum | cut -d' ' -f1)" \
  http://127.0.0.1:8000/objects/dir/a.txt

# 重复写相同内容到另一个 key -> deduplicated=true
printf 'hello' | curl -sS -X PUT --data-binary @- http://127.0.0.1:8000/objects/b.txt

# 覆盖写 / 读历史版本 / 列版本 / 删除 / 回滚
curl -sS -X PUT --data-binary 'hello-v2' http://127.0.0.1:8000/objects/dir/a.txt
curl -sS 'http://127.0.0.1:8000/objects/dir/a.txt?version=1'
curl -sS http://127.0.0.1:8000/objects/dir/a.txt/versions
curl -sS -X DELETE http://127.0.0.1:8000/objects/dir/a.txt        # 410 之后
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"version":1}' http://127.0.0.1:8000/objects/dir/a.txt/rollback

# 校验失败 -> 422 checksum_mismatch
curl -sS -i -X PUT --data-binary 'x' -H "X-Content-SHA256: $(printf '0%.0s' {1..64})" \
  http://127.0.0.1:8000/objects/bad

# 回收无引用 blob
curl -sS -X POST http://127.0.0.1:8000/gc
```

## 6. 测试覆盖

`tests/` 下 27 个用例，按场景组织：

- `test_dedup.py`：同内容单物理副本、跨 key 共享、异内容多副本、GC 引用判定、重复写不破坏已有对象；
- `test_versions.py`：版本号单调、逻辑删除与墓碑、历史可读、幂等删除、回滚复用引用不复制、删除后重写；
- `test_concurrency.py`：30 线程并发写同一 key（版本 1..N 不重不漏、每版本 blob 可校验、读到的内容永不半成品）、并发写相同内容去重、并发读写不变量；
- `test_recovery.py`：重启清空暂存半成品、blob 已落 meta 未发只剩可回收孤儿、版本/墓碑/回滚状态跨重启持久、无 `.meta-*.tmp` 残留；
- `test_integrity.py`：声明校验不符被拒绝且无残留、损坏/缺失检测、正确内容重写原子修复、5 种错误码逐一可区分；
- `test_api.py`：HTTP 状态码、错误体 code、去重标志、版本响应头。

测试日志统一打印 **内容标识（digest）、版本（version）与判定依据（依据=…）**，例如：

```
INFO 【GC-引用判定】referenced=('832864603d39','d11b889d5330') orphan=264ff08fc317 collected=['264ff08fc317'] 依据=任一版本记录(含历史)引用即存活,无引用即回收
INFO 【读取-损坏检测】expected_digest=34a989eb3e22 actual_digest=dd7046110708 code=content_corrupted 依据=读取后重算sha256与引用digest不符
```

## 7. 代码结构

```
app/
├── errors.py     # 5 种可区分错误（稳定 code）
├── hashing.py    # SHA-256 原语
├── blobstore.py  # 内容寻址物理层：原子写/去重/校验/枚举/删除/暂存清理
├── catalog.py    # 元数据层：ObjectState/Version，原子发布，引用集合枚举
├── service.py    # 门面：写锁、版本语义、GC、启动恢复，判定日志
└── api.py        # FastAPI 路由与错误码 -> HTTP 状态码映射
main.py           # create_app(OSS_ROOT)
tests/            # pytest 套件
```

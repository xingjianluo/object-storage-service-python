# 本地对象存储与版本管理服务（FastAPI）

以**本地磁盘**为对象层，提供：

- **内容寻址 + 去重**：相同内容（SHA-256 相同）全局只保留一份物理副本；
- **写入校验**：可携带 `X-Content-SHA256`，声明值与实际不符的写入被拒绝，且不留任何残留；
- **多版本管理**：覆盖写、逻辑删除（墓碑）、回滚的可见性语义明确；
- **单机多进程并发**：同一台机器上起多个服务进程共享同一数据目录时，跨进程文件锁保证同一对象版本号严格连续、不重不跳，成功返回即可读回对应内容；不同对象互不阻塞；
- **并发安全 GC**：回收与写入/删除互斥协调，任何被历史版本引用的内容、以及别的进程在途提交的内容都不会被误删，读取方拿不到残缺数据；
- **对象名无歧视**：点号开头（`.env`、`.gitignore`、`.config/settings.json`）及含斜杠/百分号/中文等特殊字符的名字与普通名字完全同权，写入、按版本读、回收一视同仁，旧数据目录直接打开即可；
- **强杀恢复**：写入/发布过程中被 `SIGKILL`，重启后数据目录回到一致状态，中间残留（暂存、meta 半成品）被发现并清扫且不累积，服务立即可读写；
- **损坏可检测**：读取时重算哈希，物理缺失或损坏返回与其他错误可区分的 `content_corrupted`。

## 1. 存储布局

```
<root>/
├── blobs/
│   └── <ab>/                     # 按内容 sha256 前 2 位分桶
│       └── <64hex>               # 不可变内容文件，文件名即内容 sha256
├── tmp/
│   └── .put-*                    # 写入暂存；启动恢复时在跨进程锁保护下清空
├── meta/
│   └── <ab>/                     # 按 sha256(key) 前 2 位分桶
│       └── <urlencoded-key>.json # 每个逻辑对象一个元数据文件
└── locks/                        # 跨进程文件锁（flock），按需自动创建
    ├── gc.lock                   # GC 屏障：变更持共享锁，GC 持排他锁
    └── obj/<ab>/<sha256(key)>.lock  # 每对象一把排他锁（空文件，可复用）
```

> **老数据目录无需迁移**：多进程能力只新增了 `locks/` 目录，服务启动时按需创建；
> 旧版本写出的 `blobs/`、`meta/` 布局与 schema=1 元数据完全不变，直接打开即可用。
> 锁文件是空的可复用文件，不是运行残留，不要手工删除（删除不影响数据正确性，
> 只会在被持锁时因重新创建产生一次轻微竞争）。

- **Blob 文件名 = 内容 SHA-256**。路径天然唯一，因此“同内容 = 同路径 = 单副本”，去重不需要额外索引。
- key 经 URL 安全编码（`/` 也被编码）落盘，杜绝路径穿越；对象自身再带 `key` 字段，加载时校验一致。

### 对象名（key）的合法范围与特殊字符

- **合法范围**：任意非空 UTF-8 字符串均可作为对象名，唯一硬性限制是
  **不能包含 NUL（`\x00`）字符**（操作系统不允许其出现在文件名中）；
  空字符串不是合法对象名。
- 下列名字都是**一等公民，与普通名字行为完全一致**（可写、可按版本读、
  正常参与去重与回收，重启/反复回收都不丢内容）：
  - **点号开头**：`.env`、`.gitignore`、`.bashrc`；
  - **点号开头的嵌套路径名**：`.config/settings.json`、`.ssh/known_hosts`；
  - **带斜杠 / 百分号 / 空格 / 中文等特殊字符**：`dir/a.txt`、
    `100%done.dat`、`.百 分%/x`、`中文/配置.env`；
  - 极端名字 `.`、`..`、`a/.b/..c` 同样可用。
- 实现要点：落盘文件名是 `quote(key, safe="") + ".json"`，其中 `/`、`%`、
  空格、中文等都会被百分号编码，`.` 与 `-`、`_` 等保持原样——因此
  `.env` 的元数据文件名就是 `.env.json`。**枚举对象时一律以
  “普通文件且文件名以 `.json` 结尾”为判据，绝不能把“点号开头”当隐藏文件
  跳过**；还原对象名时按固定后缀长度截取再 `unquote`（不能用 pathlib 的
  `suffix/stem`，key 恰为 `.`/`..` 时文件名是 `..json`/`...json`，
  前导点不被视为扩展名）。
- **旧数据目录零迁移**：编码与布局从未改变，旧版本写出的点号名字目录
  直接打开即可读、可正常参与回收，不需要手工改名或搬迁。
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
- **存活判定的边界**：
  - 标记阶段必须枚举到**每一个**对象的元数据文件，**对象名如何开头/含什么
    特殊字符一律不影响枚举**——尤其是 `.env`、`.gitignore`、
    `.config/settings.json` 这类点号开头（含嵌套路径）的对象，与普通名字
    完全同权；漏掉任何一个对象都会把仍被其（历史）版本引用的内容误判成垃圾。
  - 判定单位是“**是否还有任意一个版本记录（含历史版本）在引用这份内容**”，
    与该版本当前是否可见、对象名长什么样都无关；回滚到历史版本不复制内容，
    靠的就是历史引用始终存活。
  - 墓碑版本不引用任何 digest，因此“某内容只被某对象的墓碑提到”等价于无引用。
- **清除（sweep）**：`POST /gc` 删除“磁盘存在但无任何版本引用”的 blob。
  GC 在跨进程排他屏障下执行（见 3.2），可与其他进程的写入/删除安全并发，
  不会误删历史引用内容或别人在途提交的内容。
- **幂等与收敛**：GC 是“磁盘集合 − 引用集合”的确定性差集，连续多次执行
  结果一致：第一轮清掉全部孤儿之后，再跑恒为空集，磁盘集合稳定等于
  全量引用集合，不会时好时坏。
- 因此：
  - 覆盖写后旧版本 blob **不会**被回收（历史版本/回滚仍引用它）；
  - 多对象共享同一内容时，只要还有一个版本引用，blob 就存活；
  - 写入中断留下的“blob 已落、meta 未发布”孤儿不会被任何对象读到，由 GC 精确回收；
  - 已被元数据引用但物理缺失/损坏的 blob **不会**被 GC 当作垃圾删除（引用仍在），读取时报 `content_corrupted`。

## 3. 并发一致性保证（单机多进程共享数据目录）

### 3.1 跨进程锁模型
互斥不再依赖只在单进程内有效的线程锁，而是落在数据目录内的 `fcntl.flock`
文件锁（见 `app/locking.py`）。每次获取都独立 `open()` 一个独立的
open file description，因此同一进程内的多线程也严格按锁语义排队，
一把锁同时覆盖“多进程 + 多线程”。

每次变更（`PUT/DELETE/ROLLBACK`）按全局固定顺序加两把锁：

```
1) locks/gc.lock           —— 共享锁（SH）：所有变更共享共存
2) locks/obj/<...>.lock    —— 排他锁（EX）：同一逻辑对象串行
```

- **同一对象**：对象锁在所有进程间排他，同一 key 的覆盖写、删除、回滚
  被严格串行化 → 版本号严格连续、**不重不跳**；临界区里完成“分配版本号 +
  落 blob + 原子发布 meta”，所以一次成功返回后，其内容已持久化，
  之后任何进程读到该版本都能拿到一致内容，**不会被别的进程悄悄覆盖**
  （blob 内容寻址不可变，历史版本记录只追加）。
- **不同对象**：只共享 `gc.lock`，对象锁互不阻塞，不同 key 的并发操作
  互不影响、可真正并行。
- **读路径不持锁**：`GET/HEAD/versions` 不加任何锁。meta 以原子 rename
  发布、blob 不可变，读者任一时刻拿到的都是某个完整版本；GC 又只会删除
  “任何版本记录都不引用”的 blob，因此读永远不会读到残缺内容。
- 锁顺序全局唯一（先屏障、后对象锁），代码中不存在反向嵌套，因此
  **不可能死锁**；锁获取带 30s 默认超时，异常以 `LockAcquireTimeout` 暴露。

### 3.2 GC 与在途写入/删除的协调
`gc()` 获取的是 `locks/gc.lock` 的**排他锁**：

- 它必须等待所有已在途变更（持有共享屏障）结束才能开始；
- 它开始后，新的变更排队等待，直到 GC 结束。

于是 GC 看到的“磁盘 blob 集合”与“全量版本引用集合”是一个**没有任何
在途写入的稳定截面**：

- 任何被历史版本（含墓碑之前的旧版本、跨对象共享）引用的 blob 都存活，
  **不会误删历史内容**；
- “blob 已落、meta 未发布”的在途提交在 GC 期间不可能存在，
  **不会把别人刚提交但尚未可见的内容当垃圾**；
- 读者按版本记录取 blob，而被引用的 blob 必在，**不会得到残缺内容或
  把完好数据报成缺失**。

### 3.3 单进程 / 单线程模式
同一套文件锁在单进程、单线程下同样工作，既有用法与行为完全不变；
不需要任何特殊开关。

### 物理层原子性（崩溃安全）
写入一个 blob 的顺序固定为：

1. 在 `tmp/` 建临时文件，流式写入并同步计算 SHA-256，`fsync` 文件；
2. 若客户端给了期望值，先比对，不符即删临时文件并抛 `checksum_mismatch`；
3. 校验通过后 `os.replace` 原子改名到 `blobs/<ab>/<digest>`，并 `fsync` 目录。

`blobs/` 下永远只会出现“内容完整且哈希自洽”的文件，不存在半成品。

### 3.4 两阶段发布顺序与强杀恢复
```
先：blob 原子提交（内容寻址）
后：meta  原子发布（tmp + fsync + os.replace）
```
崩溃点分析（含进程被 `SIGKILL`、机器断电后重启）：

- **崩在 blob 之前 / 之中**：只有 `tmp/` 暂存。重启构造服务时在
  **排他 gc 屏障**保护下清空 `tmp/` 与 meta 树下的 `.meta-*.tmp`；
  持排他屏障意味着此刻不存在别的进程在途变更，清掉的只会是崩溃者的残留，
  不会误伤正在运行的进程。对象从未存在，读不到半成品。
- **崩在 blob 之后、meta 之前**：有孤儿 blob，但没有任何元数据指针指向它
  → 对象返回 `object_not_found`；孤儿由 GC 精确回收（不在每次启动时自动删，
  避免对刚加入目录的存活内容做任何激进动作）。
- **崩在 meta 发布期间**：`os.replace` 原子性保证元数据要么是旧完整版本、
  要么是新完整版本；`.meta-*.tmp` 半成品在下一次启动时被清扫。
- **锁的清理**：flock 由内核持有，进程无论怎么死，锁都会被内核自动释放，
  不会留下“谁都拿不到”的死锁；锁文件本身是可复用空文件，不随崩溃累积。

重启恢复完成后服务立即可用：被杀进程中“已成功返回”的版本全部仍在且可读，
版本号对该对象依然连续（临界区中断后，后来者在对象锁保护下重算版本号），
种子/其他对象数据不丢。

读取路径在返回前对 blob **重算 SHA-256**：哈希不符 → `content_corrupted`（校验失败）；文件缺失 → `content_corrupted`（内容缺失）。绝不会把损坏字节当成功结果返回。

### 重复写不破坏已存在对象
目标 blob 已存在时，先校验现有副本：完好则丢弃本次暂存直接复用；万一现有副本已损坏，则用本次校验通过的新临时文件**原子替换**修复（此时不计为去重）。

## 4. 多进程部署：数据目录要求与一致性范围

### 4.1 怎么起多进程
所有进程指向**同一个** `<root>` 即可，每个进程各自构造 `StorageService(root)`
（HTTP 方式就是多个 uvicorn/gunicorn worker 共用 `OSS_ROOT`）：

```bash
OSS_ROOT=/data/oss uv run uvicorn main:app --host 0.0.0.0 --port 8001 &
OSS_ROOT=/data/oss uv run uvicorn main:app --host 0.0.0.0 --port 8002 &
OSS_ROOT=/data/oss uv run uvicorn main:app --host 0.0.0.0 --port 8003 &
```

> 不要把多个进程各自指到同一目录的不同副本/快照（如绑定挂载的各容器私有
> 可写层）：那样它们看不到同一把锁文件与同一份 meta，互斥会失效。
> 必须是内核层面“同一个”目录。

### 4.2 对数据目录 / 文件系统的要求
1. **POSIX `flock(2)` 语义可用且对所有进程一致**（这是跨进程互斥的基础）；
2. `rename(2)` / `os.replace` 是**原子**的，`fsync` 生效（发布与持久性基础）；
3. 目录对所有参与进程可读写，进程由同一用户启动或对目录有一致权限。

| 介质 | 是否支持 | 说明 |
| --- | --- | --- |
| 本机本地盘（ext4 / xfs / btrfs） | ✅ 完全支持 | 推荐部署方式 |
| tmpfs / 本机内存盘 | ✅ 支持 | 仅注意断电即失，不适合持久数据 |
| **NFSv4** | ⚠️ 有条件支持 | 需服务端与客户端都启用并正确传递 flock（建议用 `nconnect`、避免与不持锁的外部客户端共用目录）；请先用第 6 节的多进程用例在该挂载点跑通 |
| NFSv3 / 旧式 NFS、`nolock` 挂载 | ❌ 不保证 | `flock`/`fcntl` 锁语义可能退化为本地锁，跨主机/跨进程互斥不可靠 |
| SMB/CIFS 网络共享、FUSE 网盘 | ❌ 不保证 | 锁与 rename/fsync 语义依实现而异，未经测试 |
| 对象存储（S3/OSS/COS 等）挂载 | ❌ 不支持 | 非文件系统语义，rename/lock 均不成立 |

### 4.3 一致性的适用范围（边界要说清）
- 支持范围：**同一台机器上、共享同一个本地（或满足上表条件的）目录的多个
  进程**，以及这些进程内的多个线程。
- 不覆盖：**跨多台机器**的分布式一致性（多机各自挂载网络盘时的时钟、
  锁租约、脑裂等不在保证内）；对同一份数据做**离线手工修改**后并发写入；
  绕过本服务、不持锁的外部写者。
- 一致性级别：对单个逻辑对象提供“每次成功写入立即可线性读到对应版本、
  版本号严格连续”的保证；不同对象间无全局顺序要求，也不提供跨对象事务。
- GC 提供“在任意写入/删除并发下只回收无引用 blob”的安全性，但**不提供
  容量的即时归零**：被杀在“blob 已落 meta 未发”之间的孤儿要等下一次
  `POST /gc` 才回收（这是有意为之，避免启动期的激进删除）。

### 4.4 崩溃后的恢复表现（小结）
| 崩溃时刻 | 重启后表现 |
| --- | --- |
| 写 blob 中途 | 暂存被启动恢复清掉，对象无该版本，无残留累积 |
| blob 已提交、meta 未发布 | 对象看不到该版本；孤儿 blob 等 GC 回收，其他数据完好 |
| meta 原子发布期间 | 生效的是旧版或新版之一，绝不半成品；`.meta-*.tmp` 被清扫 |
| 持有任何锁时死亡 | 内核自动释放锁，无死锁；后来者继续，版本号仍连续 |
| 恢复完成 | 立即正常读写，新版本接续当前最大版本号 |

## 5. HTTP 接口

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

## 6. 运行与验证

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

### 点号开头 / 特殊字符对象名的本地复现

历史缺陷（已修复）：旧版 `Catalog.all_keys()` 把“文件名以点号开头”的元数据
文件当隐藏文件跳过，`.env` 等对象因此从 GC 可达集合中消失，**一次回收就删掉
仍被版本引用的 blob**——版本列表照旧挂着，但按当前/历史版本读取都会报
`content_corrupted`。可用下面任一方式复现修复后的行为（在旧代码上会失败）：

```bash
# 方式 A：自动化回归（推荐）——单进程 + 多进程并发两组用例
uv run pytest tests/test_dotfile_gc.py -s
# -s 可看到逐行日志：对象 / 版本 / 内容digest / 判定依据 / 结果
# 多跑几轮确认并发场景无时序抖动：
for i in 1 2 3; do uv run pytest tests/test_dotfile_gc.py -q; done

# 方式 B：HTTP 手工验证（--data-binary 与 --path-as-is 保证名字原样传递）
# 1) 写入点号开头（含嵌套路径）与带 % / 中文的对象，各写两个版本
curl -sS -X PUT --data-binary 'env-v1'   --path-as-is http://127.0.0.1:8000/objects/.env
curl -sS -X PUT --data-binary 'env-v2'   --path-as-is http://127.0.0.1:8000/objects/.env
curl -sS -X PUT --data-binary 'settings' --path-as-is http://127.0.0.1:8000/objects/.config/settings.json
curl -sS -X PUT --data-binary 'pct'      --path-as-is 'http://127.0.0.1:8000/objects/100%25done'
# 2) 回收（可反复多跑几次）
curl -sS -X POST http://127.0.0.1:8000/gc
# 3) 当前版本与历史版本都必须原样读回，版本列表完整
curl -sS --path-as-is http://127.0.0.1:8000/objects/.env                 # env-v2
curl -sS --path-as-is 'http://127.0.0.1:8000/objects/.env?version=1'    # env-v1
curl -sS --path-as-is http://127.0.0.1:8000/objects/.env/versions
# 4) 再回收一次：collected 必须为 []（已收敛）；真正无引用内容上一轮已被清掉
curl -sS -X POST http://127.0.0.1:8000/gc
```

### 多进程行为的本地复现

**方式 A：自动化（推荐）**——真·独立 OS 进程（`spawn`）的并发套件：

```bash
# 只看多进程场景，-s 打印每个 worker 的 pid/key/版本/digest/依据/结果
uv run pytest tests/test_multiprocess.py -s

# 跑多轮确认无时序抖动（版本连续、存活不丢、无残留累积）
for i in 1 2 3; do uv run pytest tests/test_multiprocess.py -q; done
```

**方式 B：起多个真实 HTTP 进程打同一个目录**：

```bash
rm -rf /tmp/oss-mp && mkdir -p /tmp/oss-mp
for p in 8001 8002 8003; do
  OSS_ROOT=/tmp/oss-mp uv run uvicorn main:app --host 127.0.0.1 --port $p &
done
sleep 2

# 8 路并发覆盖写同一对象（-P 并行），请求在 8001/8002/8003 三个进程间轮换；
# 随后校验版本必须为 1..8，且每个版本都能读回对应内容
seq 8 | xargs -P8 -I{} sh -c 'p=$(( ({} - 1) % 3 + 8001 )); \
  curl -sS -X PUT --data-binary "content-{}" "http://127.0.0.1:$p/objects/hot"'
for v in $(seq 1 8); do
  echo "version=$v -> $(curl -sS "http://127.0.0.1:8001/objects/hot?version=$v")"
done
curl -sS http://127.0.0.1:8001/objects/hot/versions

# 一边持续写、一边在另一进程上反复 GC：应全部成功，存活内容不丢
( for i in $(seq 1 200); do
    curl -sS -X PUT --data-binary "load-$i" http://127.0.0.1:8002/objects/load;
  done ) &
for i in $(seq 1 50); do curl -sS -X POST http://127.0.0.1:8003/gc >/dev/null; done
wait
curl -sS -X POST http://127.0.0.1:8001/gc   # 收敛：磁盘集合 == 引用集合
```

**方式 C：强杀 + 重启恢复**（对应 `test_mp_sigkill_then_restart_recovery`）：

```bash
# 后台起 4 个持续写入的独立进程，随机时刻整体 kill -9，再用一个新进程打开目录
OSS_ROOT=/tmp/oss-mp uv run python - <<'PY'
# 详见 tests/test_multiprocess.py 中的 _worker_crash_writers：
# 多进程写 -> SIGKILL -> 重新 StorageService(root) -> 断言无 tmp/.meta-*.tmp 残留、
# 版本连续、已成功版本可读、新版本接续；最后 gc() 收敛孤儿。
PY
```

## 7. 测试覆盖

`tests/` 下 34 个用例，按场景组织：

- `test_dedup.py`：同内容单物理副本、跨 key 共享、异内容多副本、GC 引用判定、重复写不破坏已有对象；
- `test_versions.py`：版本号单调、逻辑删除与墓碑、历史可读、幂等删除、回滚复用引用不复制、删除后重写；
- `test_concurrency.py`：30 线程并发写同一 key（版本 1..N 不重不漏、每版本 blob 可校验、读到的内容永不半成品）、并发写相同内容去重、并发读写不变量；
- `test_recovery.py`：重启清空暂存半成品、blob 已落 meta 未发只剩可回收孤儿、版本/墓碑/回滚状态跨重启持久、无 `.meta-*.tmp` 残留；
- `test_multiprocess.py`：**真·多进程（spawn）**——① 多进程并发写同一对象版本 1..N 连续；
  ② 写/读/删/回滚混合，每个成功版本立刻可按版本读回且 sha256 自洽；
  ③ 不同对象并发互不影响、各自版本连续；
  ④ GC 与多进程写入并发，存活内容（含历史引用）零误删、读者永不残缺、收敛后磁盘==引用；
  ⑤ 多进程写入中 `SIGKILL` 强杀后重启恢复：版本连续、成功过的版本不丢、
  暂存与 meta 半成品三轮无残留累积、全新进程立即可读可写、孤儿由 GC 收敛；
- `test_integrity.py`：声明校验不符被拒绝且无残留、损坏/缺失检测、正确内容重写原子修复、5 种错误码逐一可区分；
- `test_api.py`：HTTP 状态码、错误体 code、去重标志、版本响应头；
- `test_dotfile_gc.py`：**点号开头/特殊字符对象名与回收回归**——
  ① 单进程：`.env`/`.gitignore`/`.config/settings.json` 及斜杠、百分号、中文、
  连续点号等名字各写 3 个版本，GC 前后逐版本读回校验、版本 1..N 连续，
  无引用孤儿被回收、连续多轮 GC 恒为空集收敛，回滚历史版本后再回收内容仍存活，
  重启打开旧目录直接可读可回收、无需改名迁移；
  ② 多进程（spawn）：4 个写进程同时覆盖写同一批 5 个点号/特殊名字对象，
  与 2 个持续 GC 的进程并发，断言全部成功提交版本零误删、按版本读回 sha256 一致、
  每对象版本严格连续，结束后磁盘集合==引用集合（孤儿回收干净）、重复 GC 结果一致。

worker 进程以结构化 JSON Lines 记录 **pid / 操作 / key / 版本 / digest / 判定依据 / 结果**
（`*.mp` 的 `worker-log` 行），父进程用例再以中文「依据=…」汇总；`-s` 可实时查看，例如：

```
INFO 【GC-引用判定】referenced=('832864603d39','d11b889d5330') orphan=264ff08fc317 collected=['264ff08fc317'] 依据=任一版本记录(含历史)引用即存活,无引用即回收
INFO 【读取-损坏检测】expected_digest=34a989eb3e22 actual_digest=dd7046110708 code=content_corrupted 依据=读取后重算sha256与引用digest不符
INFO 【第2轮强杀-重启恢复无残留】被杀进程=[9, 9, 9, 9] 已确认提交数=61 tmp暂存残留=0 meta半成品残留=0 依据=启动时持排他gc屏障清扫tmp与.meta-*.tmp;被杀者锁由内核释放
INFO worker-log {'pid': 3435338, 'op': 'put', 'key': 'crash-hot', 'version': 3, 'digest': '5aa1d0bce8aa', 'size': 12298, 'reason': '持对象锁分配版本并原子发布;被杀则锁由内核释放', 'result': 'ok'}
```

## 8. 代码结构

```
app/
├── errors.py     # 5 种可区分错误（稳定 code）
├── hashing.py    # SHA-256 原语
├── locking.py    # 跨进程文件锁：fcntl.flock，gc 屏障(共享/排他) + 每对象排他锁
├── blobstore.py  # 内容寻址物理层：原子写/去重/校验/枚举/删除/暂存清理
├── catalog.py    # 元数据层：ObjectState/Version，原子发布，引用集合枚举，半成品清扫
├── service.py    # 门面：跨进程锁、版本语义、并发安全 GC、启动恢复，判定日志
└── api.py        # FastAPI 路由与错误码 -> HTTP 状态码映射
main.py           # create_app(OSS_ROOT)
tests/            # pytest 套件
```

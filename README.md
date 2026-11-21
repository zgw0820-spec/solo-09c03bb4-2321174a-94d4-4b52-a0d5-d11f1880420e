# 离线航拍瓦片打包服务（Offline Tile Packer）

巡检车沿路线读取离线航拍图：FastAPI 接收一批 PNG 瓦片，两个本地工作进程将其转换为
PPM，按**提交顺序**写入 tar 包；整包完成后才开放下载。多批共享一个可配置的 PPM
暂存字节上限，最多两批同时打包。

## 做成的功能

| 需求 | 实现 |
| --- | --- |
| FastAPI 接收一批 PNG | `POST /batches`（multipart 多文件，附件顺序即提交顺序） |
| 两个本地工作进程转 PPM | `multiprocessing` 子进程 ×2（`WORKERS` 可配），Pillow 解码后自写 P6 PPM |
| tar 内瓦片按提交顺序排列 | 每批严格按序开工、按序入包；条目名带序号前缀 `000000_xxx.ppm` |
| 两批同时打包 | `MAX_ACTIVE_BATCHES=2`，其余批次排队，空位即补 |
| 共用可配置的 PPM 暂存字节上限 | 全局限额 `PPM_STAGING_BYTES`，转换中+已转换未入包的瓦片共同占用 |
| 转换中按完整 PPM 大小占额度 | 派发前从 PNG IHDR 算出精确 PPM 尺寸并预留；实际尺寸有出入时按实修正 |
| 入包并清除暂存后才归还额度 | tar 追加完成且暂存文件删除后才释放该瓦片额度 |
| 后序瓦片先转好须留待入包 | 已转换瓦片驻留暂存区，等全部前序瓦片入包后才轮到自己 |
| 不挤占下一块应入包瓦片的开工空间 | 每批严格按序开工：后序瓦片在"下一块应入包瓦片"开工前根本不占额度 |
| 额度与写包顺序允许时两进程同时转换 | 额度够即向空闲 worker 派发，实测 `inflight=2` |
| 单片超限/转换失败 | 该批标记 `failed`，报出瓦片名与原因，释放其全部额度、删除暂存与半成品 tar |
| 另一批照常处理 | 额度按瓦片独立记账，失败批不影响其他批；排队批次顶上空位 |
| 转换进程意外退出 | 看门狗 0.2s 内发现，只有丢了转换结果的批次判失败并报名字；其余进程保留，自动补足配置进程数（见"实测三"） |
| 整包完成才开放下载 | 仅 `done` 状态可 `GET /batches/{id}/download`，否则 409 |
| Docker Compose 启动 | `docker compose up --build`，见下 |
| Swagger 提交与取包 | `/docs` 可上传多文件、查询状态、下载 tar（二进制流） |

## 架构与关键机制

```
uvicorn (FastAPI, 事件循环)
 ├── 调度器 Scheduler（额度记账、按序派发、按序写 tar、失败清理、worker 看门狗）
 ├── multiprocessing.Queue ×1（结果/回执）+ 每 worker 一条私有任务队列
 └── worker 子进程 ×2（PNG → PPM，Pillow；异常退出后自动补新进程）
```

- **额度模型**：`used = Σ(转换中瓦片的完整 PPM 尺寸) + Σ(已转换未入包瓦片的尺寸)`；
  派发时预留、入包并删暂存后归还；任何时刻 `used ≤ PPM_STAGING_BYTES`。
- **按序开工**：每批维护 `next_to_start` / `next_to_pack` 两个指针，只派发该批
  下一块未开工瓦片。后序瓦片绝不可能比"下一块应入包瓦片"更早占用额度，因此
  暂存中的后序瓦片永远挤不掉它开工所需的空间（结构性保证，而非事后补救）。
- **跨批调度**：按激活先后轮询各活动批，额度够才开工；单批被额度卡住时其他批
  仍可推进，不会死锁（每批的流水线都会按序排空）。
- **失败隔离**：单片 PPM 超限（派发前判定）或转换异常（worker 上报）→ 该批
  `failed`：`failed_tile` 记录瓦片名，释放该批全部预留、删除暂存 PPM 与半成品
  tar；仍在转换中的该批瓦片结果到达时直接丢弃并删文件。其他批不受影响。
- **worker 看门狗**：每个 worker 一条私有任务队列，调度器因此确切知道哪个瓦片
  （若有）随进程丢失。worker 异常退出时：
  1. 立即 reap 并**补一个新进程**（存活 worker 不动，池子回到 `WORKERS` 配置数）；
  2. **宽限 1s** 等在途消息——退出前已交回的结果照常处理；worker 取任务时会先
     回执 `started`，没回执说明任务未被取走，直接**改派**给存活/新补的 worker，
     该批不判失败；
  3. 只有真正丢了转换结果的批次才 `failed` 并报名字。额度按瓦片归还：丢失瓦片
     在进程 reap（不可能再写暂存）并删除残缺 PPM **之后**才还；该批已转好驻留的
     瓦片由清理任务删文件后归还；仍在其他 worker 上转换的瓦片等结果到达、文件
     删除后才归还。
- **tar 写入**：`tarfile` 顺序追加，条目名 `{序号:06d}_{原名}.ppm`；整包写完
  关闭后才置 `done` 开放下载。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PPM_STAGING_BYTES` | 67108864（镜像内）/ 20971520（compose 演示） | 共享 PPM 暂存字节上限 |
| `WORKERS` | 2 | 本地转换进程数 |
| `MAX_ACTIVE_BATCHES` | 2 | 可同时打包的批次数 |
| `DATA_DIR` | `/data` | 上传/暂存/产物根目录 |

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 上传一批 PNG（`files` 多文件，顺序即打包顺序），返回批次状态 |
| `GET` | `/batches` | 列出全部批次 |
| `GET` | `/batches/{id}` | 批次详情（状态、进度、逐瓦片状态、失败瓦片名与原因） |
| `GET` | `/batches/{id}/download` | 下载 tar（仅 `done`；否则 409 并说明） |
| `GET` | `/status` | 全局限额/已用、在转换数、活动/排队批次、各 worker 进程 pid 与当前任务 |

## 启动

```bash
docker compose up --build
# Swagger UI: http://localhost:8000/docs
```

本地开发（等价命令）：

```bash
pip install -r requirements.txt
DATA_DIR=/tmp/tiler_data PPM_STAGING_BYTES=20971520 WORKERS=2 MAX_ACTIVE_BATCHES=2 \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 实测结果

环境：Python 3.11，本机直接运行 uvicorn（与容器相同的进程结构与 env）。
测试资产由 `scripts/gen_tiles.py` 生成：big 15,600,017 B、small×4 各 3,145,745 B、
medium 12,582,929 B、huge 27,000,017 B（超 20 MiB 限额）、truncbig（头完整身截断）、
junk（非 PNG）。驱动：`scripts/smoke_test.py`，共 26 项断言，**全部通过**。另做了
45 批交错压力测试（成功/超限/损坏混发）：0 异常堆栈，结束时额度归零、暂存目录
无残留、产物 tar 数与完成批数一致。

### 实测一：首片慢（顺序保持 / 并发转换 / 额度门控）

提交 `[big, small1..4]`（big 转换 205ms，small 约 20–60ms），服务日志：

```
tile=0 big.png   DISPATCH reserve=15600017 used=15600017/20971520 inflight=1
tile=1 small1.png DISPATCH reserve=3145745 used=18745762/20971520 inflight=2   ← 两进程同时转换
tile=1 small1.png CONVERTED ... duration=62ms                                   ← 后序瓦片先转好，留待入包
tile=0 big.png   CONVERTED ... duration=205ms
tile=0 big.png   PACKED as=000000_big.ppm   used=3145745                        ← 入包+清暂存后才归还
tile=1 small1.png PACKED as=000001_small1.ppm used=0                            ← 严格按提交顺序入包
tile=2 small2.png DISPATCH ...                                                  ← 此前 18.7M+3.1M 超限，被额度挡住
```

- tar 条目顺序实测：`000000_big.ppm, 000001_small1.ppm, …, 000004_small4.ppm` ✔
- 全程 `used` 峰值 18,745,762 ≤ 20,971,520 ✔；首片转换期间第三片因额度未开工 ✔

### 实测二：并发打包中一批失败

同时提交 A=`[medium, truncbig, small2]`、B=`[small3, medium, small4, small1]`、
C=`[small2, small3]`：

```
batch=A ACTIVATED ... active=1        batch=B ACTIVATED ... active=2   ← 两批同时打包
（C 保持 queued，直到 A 失败让出空位）
batch=A FAILED tile=truncbig.png error='conversion failed: OSError: image file is truncated'
        released=18745762 used=0/20971520                              ← 报名字、清暂存、还额度
batch=C ACTIVATED ...                 batch=B ... DONE                 ← 另一批照常完成
batch=A tile=2 result DISCARDED (batch state=failed)                   ← 在飞瓦片结果被丢弃清理
```

- A 下载 → 409；B、C 下载 → 200 且条目有序 ✔；结束后 `used=0` ✔

### 实测三：转换进程中途退出（`scripts/worker_exit_test.py`）

提交 A=`[small1, big, small2]`、B=`[small3, medium, small4]`、C=`[small1, small2]`
（C 排队）。A 的 big（15.6MB）转换中途，对其 worker **SIGKILL**；此刻 A 占住
18.7MB 额度，B 连第一片都开不了工（`started=0`）——正是本次要修的卡死场景：

```
batch=A tile=1 big.png DISPATCH worker=0 reserve=15600017 used=18745762/20971520
（SIGKILL pid=502）
worker=0 pid=502 EXITED unexpectedly exitcode=-9 in_flight=('A', 1)
worker=2 pid=517 STARTED alive=2/2                                   ← 立即补新进程，幸存者不动
batch=A tile=1 big.png LOST remnant removed, released=15600017       ← reap+清残件后才还额度
batch=A FAILED tile=big.png error='conversion worker exited ... result was lost'
batch=C ACTIVATED                                                     ← 排队批顶上空位
batch=A CLEANUP released=3145745                                      ← 已转好驻留片删文件后归还
batch=C DONE   batch=B DONE                                           ← 其他批沿原进度出包
```

断言（21 项，全过）：A 判负并报名 `big.png`、下载 409；B、C 的 tar 按提交顺序
完整可下载；存活 worker pid 不变、恰好补一个新进程；最终 `used=0`、失败批
`held=0`、暂存目录无残件。同脚本 TEST 2：把 worker **SIGSTOP 在取任务前**再
SIGKILL——任务未被取走，不判失败，日志 `REDISPATCHED to worker=2`，批次正常
完成。另单独验证：空闲 worker 被杀只补进程、无人判负。累计 8 次强杀 + 杀后
重跑 26 项冒烟断言，全部通过。

### 其余实测

- 单片超限：`[small1, huge]` → `failed_tile=huge.png`，"exceeds the 20971520 byte
  staging limit"，暂存清零，下载 409 ✔
- 非 PNG：`[junk]` → `failed_tile=junk.png`，"invalid PNG" ✔
- 打包中下载 → 409；完成后 → 200，PPM 内容校验（`P6\n2600 2000\n255` + 精确字节数）✔

## 遗留事项

1. **compose 未在本环境实跑**：交付环境无 Docker 守护进程，`Dockerfile` /
   `docker-compose.yml` 未实际构建；已用相同 env 在本机直接运行 uvicorn 全量实测
   通过（含 worker 强杀场景）。首次构建需联网安装依赖。
2. **状态不持久**：批次状态在内存中，重启即丢失；启动时会清空 `DATA_DIR` 下的
   上传/暂存/产物目录（视为全新实例）。需要续跑能力得加持久化队列。
3. **worker 挂起无超时**：看门狗只处理"进程退出"（`is_alive()` 为假）；worker
   活着但卡死（如解码死循环）不会被发现，需要任务级超时心跳才能覆盖。
4. **上传的 PNG 不占额度**：限额只覆盖 PPM 暂存；上传目录无大小限制，失败批次的
   原始 PNG 保留在 `uploads/` 便于排查，需自行清理。
5. **跨批公平性从简**：活动批按激活先后轮询抢额度，单批被额度卡住时其他批可继续
   推进（吞吐优先）；未实现跨批预留/严格 FIFO。
6. **单机单进程额度**：限额是单个 API 进程内的记账，多副本部署不共享（符合
   "本地工作进程"设定）。
7. **看门狗簿记随重启次数缓慢增长**：`_workers_by_id` 为迟到结果保留已死 worker
   的条目（每条几十字节，仅在崩溃重启时增加一条）；丢失判定有 1s 宽限，极端
   慢的结果通道下"退出前已交回"与"丢失"的边界以宽限期为准。

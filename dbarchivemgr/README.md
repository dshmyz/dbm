# DBArchiveManager

数据库备份策略优化系统的**可运行服务骨架**。统一调度枢纽（**不传输文件**，只做**调度、校验、元数据、指令下发**），覆盖：

> 备份上报 → 双卷存在性判定 → 每日完整性巡检 → 缺失补发 → 滚动归档清单 → 归档结果校验 → GFS 安全清理。

以**纯内存模拟桩**运行，无需真实 MySQL / GlusterFS / HDFS 环境。

**内置可视化管理后台**：服务启动后打开 `http://<addr>/ui/`（如 `http://localhost:8090/ui/`），纯静态单页（`go:embed` 打进二进制，零外部依赖），覆盖概览/集群拓扑/备份元数据/巡检/补发/归档/清理 8 个视图，并支持在页面触发巡检、补发、滚动清单、清理等操作（数据全部来自 `/api/v1` 只读列表接口）。

## 架构：管控出站只对接 GFS 管理端（不直连大数据）

管控为调度中枢，**不内联读写文件**——出站三类请求全部打 **GFS 管理端**（多机房时按集群 region 分发到对应机房的管理端）：巡检/复核的 stat、清理的 delete、归档清单下发（转发大数据是 GFS 的职责）、补发指令下发（**DB 代理监听同在 GFS 管理端**，由其转交 DB 侧重跑备份）。**管控不直连大数据**——大数据唯一的入站动作是回调管控：

```
            ┌────────────┐  [HTTP 管控→GFS]             ┌─────────┐
   DB ──写──▶│   GFS 组件  │◀── stat / delete / 归档清单 ──│  管控    │
   (双卷)    │ GlusterFS  │──转发归档通知──┐               └────┬────┘
            └─────┬──────┘               │[HTTP]               │ ▲ 补发状态回调
                  ▲ 文件拉取(定向)         ▼                     │ │ 归档结果回调
            ┌──────────────────┐   ┌──────────┐                │ │
            │      大数据组件    │──▶│  HDFS    │                ▼ │
            │ 自治拉取→写HDFS   │   └──────────┘      ┌──────────────┐
            └─────────┬────────┘                     │ DBArchiveManager │
                      └───────── 回调 ──────────────▶│  /archive/result │
   管控 ──补发指令(HTTP)──▶ GFS 管理端 ──转交──▶ DB 代理 ──重跑备份写双卷──▶ GFS
```

- **GFS 组件**（文件载体 + 清单转发）：`GET /v1/fs/stat?path=` `{exists,size}`、`DELETE /v1/fs/delete`、`POST /v1/archive/notify`（接收管控归档清单并**转发大数据**）；写入由 DB 直写挂载目录，不归管控。
- **大数据组件**（归档目标）：收到 GFS 转发的清单后**自治**拉取 GFS→写 HDFS→回调 `/api/v1/archive/result`；管控不感知 HDFS。
- **DB 代理**（补发执行方）：监听地址同在 GFS 管理端，接收 `POST /v1/replenish/notify` 转交，重跑备份重写缺失卷后回调 `/api/v1/replenish/task/status`。
- 桩（`internal/sim`）、大数据（`internal/bigdata`）均以 `httptest.Server` 暴露为独立 HTTP server；管控用真实 `http.Client` 访问。

## 核心设计

- **集群 ↔ 实例 IP 关联**：备份文件路径以 IP 标识（`/gfs/<ip>/<db>/<date>/{city|dr}/full.sql.gz`）；系统维护 `tb_cluster_instance`（一个 IP 只属于一个集群，一集群可多 IP），上报/巡检时按 IP 反查所属集群与库。
- **GFS 路径模板可自定义（按集群/库各配各）**：模板占位符 `{root}{cluster}{db}{date}{vol}`（`vol`=city/dr 双卷标识），库级 → 集群级 → 全局默认三级回退；支持按模板**生成**双卷路径、以及从一条路径**反向解析**出 `cluster/db/date/vol`（供"只给路径自动归属"）。例：`{root}/prod/{cluster}/{db}/{date}/{vol}/orders.sql.gz`。
- **GlusterFS 双卷**：异地备(`/gfs/.../dr/`) 与 同城备(`/gfs/.../city/`) 是两个独立 GlusterFS 卷，数据库把同一份备份写入两条路径。
- **上传判定**：`文件存在 && size > 10MB 阈值`（FUSE 挂载本地目录语义的 `stat`），**不做 MD5**。
- **权威模型**：GFS 是唯一读写载体；双卷文件「存在 + 非空」是上报/归档的前提。
- **归档状态机**：`NOT_ARCHIVED → ARCHIVING → ARCHIVED`（终态），失败分支 `ARCHIVE_FAILED`，重试可回 `ARCHIVING`。
- **唯一业务维度**：(cluster_id, db_id, backup_date, backup_edition)，用于元数据幂等与补发防抖。
- **滚动窗口（库级可配）**：滚动归档清单按**每个库自身的归档窗口**纳入——`DB.ArchiveWindowDays` → 所属 `Cluster.ArchiveWindowDays` → 默认 15 天，已过该库窗口的备份才纳入归档 batch；元数据去重，每份备份只归档一次。
- **GFS 留存（集群级可配）**：清理仅删 `已归档 && 距归档已满该集群留存天数`——`Cluster.RetentionDays`（`0` = 归档后即可清理，`nil` = 默认 10 天）；未满留存不清，`/cleanup` 与 `/backup/archive-status` 的 `canDelete/pendingDeleteAt` 均按留存如实计算。
- **定时调度**（cron）：自动触发「完整性巡检 + 缺失自动下发补发」「滚动归档清单生成」，替换手动 curl；演示默认秒级（`@every 15s/30s`），生产切为每日凌晨（`-schedule-demo=false`）。

## 目录结构

```
dbarchivemgr/
├── cmd/server/main.go        # 启动入口（装配 GFS/大数据 HTTP server + Gin 路由 + 种子演示数据）
├── internal/
│   ├── model/model.go        # GORM 模型（对应 DDL 10 张表）
│   ├── store/store.go        # SQLite 元数据库 + 自动迁移
│   ├── state/state.go        # 归档状态机（含单测）
│   ├── sim/sim.go            # GlusterFS 双卷 / HDFS 纯内存桩 + GFS Handler()
│   ├── gfsclient/gfsclient.go# 管控对 GFS 的访问面（FileStore：HTTP 客户端 / 内存桩）
│   ├── notify/notify.go      # 管控向外部组件的指令下发（补发 / 归档清单，走 HTTP）
│   ├── bigdata/bigdata.go    # 大数据组件 HTTP server 桩（自治拉取→写HDFS→回调管控）
│   ├── service/service.go    # 核心业务（上报/巡检/补发/归档/清理，经 FileStore 访问 GFS）
│   ├── scheduler/scheduler.go# 定时调度（cron：巡检/补发/滚动清单，含单测）
│   ├── api/api.go            # REST 接口层（/api/v1/*）
│   ├── integration/          # 跨包 HTTP 全链路集成测试
│   ├── demo/demo.go          # 演示种子数据
│   └── web/                  # 管理后台（go:embed 静态页，/ui）
└── scripts/demo.sh           # 一键全链路演示脚本
```

## 快速开始

```bash
cd dbarchivemgr
go mod tidy
go build -o bin/dbam ./cmd/server

# 一键跑通全链路演示（自动起 server → curl 各接口 → 关停）
chmod +x scripts/demo.sh && ./scripts/demo.sh
```

或单独启动并用 curl / 浏览器调用：

```bash
go run ./cmd/server -addr :8090
# 打开 http://localhost:8090/ui/ 使用可视化管理后台；或 curl /api/v1/* 接口
```

`demo.sh` 内部已按序执行：健康检查 → 对账 → 巡检 → 补发 → 滚动清单 → 归档结果 → 状态查询 → 清理 → 对账。

## 测试

```bash
go test ./...
```

- `internal/state/state_test.go`：状态机合法 / 非法转移、幂等、归档 size 校验通过 / 失败。
- `internal/service/service_test.go`：纯内存桩全链路集成测试（上报→巡检→补发→归档→清理）。
- `internal/integration/integration_test.go`：**HTTP 全链路集成测试**——GFS / 大数据均以 `httptest.Server` 起独立 HTTP server，验证管控经真实 `http.Client` 访问组件、大数据自治拉取写 HDFS 后回调管控。

## 主要接口（/api/v1）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/cluster` | 注册集群 |
| POST | `/cluster/import` | CSV 批量导入集群/库/实例（multipart 字段 `file`，clusterCode 关联，幂等覆盖） |
| PUT | `/cluster/:id` | 更新集群 |
| DELETE | `/cluster/:id` | 删除集群（级联删除其下库+实例，备份/归档历史保留） |
| PUT | `/db/:id` | 更新库 |
| DELETE | `/db/:id` | 删除库（备份/归档历史保留） |
| PUT | `/instance/:id` | 更新实例 |
| DELETE | `/instance/:id` | 删除实例 |
| GET | `/gfs/endpoints` | 机房 GFS 管理端配置列表（管理后台「机房管理」） |
| POST | `/gfs/endpoints` | 新增机房管理端（region 唯一，`*`=兜底；写后即时生效） |
| PUT | `/gfs/endpoints/:id` | 更新机房管理端 |
| DELETE | `/gfs/endpoints/:id` | 删除机房管理端 |
| POST | `/cluster/:id/db` | 绑定库到集群 |
| GET | `/cluster/:id` | 查询集群拓扑 |
| POST | `/cluster/:id/instance` | 给集群挂载实例 IP（按 IP 反查集群） |
| GET | `/cluster/:id/instances` | 查询集群下的实例 IP 列表 |
| GET | `/clusters` | 集群列表（管理后台） |
| GET | `/dbs?clusterId=` | 库列表（可选按集群过滤，管理后台） |
| GET | `/backup/meta?clusterId=&dbName=&archiveStatus=` | 备份元数据列表（条件均可选，管理后台） |
| GET | `/inspects?limit=` | 巡检历史（倒序，管理后台） |
| GET | `/replenish/tasks` | 补发任务列表（管理后台） |
| GET | `/archive/batches` | 归档批次列表（管理后台） |
| POST | `/backup/meta/report` | 上报备份元数据（按 hostIp 反查集群 + 双卷存在+非空判定） |
| POST | `/inspect/run` | 每日完整性巡检 |
| GET | `/inspect/:id` | 巡检结果与缺失明细 |
| POST | `/replenish/task` | 下发补发指令 |
| POST | `/replenish/task/status` | 补发状态回调 |
| POST | `/archive/rolling-list/generate` | 生成滚动归档清单（经 HTTP 通知大数据自治拉取） |
| POST | `/archive/result` | 归档结果上报（大数据回调；size 校验 → ARCHIVED） |
| GET | `/backup/archive-status` | 归档状态查询（清理判定） |
| GET | `/transfer/status` | 传输状态查询 |
| POST | `/cleanup` | GFS 清理 |
| GET | `/meta/reconciliation` | 归档状态对账 |
| GET | `/schedule/status` | 定时调度状态（各任务 cron 规格 + 最近运行摘要） |

### 集群管理：CSV 批量导入 + 编辑/删除

「集群拓扑」管理后台支持 **CSV 批量导入**（一次建立「集群 + 库 + 实例 IP」完整结构）与**逐条编辑/删除**。

**CSV 格式**：一行一个实体，首列 `type` 区分 `cluster` / `db` / `instance`；可选表头（首行首列为 `type` 即跳过）。列序固定为：

```
type,clusterCode,clusterName,primaryRegion,drRegion,cityRegion,archiveWindowDays,retentionDays,pathTemplate,dbName,dbEditions,dbWindowDays,dbArchiveTime,dbPathTemplate,hostIp,hostName,role
```

- `clusterCode` 是**关联键**：`db` / `instance` 行靠它定位所属集群（找不到则整行报错）。
- `dbEditions` 用 `;` 分隔（如 `DR;SAME_CITY`），避免与 CSV 逗号冲突。
- `retentionDays`：`0`=归档后立即清、数字=留存天数、留空=未配置（默认 10）；`archiveWindowDays` 留空用默认 15。
- **逐行容错**：某行错误只计入汇总的 `errors`，不中断整体。
- **幂等**：集群按 `clusterCode`、库按 `(cluster, dbName)`、实例按 `host_ip` upsert——重复导入覆盖更新、不报错。

示例（`10.1.1.10` 属性 `MASTER`、库 `orders/pay` 各配窗口与模板）：

```csv
type,clusterCode,clusterName,primaryRegion,drRegion,cityRegion,archiveWindowDays,retentionDays,pathTemplate,dbName,dbEditions,dbWindowDays,dbArchiveTime,dbPathTemplate,hostIp,hostName,role
cluster,CL-AT-01,核心交易,杭州A,杭州B,杭州同城,15,10,,,,,,,,
db,CL-AT-01,,,,,,,,orders,DR;SAME_CITY,10,08:00,,,
instance,CL-AT-01,,,,,,,,,,,10.20.30.41,db-orders-01,MASTER
```

> 后台「集群拓扑」页顶部可**单条新增集群**、**下载 CSV 模板**、**导入 CSV**；集群卡片右上角可**编辑/删除**集群，并按「+ 库」「+ 实例」单条新增库与实例 IP（删除均二次确认；删除集群级联删库+实例，但保留备份元数据与归档历史）。

### 定时调度启动参数

```bash
# 生产模式：每日凌晨触发（巡检 3:00 / 滚动清单 2:00）
go run ./cmd/server -schedule-demo=false

# 演示模式（默认）：秒级触发便于观察（巡检 15s / 滚动 30s）
go run ./cmd/server

# 显式指定 cron 规格（覆盖默认）：
go run ./cmd/server -schedule-spec "@every 10s" -rolling-spec "0 0 2 * * *"
```

调度器默认 `-window-days 15`，与种子数据对齐；`GET /api/v1/schedule/status` 可查看各任务最近运行时间与结果。

### 组件地址可配置（线上连真实 GFS 管理端）

管控出站只对接 **GFS 管理端**一处（stat/delete + 归档清单转发 + 补发指令，DB 代理监听同在管理端）；**不直连大数据**（归档结果由大数据回调进来）。线上部署时把地址用**启动参数**（或同名环境变量）指到真实服务，或直接在管理后台「机房管理」页配置，**无需改代码**。

| 参数 | 环境变量 | 说明 | 缺省 |
|---|---|---|---|
| `-gfs-url` | `DBAM_GFS_URL` | **GFS 管理端地址**（stat/delete + 归档清单转发 + 补发指令，如 http://gfs:8200） | 内置桩 |
| `-gfs-map` | `DBAM_GFS_MAP` | **多机房 GFS 映射**：机房→管理端地址（JSON 内联或 `@file`），按集群 cityRegion/drRegion 分发；推荐在界面「机房管理」配置 | 未配置=单地址模式 |
| `-public-url` | `DBAM_PUBLIC_URL` | 管控对外地址，供大数据组件回调 `/archive/result` | 取监听地址 `-addr` |
| `-cleanup-mode` | `DBAM_CLEANUP_MODE` | 清理触发方：`mgr`=管控经 HTTP 触发删除（默认/现状）；`gfs`=管控仅放行判定、GFS 自治清理（原始设想） | `mgr` |
| `-cas-url` | `DBAM_CAS_URL` | CAS 服务根地址（如 `https://cas.example.com/cas`）；**空=演示模式不鉴权** | 空（演示模式） |
| `-cas-validate-path` | `DBAM_CAS_VALIDATE_PATH` | CAS ticket 校验路径（CAS 3.x: `/p3/proxyValidate`；CAS 2.x 服务端可用 `/serviceValidate`） | `/p3/proxyValidate` |

> 优先级：界面「机房管理」库表 **>** `-gfs-url`/`-gfs-map` 静态映射 **>** 内置桩（全都不配则纯演示）。出站统一指向 GFS 管理端，无其他地址参数。
> 管控**不做任何 HDFS 操作、不直连大数据**：HDFS 完全归大数据组件内部自有；归档清单与补发指令都发 GFS 管理端，由其转发大数据/DB 代理。

### 多机房 GFS：按机房映射多个管理端（界面配置 / `-gfs-map`）

各机房独立部署 GlusterFS 集群（同城卷在 A 机房、异地卷在 B 机房，或更多）且无统一网关时，管控可配置**机房→GFS 管理端**映射，把 stat/delete 按集群的 `cityRegion`（同城卷）/`drRegion`（异地卷）分发到对应机房的管理端。

**推荐在管理后台「机房管理」页配置**（存库表 `tb_gfs_endpoint`，增删改即时生效，30s 缓存/写后失效）；启动参数 `-gfs-map` 作为免界面的引导/兜底方式：

```bash
# 内联 JSON（"*" 键为未匹配机房的兜底管理端）
go run ./cmd/server \
  -gfs-map '{"杭州同城":"http://gfs-hz:8200","乌兰察布":"http://gfs-wlc:8210","*":"http://gfs:8200"}'

# 或指向映射文件；兜底也可用 -gfs-url 给出
DBAM_GFS_MAP=@/etc/dbam/gfs-map.json go run ./cmd/server -gfs-url http://gfs:8200
```

- **配置优先级**：库表 `tb_gfs_endpoint`（界面「机房管理」）**>** `-gfs-map`/`-gfs-url` 静态映射 **>** 单地址；库表为空时回落启动参数，两者皆无则全走单地址。
- **分发规则**：备份主卷按版本推卷——同城备(SAME_CITY)主卷=city→查 `cityRegion`，异地备(DR)主卷=dr→查 `drRegion`；对卷取另一卷的 region。上报双卷复核、巡检、补发定位、清理删除、归档清单取数均按此分发。
- **未匹配机房**（region 未配/为空）回落兜底管理端：`*` 端点 > `-gfs-url` > 映射中字典序首个机房；无任何兜底时视为文件不存在（照常进补发流程，不 panic）。
- 配置映射即进入外部 GFS 模式（不再注入演示种子数据）；判定语义不变——双卷各自 stat、任一机房不达标即上报失败/进缺失清单。

### 清理触发方可配置（原始设想 vs 现状）

原始方案设想「GFS 自治清理」（GFS 每日自己删、删前查管控归档状态）；当前实现为「管控触发清理」（管控遍历已归档、满留存的备份，经 HTTP 让 GFS 删）。两者通过 `-cleanup-mode` 切换：

- **`mgr`（默认/现状）**：`POST /api/v1/cleanup` 执行时，管控对每份「已归档 + 满该集群留存天数」的备份调用 `DELETE /v1/fs/delete` 让 GFS 删文件。
- **`gfs`（原始设想）**：`POST /api/v1/cleanup` 只做**放行判定**（返回清点结果），**不实际删除** GFS 文件；真正的删除由外部 GFS 组件自治执行——GFS 删前经 `GET /api/v1/backup/archive-status` 的 `canDelete/pendingDeleteAt` 判定满留存，满足才删。

```bash
# GFS 自治清理模式（贴合原始方案）
go run ./cmd/server -cleanup-mode gfs
DBAM_CLEANUP_MODE=gfs go run ./cmd/server
```

```bash
# 线上（推荐）：出站统一指向 GFS 管理端，生产每日凌晨调度
go run ./cmd/server \
  -schedule-demo=false \
  -gfs-url  http://gfs-gateway:8200 \
  -public-url http://dbam:8090

# 等价：全走环境变量（或直接在管理后台「机房管理」页配置）
DBAM_GFS_URL=http://gfs-gateway:8200 DBAM_PUBLIC_URL=http://dbam:8090 \
  go run ./cmd/server -schedule-demo=false

# 本地/演示：什么都不配，用内置桩（demo.sh 即此模式）
go run ./cmd/server
```

- 生产需配 `-public-url`（或让其暴露的入站地址等于 `-addr`）保证大数据组件能回调到管控。
- 外部 GFS 模式不注入演示种子数据；仅当 GFS 也为内置桩时才注入（纯演示）。

### 登录鉴权：CAS 单点登录（可演示/生产切换）

管理后台与 REST 接口支持 **CAS 2/3 标准协议单点登录**（浏览器跳 CAS 登录页 → 携 ticket 回调 → 管控调 CAS `/p3/proxyValidate` 校验 → 签发本地会话 Cookie `dbam_session`，12h 有效）：

```bash
# 生产：对接真实 CAS
go run ./cmd/server -schedule-demo=false \
  -gfs-url http://gfs-gateway:8200 \
  -public-url    http://dbam:8090 \
  -cas-url       https://cas.example.com/cas

# CAS 2.x 老服务端（校验路径可换）：
go run ./cmd/server -cas-url https://cas.example.com/cas -cas-validate-path /serviceValidate

# 环境变量等价：DBAM_CAS_URL / DBAM_CAS_VALIDATE_PATH
```

- **保护范围**：`/ui/*` 与 `/api/v1/*` 均要求登录；未登录访问页面 302 跳 CAS，REST 调用返回 `401` JSON。
- **机器回调放行**（组件间调用无浏览器会话）：大数据组件回调 `POST /api/v1/archive/result`、`POST /api/v1/replenish/task/status`，以及 `GET /healthz`。
- **登录态端点**：`GET /auth/login`（跳 CAS）、`GET /auth/callback?ticket=`（校验+签发会话）、`GET|POST /auth/logout`（本地注销并联动 CAS 登出）、`GET /auth/user`（当前用户，顶栏展示）。
- **service 回调地址**：优先 `-public-url`，未配置时按请求 `Host`/`X-Forwarded-Proto` 自动推导（反向代理场景传对头即可）。
- **演示模式**：不配 `-cas-url` 时全部放行、不鉴权（`/auth/user` 返回 `demo`），demo.sh 与本地开发不受影响。

### 归档策略配置（窗口 / 留存）

归档策略通过两处独立配置控制，均**真正生效**：

| 配置 | 字段 | 层级 | 生效方式 | 默认 |
|---|---|---|---|---|
| 归档窗口 | `ArchiveWindowDays` | 库级 `DB` → 所属集群 `Cluster` | 一份备份"备份日期距今满该库窗口"才被滚动清单纳入 | 15 天 |
| GFS 留存 | `RetentionDays` | 集群级 `Cluster` | 归档后满该留存天才被清理/判可删；`0`=归档后立即清，`nil`(缺省)=10 | 10 天 |

- 注册集群/绑库时通过 JSON 传 `archiveWindowDays`、`retentionDays`（`retentionDays` 传 `0` 表示归档后立即清，缺省则按集群级 10 天）。
- `GET /backup/archive-status` 的 `canDelete` 与 `pendingDeleteAt` 按「归档时间 + 留存天数」如实计算，供 GFS 清理判定。

## 模拟桩说明

真实环境替换点：

- `sim.GFS`（文件载体 + 清单转发）→ 已挂载的本地 GlusterFS 管理端；管控经 `gfsclient.HTTPFileStore` 打其 HTTP 接口，不直连目录。
- `bigdata`（大数据组件）→ 真实大数据归档系统：由 GFS 转发归档清单触发，自治拉取写 HDFS 后回调管控；管控不感知 HDFS。
- `bigdata` 的 `/v1/replenish/notify`（演示中兼任 DB 代理替身）→ 真实 DB 代理监听接口（部署在 GFS 管理端同址）。
- 管控出站无其他地址：`-component-url` 已移除，一切指向 GFS 管理端。
- 元数据 `SQLite` → 生产 MySQL（DDL 见 `../技术方案/DBArchiveManager元数据DDL.sql`）。

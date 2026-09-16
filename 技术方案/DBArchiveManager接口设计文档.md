# DBArchiveManager 接口设计文档

> 版本：v1.0 ｜ 归属：备份归档管控系统（全局调度中枢）
> 本文档与《数据库备份策略优化方案》配套，定义 DBArchiveManager 对外提供的全部标准接口，
> 并覆盖 MySQL 集群「异地备 / 同城备」关系维度的维护。

---

## 0. 通用约定

### 0.1 接口协议
- 协议：HTTPS
- 交互格式：JSON（UTF-8）
- 鉴权：调用方 Request Header 携带 `Authorization: Bearer <token>`，由管控系统签发并下发各模块。
- 幂等：写操作请求必须携带 `requestId`（UUID），服务端按 `requestId` 去重，保证重试不产生重复效果。

### 0.2 调用方与角色
| 调用方 | 说明 |
|---|---|
| `DB` | 数据库业务系统（上报元数据、接收补发指令） |
| `GFS` | GFS 中转存储系统（巡检、清单、状态查询） |
| `BIGDATA` | 大数据归档系统（传输状态、归档结果上报） |

### 0.3 统一响应结构
```json
{
  "code": 0,              // 0=成功，非0=业务错误码
  "message": "success",
  "requestId": "uuid",
  "data": { }             // 业务数据，可为 null
}
```

### 0.4 通用枚举
**backup_edition 备份版本（体现集群「异地备 / 同城备」关系）**
```json
"DISASTER_RECOVERY"   // 异地备：跨地域灾备副本
"SAME_CITY"           // 同城备：同城冗余副本
```

**archive_status 归档状态**
```json
"NOT_ARCHIVED"   // 未归档
"ARCHIVING"      // 归档中
"ARCHIVED"       // 已归档
"ARCHIVE_FAILED" // 归档失败（可回退重试）
```

**transfer_status 传输状态**
```json
"TRANSFER_PENDING"   // 待传输
"TRANSFERRING"       // 传输中
"TRANSFER_DONE"      // 传输完成
"TRANSFER_FAILED"    // 传输失败
```

**校验结果 upload_verify / archive_verify（文件落地存在性判定）**
```json
"PENDING"     // 待校验
"VERIFIED"    // 校验通过（文件存在 且 size>最小阈值）
"FAILED"      // 校验失败（文件缺失或为空/截断）
```
> 说明：基于 GlusterFS，判定采用「文件存在 + 非空」策略，**不做 MD5**。

**上传/传输失败原因 upload_fail_reason**
```json
"CONNECTION"      // 网络/GFS挂载中断
"TIMEOUT"         // 超时
"EMPTY"           // 文件为空或未达最小阈值(半截)
"NOT_FOUND"       // 目标文件不存在
"STORAGE"         // GFS写入异常
"UNKNOWN"         // 未知
```

**task_status 补发任务状态**
```json
"PENDING"   // 待执行
"RUNNING"   // 执行中
"SUCCESS"   // 成功
"FAILED"    // 失败
```

### 0.5 数据模型维度说明
元数据以 **「MySQL 集群 + 库名 + 备份日期 + 备份版本(异地备/同城备)」** 为唯一业务维度。
集群关系通过 `cluster_id`（集群标识）建模：一个业务库归属一个集群，可同时存在 `DISASTER_RECOVERY` 与 `SAME_CITY` 两套备份版本。
> 详细表结构见《元数据表结构设计 DDL》。接口中的 `dbKey` 形如 `{cluster_id}.{db_name}`。

---

## 1. 集群 / 库关系管理接口（维护「异地备/同城备」关系）

### 1.1 注册 MySQL 集群
`POST /api/v1/cluster`

用于登记一个 MySQL 集群及其拓扑信息（含异地/同城关系）。

请求：
```json
{
  "requestId": "uuid",
  "clusterCode": "CORE_ORDER",          // 集群编码（唯一）
  "clusterName": "核心订单集群",
  "primaryRegion": "bj-a",              // 主地域
  "drRegion": "sh-b",                   // 异地备地域（灾备）
  "cityRegion": "bj-b",                 // 同城备地域（同城冗余）
  "editions": ["DISASTER_RECOVERY", "SAME_CITY"],  // 该集群启用的备份版本
  "retentionDays": 10,                  // 集群级 GFS 留存天数：0=归档后即可清，缺省(nil)=默认10（真正生效，见 6.1）
  "archiveWindowDays": 15,              // 集群级归档窗口（库级优先于集群级，缺省=15）
  "pathTemplate": ""                    // 集群级GFS路径模板(可空, 空→全局默认), 见 1.4
}
```

响应：
```json
{ "code": 0, "data": { "clusterId": 1001 } }
```

### 1.2 绑定库到集群
`POST /api/v1/cluster/{clusterId}/db`

```json
{
  "requestId": "uuid",
  "dbName": "order",
  "editions": ["DISASTER_RECOVERY", "SAME_CITY"],
  "archiveWindowDays": 15,
  "pathTemplate": ""                    // 库级GFS路径模板(可空, 优先于集群级), 见 1.4
}
```

响应：`{ "code": 0, "data": { "dbKey": "CORE_ORDER.order" } }`

### 1.3 查询集群/库拓扑
`GET /api/v1/cluster/{clusterId}`

返回集群详情、库列表、各库启用的备份版本（异地备/同城备）。

### 1.4 GFS 路径模板规则（按集群/库各配各）

备份文件在 GlusterFS 上的存放路径允许**每个库（或集群）单独配置**，以适配不同集群的目录层级、卷位置与文件名差异。

- **占位符**：`{root}` 挂载前缀(默认 `/gfs`) · `{cluster}` 实例IP标识 · `{db}` 库名 · `{date}` 备份日期(YYYY-MM-DD) · `{vol}` 双卷标识。
- **双卷标识**：`{vol}` 替换为 `city`(同城备卷) / `dr`(异地备卷)；同一模板生成主从两卷路径，仅 `{vol}` 段不同，其余（如文件名）保持一致。
- **默认模板**：`{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz`。
- **解析优先级（库级优先，集群/全局回退）**：`库.pathTemplate` → `集群.pathTemplate` → 全局默认。
- **反向解析**：给定一条路径 + 模板，可反解析出 `cluster / db / date / vol`（要求每个占位符独占一个路径段），供"只给路径自动归属"使用。

示例（库级模板 `{root}/prod/{cluster}/{db}/{date}/{vol}/orders.sql.gz`）：
```
主卷(同城备): /gfs/prod/10.1.2.3/orders/2026-07-21/city/orders.sql.gz
从卷(异地备): /gfs/prod/10.1.2.3/orders/2026-07-21/dr/orders.sql.gz
```

---

## 2. 备份元数据上报接口（调用方：DB）

### 2.1 上报备份元数据
`POST /api/v1/backup/meta/report`

数据库备份程序在备份文件**成功上传 GFS 后**调用，同步元数据入库。

请求：
```json
{
  "requestId": "uuid",
  "hostIp": "10.1.2.3",             // 来源实例IP，系统据此反查归属集群(见 tb_cluster_instance)
  "dbName": "order",
  "backupDate": "2026-08-05",          // 备份数据日期
  "backupEdition": "DISASTER_RECOVERY", // 异地备
  "gfsPath": "/gfs/10.1.2.3/order/20260805/dr/full.sql.gz",
  "fileSizeBytes": 2684354560,
  "backupStartTime": "2026-08-05T00:10:00+08:00",
  "backupEndTime": "2026-08-05T01:22:00+08:00"
}
```

> **集群归属判定**：备份文件路径以**实例 IP** 标识（`/gfs/<ip>/<db>/<date>/{city|dr}/full.sql.gz`），而非 `cluster_code`。
> 上报时以 `hostIp` 调用，系统经 `tb_cluster_instance`（一个 IP 只属于一个集群，一集群可多 IP）**反查归属集群**，再按 `(cluster, dbName)` 绑定库；未关联任何集群的 IP 上报将返回明确错误。


> **上传判定（GlusterFS / FUSE 挂载本地目录）**：数据库端将同一份备份**同时写入「同城卷」与「异地卷」两个路径**（见 `backupEdition`），各自完成后对目标文件执行 `stat` 校验；
> 判定成功条件 = **文件存在且 size>0（建议最小阈值 10MB 防半截）**。两个卷都判定成功后，才上报元数据。
> **不做 MD5**（GlusterFS FUSE 目录无服务端验签，且业务明确不需要）；文件大小 `fileSizeBytes` 由 `stat` 实测回填。

响应：`{ "code": 0, "data": { "backupId": 500001, "archiveStatus": "NOT_ARCHIVED" } }`

---

## 3. 巡检接口（管理端 / 调度内部）

### 3.1 触发完整性巡检（调度触发，可手动）
`POST /api/v1/inspect/run`

```json
{ "requestId": "uuid", "targetDate": "2026-08-05", "scope": "ALL" }
```
`scope`：`ALL`（全集群）｜ `{clusterId}`｜ `{dbKey}`。

响应：
```json
{
  "code": 0,
  "data": {
    "inspectId": 9001,
    "summary": { "totalSlots": 120, "missing": 3, "broken": 1, "ok": 116 },
    "missingList": [
      { "clusterCode": "CORE_ORDER", "dbName": "order",
        "backupDate": "2026-08-04", "backupEdition": "DISASTER_RECOVERY" }
    ]
  }
}
```

### 3.2 查询巡检结果
`GET /api/v1/inspect/{inspectId}`

返回巡检详情：按「集群+库+日期+版本」逐 slot 列出文件存在性、size、日期连续性校验结果。

> 巡检窗口默认 30 天（≥ 归档窗口+缓冲），避免窗口内尚未归档的文件被误判缺失。

---

## 4. 备份补发接口（调用方：DB）

### 4.1 下发补发指令
`POST /api/v1/replenish/task`

由巡检发现缺失后自动下发，也支持手动补发。管控记录本地任务后，经 **HTTP** 把补发指令推送给**数据库业务系统/DB 代理**（监听 `POST /v1/replenish/notify`），由其在 GFS 重跑备份重写缺失卷，完成后回调（见 4.2）。**管控不直连大数据**。
> **执行方地址**：补发监听同在 **GFS 管理端**（`-gfs-url` / `-gfs-map` / 管理后台「机房管理」配置），由管理端转交 DB 代理/业务系统执行；多机房时按缺失卷所在机房分发到对应管理端；未配置且非演示模式时补发指令无法投递。

请求：
```json
{
  "requestId": "uuid",
  "inspectId": 9001,
  "items": [
    { "clusterCode": "CORE_ORDER", "dbName": "order",
      "backupDate": "2026-08-04", "backupEdition": "DISASTER_RECOVERY" }
  ]
}
```

响应：
```json
{ "code": 0, "data": { "taskId": "REP202608050001", "status": "PENDING" } }
```

### 4.2 补发任务状态推送（DB → 管控）
`POST /api/v1/replenish/task/status`

数据库完成补发（同时写入同城/异地双卷并通过存在校验）后回调：
```json
{
  "requestId": "uuid",
  "taskId": "REP202608050001",
  "status": "SUCCESS",
  "gfsPath": "/gfs-dr/CORE_ORDER/order/20260804/dr/full.sql.gz",
  "fileSizeBytes": 2147483648
}
```
管控收到 `SUCCESS` 后：更新元数据（补发日期、路径、size），并自动纳入滚动归档清单。

### 4.3 防抖与幂等
- 同一「簇+库+日期+版本」在 `task_status != SUCCESS` 期间不重复下发。
- 补发成功后 24h 探活窗口，失败自动重试（默认 3 次）。

---

## 5. 滚动归档清单接口（调用方：GFS）

### 5.1 生成并下发归档清单
`POST /api/v1/archive/rolling-list/generate`

管控按**每个库自身的归档窗口**（库级 `ArchiveWindowDays` → 所属集群 → 默认 15 天，已过窗口才纳入）+ 元数据去重，生成待归档清单。管控是调度枢纽，**不读卷写盘、不直连大数据**：清单生成后经 **HTTP 推送给 GFS 管理端**（`POST /v1/archive/notify`），由 **GFS 转发归档通知**给大数据；大数据**自治**从 GFS 拉取 → 写 HDFS → 回调 `/api/v1/archive/result`（见 7.2）。接口同时返回清单，供调用方立即使用。窗口不再由调用方统一传入，由各库/集群配置决定。

请求（管控 → GFS 推送，或 GFS 拉取）：
```json
{
  "requestId": "uuid",
  "cutoffDate": "2026-08-05"
}
```

响应（GFS 侧接收到的清单数据结构）：
```json
{
  "code": 0,
  "data": {
    "archiveBatchId": "ARC20260805001",
    "items": [
      { "archiveItemId": 70001,
        "clusterCode": "CORE_ORDER", "dbName": "order",
        "backupDate": "2026-07-25", "backupEdition": "DISASTER_RECOVERY",
        "gfsPath": "/gfs-dr/CORE_ORDER/order/20260725/dr/full.sql.gz",
        "fileSizeBytes": 2684354560 }
    ]
  }
}
```

去重规则：仅纳入 `archive_status = NOT_ARCHIVED` 且已过**该库**归档窗口的备份；`ARCHIVING/ARCHIVED` 一律排除。

### 5.2 查询指定库的待归档清单
`GET /api/v1/archive/rolling-list?dbKey=CORE_ORDER.order&cutoffDate=2026-08-05`

---

## 6. 归档状态查询接口（调用方：GFS / 大数据）

### 6.1 查询备份归档状态（GFS 清理前置校验）
`GET /api/v1/backup/archive-status`

GFS 每日清理前调用，判断中转文件是否满足删除条件（清理触发方由部署配置 `-cleanup-mode` 决定：`mgr`=管控经 HTTP `DELETE /v1/fs/delete` 触发删除；`gfs`=管控仅此作为放行判定、GFS 自治删除）。

请求：`?clusterCode=CORE_ORDER&dbName=order&backupDate=2026-07-25&backupEdition=DISASTER_RECOVERY`

响应：
```json
{
  "code": 0,
  "data": {
    "backupId": 500012,
    "archiveStatus": "ARCHIVED",
    "archivedAt": "2026-07-26T05:40:00+08:00",
    "canDelete": true,               // 已归档 且 距归档>=该集群retentionDays（0=归档后即可清，nil=默认10）
    "pendingDeleteAt": "2026-08-05"  // = archivedAt + retentionDays
  }
}
```

### 6.2 批量状态查询（性能优化）
`POST /api/v1/backup/archive-status/batch`

支持一次查询最多 1000 条，供 GFS 清理任务批量取数。

---

## 7. 传输状态 / 归档结果接口（调用方：GFS ↔ 大数据）

### 7.1 查询大数据拉取传输状态
`GET /api/v1/transfer/status?archiveBatchId=ARC20260805001&archiveItemId=70001`

返回 `transfer_status`（待传输/传输中/完成/失败）。

### 7.2 归档结果上报（大数据 → 管控，自治拉取后回调）
`POST /api/v1/archive/result`

大数据组件收到归档清单通知（`/v1/archive/notify`）后，**自治**从 GFS 拉取源卷 → 写 HDFS → 完成【存在 + size 判定】后回调本接口：
```json
{
  "requestId": "uuid",
  "archiveItemId": 70001,
  "transferStatus": "TRANSFER_DONE",
  "hdfsPath": "/archive/CORE_ORDER/order/20260725/dr/full.sql.gz",
  "hdfsSizeBytes": 2684354560,         // 大数据侧 stat 实测(写HDFS后), 与清单记录一致才算成功
  "archivedAt": "2026-07-26T05:40:00+08:00"
}
```

成功条件：`transferStatus=DONE` 且 `hdfsSizeBytes` 与清单内 `fileSizeBytes` 一致（或以模块内文件存在+size>阈值为准）。满足后管控将元数据置为 `ARCHIVED`，并触发 GFS 清理放行（见 6.1）。
> 管控本身**不读 GFS 卷、不写 HDFS**：文件读写与 HDFS 落盘全部发生在大数据组件内部。

---

## 7.5 组件间 HTTP 契约（管控 ↔ GFS / 大数据）

管控是调度枢纽，**不直连文件系统**，全部经 HTTP 与两个独立组件交互：

### ① GFS 组件（文件载体）
| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/v1/fs/stat?path=...` | 返回 `{"exists":bool,"size":int}`；巡检、上报双卷复核、stat 回显用 |
| DELETE | `/v1/fs/delete` body `{"path":...}` | 清理：删主卷/异地卷 |

> 写入（初始备份 / 补发写卷）由 DB 直接写 GFS 挂载目录（FUSE），**不归管控**。

### ①-补 GFS 管理端额外职责（清单转发）
| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/v1/archive/notify` body `{archiveBatchId, items[]}` | 接收管控归档清单并**转发大数据**；管控不直连大数据 |

### ② 大数据组件（归档目标，被动触发）
| 方法 | 路径 | 用途 |
|---|---|---|
| （无管控直连接口） | — | 收到 GFS 转发的归档通知后**自治**从 GFS 拉取 → 写 HDFS → 回调 `/api/v1/archive/result` |

### ③ DB 代理/业务系统（补发执行）
| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/v1/replenish/notify` body `{taskId, cluster, db, date, edition, path, sizeBytes}` | 接收管控补发指令；DB 侧重跑备份在 GFS 重写缺失卷 → 回调 `/api/v1/replenish/task/status`(SUCCESS) |

大数据**不是被动等管控给文件**：收到 GFS 转发的归档通知后自行经 GFS 拉取并落 HDFS，完成后再回调管控。补发由 DB 代理/业务系统在 GFS 重写缺失卷后回调——管控对大数据零直连，只被动接收其归档结果回调，不参与具体文件搬运。

---

## 8. 全局对账 / 管理接口

### 8.1 元数据对账查询
`GET /api/v1/meta/reconciliation?dateFrom=2026-08-01&dateTo=2026-08-05`

返回各（集群,库,版本）的：应备数、实备数、归档数、补发数、缺失数，用于定时对账与故障溯源。

### 8.2 健康检查
`GET /healthz` → `200 {"status":"UP"}`

### 8.3 定时调度状态查询
`GET /api/v1/schedule/status`

返回已注册的定时作业及其规格、最近运行时间、结果计数，用于核对调度是否自动在跑。

> **定时调度**：服务内嵌 cron 自动触发两类日常作业——
> - **完整性巡检 + 缺失自动下发补发**（生产默认 `0 0 3 * * *`）；
> - **滚动归档清单生成**（生产默认 `0 0 2 * * *`）。
>
> 演示模式（默认 `-schedule-demo`）降为秒级（`@every 15s` / `@every 30s`）便于观察；可用 `-schedule-spec` / `-rolling-spec` 显式覆盖 cron 规格，`-window-days` 控制巡检/滚窗窗口天数。

---

## 9. 文件上传完整性判定方案（GlusterFS）

> **背景**：GFS 为 **GlusterFS**，数据库端以 **FUSE 挂载为本地目录**。备份文件即普通本地路径写入。
> **判定口径**：**不校验 MD5**，采用「**文件存在 + 非空（size>最小阈值）**」作为上传/传输成功判定。
> **主备**：异地备、同城备为**两个独立 GlusterFS 卷**，数据库端把同一份备份**同时写入同城卷与异地卷两个路径**，各自通过存在校验后才上报元数据。

### 9.1 两条链路与双卷写入

```
链路A(DB→GFS):  数据库 ──写──> 同城卷 /gfs-city/...   +  异地卷 /gfs-dr/...
                判定: 两个路径都 stat 存在且 size>阈值  →  才上报元数据
链路B(GFS→大数据): 大数据 ──读──> 同城卷(或清单指定卷) ──拉取──> HDFS
                判定: 读取成功、目标文件存在且 size 合理 → 上报归档成功
```

| 链路 | 谁判定 | 判据 |
|---|---|---|
| A：DB→同城/异地两卷 | DB 侧（写后自检） | 两路径 `stat` 均存在 且 `size>MIN_FILE_SIZE`（建议 10MB） |
| B：GFS→大数据 | 大数据侧 | 读取完成、写 HDFS 后文件存在且 size 合理 |

> **不做 MD5 的原因**：GlusterFS FUSE 挂载目录不具备对象存储的服务端 `Content-MD5` 验签能力；
> 且业务确认对全量备份（数 GB）逐文件读算 MD5 的重 IO 开销不可接受，判定以「存在 + 非空」即可。

### 9.2 判定流程（DB 写同城 + 异地两卷）

```
数据库备份程序生成全量备份（本地临时文件）
   │
   ▼
同时写入同城卷路径 /gfs-city/<cluster>/<db>/<date>/<edition>/full.sql.gz
    与 异地卷路径 /gfs-dr/<cluster>/<db>/<date>/<edition>/full.sql.gz
   │
   ▼
两侧各自落盘后执行 stat 校验
   │  size <= MIN_FILE_SIZE(10MB) 或 文件不存在
   ├──▶ 判 EMPTY / NOT_FOUND → 删除残file，重写该卷
   │  两侧均 size > 阈值
   ▼
两侧校验均通过 → DB 调用 POST /api/v1/backup/meta/report
                    上报 gfsPath(主卷路径) + fileSizeBytes(stat实测)
                    archive_status = NOT_ARCHIVED
```

**判定要点**：
- **DB 必须两个卷都校验通过后才上报元数据**；任一卷失败 → 不进入 NOT_ARCHIVED，触发重写或补发。
- `NOT_ARCHIVED` 记录 ⇒ GFS 双卷文件都已存在且非空，这是下游巡检/归档/清理的**可信前提**。
- GlusterFS 是**挂载目录**，无服务端回执；"写入成功"由 DB 侧 `stat` 实测确认，避免异步/半截写。

### 9.3 链路B：GFS→大数据 传输判定流程

```
GFS 收到归档清单 → 大数据按清单内路径读取对应卷文件
   │
   ▼
大数据读取完成 → 写入 HDFS 目标路径
   │
   ▼
大数据对 HDFS 副本 stat 校验（存在 + size==清单记录 size）
   │  读取失败或 size 不一致
   ├──▶ 上报 TRANSFER_FAILED → 重试(≤3次)
   │  校验通过
   ▼
大数据上报 POST /api/v1/archive/result (TRANSFER_DONE + hdfsPath + hdfsSize)
   │
   ▼
DBArchiveManager 判定成功 → archive_status 置 ARCHIVED → 放行 GFS 清理
```

### 9.4 失败处理与告警策略

| 失败原因 | 处理策略 | 告警级别 |
|---|---|---|
| CONNECTION / TIMEOUT | 自动重写/重拉，指数退避，上限 3 次 | 重试中不告警；超上限 P1 |
| EMPTY / NOT_FOUND（半截/缺失） | 删除残file，重写该卷（重试无意义） | 🔴 P0 数据完整性 |
| STORAGE（GlusterFS 卷异常） | 告警人工介入，标记该备份为 BROKEN | 🔴 P0 |
| REPLENISH 补发失败 | 重试 ≤3 次，失败转人工，参与下轮巡检 | 🟠 P1 |

**告警分级**：
- 🔴 **P0（数据完整性）**：EMPTY / NOT_FOUND / STORAGE 异常 → 立即告警，阻断该文件后续归档。
- 🟠 **P1（重试超限）**：上传/补发超过重试上限 → 告警转人工。
- 🟡 **P2（进度/性能）**：传输耗时超阈值 → 记录并告警，定位 GFS/HDFS IO 瓶颈。

### 9.5 GFS 文件读写方式（GlusterFS / FUSE 挂载目录）

**9.5.1 写入（DB→GFS）**
数据库程序直接向挂载目录写入普通文件（`cp` / 流式写文件），无自定义上传接口：
```
挂载点: /gfs-city  → 同城卷    /gfs-dr  → 异地卷
写入:   db 程序写  /gfs-city/<cluster>/<db>/<date>/<edition>/full.sql.gz
                + /gfs-dr/<cluster>/<db>/<date>/<edition>/full.sql.gz
校验:   写后 fstat 目标文件, 确认 exists 且 size > MIN_FILE_SIZE
```

**9.5.2 巡检判定（DBArchiveManager 每日遍历）**
管控系统同样以 FUSE 挂载方式访问 GFS，遍历时对每个应备路径执行 `stat`：
- 文件存在且 `size > MIN_FILE_SIZE` → 计为正常；
- 文件缺失 → `MISSING`（触发补发）；
- 文件存在但 `size <= 阈值` 或为空 → `BROKEN/EMPTY`（触发重写或补发）。

**9.5.3 目录结构约定**
```
/gfs-{city|dr}/<cluster_code>/<db_name>/<YYYYMMDD>/<edition>/full.sql.gz
<!-- edition: dr=异地备, city=同城备 -->
```
> 巡检按此目录结构遍历，即可定位每个「集群+库+日期+版本」的应备文件。

---

## 10. 接口清单汇总表

| # | 方法 | 路径 | 调用方 | 用途 |
|---|---|---|---|---|
| 1 | POST | /api/v1/cluster | 管理端 | 注册集群(异地/同城拓扑) |
| 2 | POST | /api/v1/cluster/{id}/db | 管理端 | 绑库与备份版本 |
| 3 | GET | /api/v1/cluster/{id} | 管理端 | 查询集群拓扑 |
| 3a | POST | /api/v1/cluster/{id}/instance | 管理端 | 给集群挂实例IP(按IP反查集群) |
| 3b | GET | /api/v1/cluster/{id}/instances | 管理端 | 查询集群实例IP列表 |
| 4 | POST | /api/v1/backup/meta/report | DB | 上报备份元数据(按hostIp反查集群) |
| 5 | POST | /api/v1/inspect/run | 调度/管理 | 触发完整性巡检 |
| 6 | GET | /api/v1/inspect/{id} | 管理端 | 查询巡检结果 |
| 7 | POST | /api/v1/replenish/task | 调度 | 下发补发指令 |
| 8 | POST | /api/v1/replenish/task/status | DB | 补发状态回调 |
| 9 | POST | /api/v1/archive/rolling-list/generate | GFS | 生成/下发归档清单 |
| 10 | GET | /api/v1/archive/rolling-list | GFS | 查询待归档清单 |
| 11 | GET | /api/v1/backup/archive-status | GFS | 归档状态(清理判断) |
| 12 | POST | /api/v1/backup/archive-status/batch | GFS | 批量状态查询 |
| 13 | GET | /api/v1/transfer/status | GFS | 拉取传输状态 |
| 14 | POST | /api/v1/archive/result | 大数据 | 归档结果上报（大数据自治拉取后回调，存在+size判定） |
| 15 | GET | /api/v1/meta/reconciliation | 管理端 | 对账查询 |
| 16 | GET | /healthz | 监控 | 健康检查 |
| 17 | — | — | DB | GFS 为 FUSE 挂载目录，无上传接口；DB 直接写文件 + stat 自检 |
| 18 | GET | /gfs/file/stat | 巡检 | 巡检遍历时 stat 文件(存在+size)判定 |
| 19 | GET | /api/v1/schedule/status | 管理端 | 定时调度状态(各任务 cron 规格 + 最近运行摘要) |
| 20 | GET | /v1/fs/stat | 管控→GFS | 组件对外：stat 文件(存在+size) |
| 21 | DELETE | /v1/fs/delete | 管控→GFS | 组件对外：删除文件 |
| 22 | POST | /v1/archive/notify | 管控→大数据 | 组件对外：下发归档清单，大数据自治拉取 |
| 23 | POST | /v1/replenish/notify | 管控→大数据 | 组件对外：下发补发指令，大数据重写缺失卷 |

const fs = require('fs');
const { Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType,
        Table, TableRow, TableCell, WidthType, ShadingType, BorderStyle,
        PageBreak, TableOfContents, LevelFormat, VerticalAlign } =
  require('/Users/gracegaoya/.npm-global/lib/node_modules/docx');

// ---------- helpers ----------
const FONT = 'Microsoft YaHei';
const FONT_EN = 'Consolas';

function txt(content, opts = {}) {
  return new TextRun({
    text: content,
    font: FONT,
    size: opts.size || 21, // half-points -> 10.5pt
    bold: opts.bold || false,
    boldItalic: opts.boldItalic || false,
    italics: opts.italics || false,
    color: opts.color,
    children: opts.children,
    break: opts.break,
  });
}

function mono(content, opts = {}) {
  return new TextRun({
    text: content,
    font: FONT_EN,
    size: opts.size || 18, // 9pt for code
    bold: opts.bold || false,
    color: opts.color,
  });
}

function p(content, opts = {}) {
  return new Paragraph({
    heading: opts.heading,
    children: (Array.isArray(content) ? content : [txt(content)]),
    spacing: { after: opts.after || 120, before: opts.before || 0, line: opts.line || 276 },
    alignment: opts.align,
    indent: opts.indent,
    bullet: opts.bullet ? { level: opts.level || 0 } : undefined,
    numbering: opts.numbering,
    keepNext: opts.keepNext,
  });
}

function h1(text) { return new Paragraph({ heading: HeadingLevel.HEADING_1, spacing:{before:240,after:120}, children:[txt(text,{size:32,bold:true})] }); }
function h2(text) { return new Paragraph({ heading: HeadingLevel.HEADING_2, spacing:{before:200,after:100}, children:[txt(text,{size:26,bold:true})] }); }
function h3(text) { return new Paragraph({ heading: HeadingLevel.HEADING_3, spacing:{before:160,after:80}, children:[txt(text,{size:22,bold:true})] }); }

function codeBlock(lines) {
  const arr = Array.isArray(lines) ? lines : lines.split('\n');
  return new Paragraph({
    children: arr.map((l, i) => mono(l + (i < arr.length - 1 ? '\n' : ''))),
    spacing: { after: 60 },
    shading: { type: ShadingType.CLEAR, fill: 'F2F2F2' },
    indent: { left: 240 },
  });
}

// bordered table for diagrams
function diagramTable(rows) {
  return new Table({
    columnWidths: [9550],
    width: { size: 9550, type: WidthType.DXA },
    rows: rows.map(cells => new TableRow({
      children: cells.map(cell => new TableCell({
        width: { size: 9550, type: WidthType.DXA },
        shading: { type: ShadingType.CLEAR, fill: cell.fill || 'FFFFFF' },
        verticalAlign: VerticalAlign.CENTER,
        borders: {
          top: {style:BorderStyle.NONE}, bottom:{style:BorderStyle.NONE},
          left: {style:BorderStyle.NONE}, right:{style:BorderStyle.NONE},
        },
        children: cell.text.split('\n').map(l => new Paragraph({
          alignment: cell.center ? AlignmentType.CENTER : AlignmentType.LEFT,
          spacing: { after: 0, line: 240 },
          children: [mono(l, { size: cell.size || 16 })],
        })),
      })),
    })),
  });
}

// ---------- content ----------
const children = [];
const C = children;

// ===== Title page =====
C.push(new Paragraph({ spacing:{before:2000}, alignment: AlignmentType.CENTER, children:[txt('数据库备份策略优化方案',{size:48,bold:true})]}));
C.push(new Paragraph({ alignment: AlignmentType.CENTER, spacing:{before:200}, children:[txt('—— DBArchiveManager 备份归档管控系统 技术方案 ——',{size:24})]}));
C.push(new Paragraph({ alignment: AlignmentType.CENTER, spacing:{before:3000}, children:[txt('版本：v1.0',{size:21})]}));
C.push(new Paragraph({ alignment: AlignmentType.CENTER, children:[txt('日期：2026-08-05',{size:21})]}));
C.push(new Paragraph({ alignment: AlignmentType.CENTER, children:[txt('归属：备份归档管控系统（全局调度中枢）',{size:21})]}));
C.push(new Paragraph({ children:[new PageBreak()]}));

// ===== TOC =====
C.push(h1('目录'));
C.push(new TableOfContents('TOC', { hyperlink: true, headingStyleRange: '1-3' }));
C.push(new Paragraph({ children:[new PageBreak()]}));

// ===== 1 概述 =====
C.push(h1('第一章  方案概述'));
C.push(p('本文档作为《数据库备份策略优化方案》的技术落地配套，定义新增调度中枢 DBArchiveManager 的接口规范、系统时序、归档状态机与元数据表结构，并纳入 MySQL 集群「异地备 / 同城备」关系维度（backup_edition）的统一维护。'));
C.push(h2('1.1 方案目标'));
C.push(p('通过「库级可配的滚动归档窗口（默认 15 天, 库级→集群级回退）」+ 库级元数据去重 + 每日巡检补发，实现：每份备份仅向 HDFS 归档一次；缺失备份自动补全；GFS 清理前按「已归档 + 满该集群留存天数」强制校验，杜绝误删。',{bullet:true}));
C.push(p('备份元数据以「MySQL集群 + 库名 + 备份日期 + 备份版本(异地备/同城备)」为唯一业务维度建模，两个版本独立上报、巡检、归档、清理。',{bullet:true}));

// ===== 2 总体架构 =====
C.push(h1('第二章  总体架构'));
C.push(h2('2.1 四大模块'));
C.push(diagramTable([
  [{ text:'+----------------------+     实线:文件物理传输     +----------------------+', center:true }],
  [{ text:'|   数据库业务系统      |-------------------------->|   GFS 中转存储系统     |', center:true }],
  [{ text:'|   (存量,小改)         |                           |   (存量,小改)          |', center:true }],
  [{ text:'+----------+-----------+                           +----------+-----------+', center:true }],
  [{ text:'    ① 元数据上报  ②补发指令                         ③ 归档清单下发            |', center:true }],
  [{ text:'+----------+-----------+                           |          +-----------+', center:true }],
  [{ text:'|   DBArchiveManager  |<---------------------------+   大数据归档系统       |', center:true }],
  [{ text:'|   (新增调度中枢)     |  ⑤ 巡检/查状态              |   (存量,零改动)       |', center:true }],
  [{ text:'+----------------------+                           +----------------------+', center:true }],
]));
C.push(h2('2.2 模块职责'));
C.push(p('DBArchiveManager：新增独立调度服务，不传输文件，只做调度、校验、元数据、指令下发。管控**不直连文件系统**，巡检 stat、上报双卷复核、清理删卷、归档结果全部经 **HTTP** 与 GFS / 大数据两个独立组件交互。',{bullet:true}));
C.push(p('数据库业务系统：保留每日分库全量备份 + 直接写 GFS 双卷（FUSE 挂载目录）+ stat 自检；新增元数据上报（/api/v1/backup/meta/report，按 hostIp 反查集群）与补发回调（存量改造点，需确认）。',{bullet:true}));
C.push(p('GFS 组件（文件载体）：独立 HTTP server，向外暴露 /v1/fs/stat（存在+size）与 /v1/fs/delete；写入由 DB 直写挂载目录，不归管控。',{bullet:true}));
C.push(p('大数据组件（归档目标 / 补发执行）：独立 HTTP server，接收 /v1/archive/notify 后**自治**从 GFS 拉取→写 HDFS→回调 /api/v1/archive/result；接收 /v1/replenish/notify 后在 GFS 重写缺失卷→回调补发状态。',{bullet:true}));

// ===== 3 接口设计 =====
C.push(h1('第三章  接口设计（DBArchiveManager）'));
C.push(p('协议 HTTPS + JSON，Bearer 鉴权，写操作携带 requestId 幂等。统一响应：{code, message, requestId, data}。'));
C.push(p('备份版本枚举 backup_edition：DISASTER_RECOVERY=异地备 / SAME_CITY=同城备。',{bullet:true}));
C.push(p('归档状态 archive_status：NOT_ARCHIVED / ARCHIVING / ARCHIVED / ARCHIVE_FAILED。',{bullet:true}));
C.push(p('传输状态 transfer_status：PENDING / TRANSFERRING / DONE / FAILED。',{bullet:true}));

// 接口清单表
const apiRows = [
  ['#','方法','路径','调用方','用途'],
  ['1','POST','/api/v1/cluster','管理端','注册集群(异地/同城拓扑)'],
  ['2','POST','/api/v1/cluster/{id}/db','管理端','绑库与备份版本'],
  ['3','GET','/api/v1/cluster/{id}','GFS/DB','查询集群拓扑'],
  ['4','POST','/api/v1/backup/meta/report','DB','上报备份元数据'],
  ['5','POST','/api/v1/inspect/run','调度','触发完整性巡检'],
  ['6','GET','/api/v1/inspect/{id}','管理端','查询巡检结果'],
  ['7','POST','/api/v1/replenish/task','调度','下发补发指令'],
  ['8','POST','/api/v1/replenish/task/status','DB','补发状态回调'],
  ['9','POST','/api/v1/archive/rolling-list/generate','GFS','生成/下发归档清单'],
  ['10','GET','/api/v1/archive/rolling-list','GFS','查询待归档清单'],
  ['11','GET','/api/v1/backup/archive-status','GFS','归档状态(清理判断)'],
  ['12','POST','/api/v1/backup/archive-status/batch','GFS','批量状态查询'],
  ['13','GET','/api/v1/transfer/status','GFS','拉取传输状态'],
  ['14','POST','/api/v1/archive/result','大数据','归档结果上报(大数据自治拉取后回调, 存在+size判定)'],
  ['15','GET','/api/v1/meta/reconciliation','管理端','对账查询'],
  ['16','GET','/healthz','监控','健康检查'],
  ['20','GET','/v1/fs/stat','管控→GFS','组件对外: stat文件(存在+size)'],
  ['21','DELETE','/v1/fs/delete','管控→GFS','组件对外: 删除文件'],
  ['22','POST','/v1/archive/notify','管控→大数据','组件对外: 下发归档清单, 大数据自治拉取'],
  ['23','POST','/v1/replenish/notify','管控→大数据','组件对外: 下发补发指令, 大数据重写缺失卷'],
];
C.push(new Table({
  columnWidths: [600,1000,3200,1300,3450],
  width:{size:9550,type:WidthType.DXA},
  rows: apiRows.map((r,ri)=>new TableRow({
    children:r.map((c,ci)=>new TableCell({
      width:{size:[600,1000,3200,1300,3450][ci],type:WidthType.DXA},
      shading: ri===0 ? {type:ShadingType.CLEAR,fill:'D9E2F3'} : undefined,
      children:[new Paragraph({spacing:{after:40},children:[txt(c,{size:16,bold:ri===0})]})],
    })),
  })),
}));

C.push(h2('3.1 关键接口示例'));
C.push(h3('备份元数据上报  POST /api/v1/backup/meta/report（DB→MGR）'));
C.push(codeBlock([
  '{',
  '  "requestId": "uuid",',
  '  "clusterCode": "CORE_ORDER",',
  '  "dbName": "order",',
  '  "backupDate": "2026-08-05",',
  '  "backupEdition": "DISASTER_RECOVERY",   // 异地备',
  '  "gfsPath": "/gfs-dr/CORE_ORDER/order/20260805/dr/full.sql.gz",',
  '  "peerGfsPath": "/gfs-city/CORE_ORDER/order/20260805/city/full.sql.gz",  // 另一卷',
  '  "fileSizeBytes": 2684354560',  // stat 实测
  '}',
]));
C.push(p('幂等键：clusterCode+dbName+backupDate+backupEdition，重复上报自动覆盖更新。'));
C.push(h3('归档状态查询  GET /api/v1/backup/archive-status（GFS 清理前置校验）'));
C.push(codeBlock([
  '// 请求 ?clusterCode=CORE_ORDER&dbName=order&backupDate=2026-07-25&backupEdition=DR',
  '{ "code":0, "data": {',
  '    "archiveStatus":"ARCHIVED", "archivedAt":"2026-07-26T05:40:00+08:00",',
  '    "canDelete":true, "pendingDeleteAt":"2026-08-05" } }',
  '// canDelete=已归档且距归档>=该集群retentionDays(0=即可清,缺省=10); pendingDeleteAt=archivedAt+retention',
]));
C.push(h3('下发补发指令  POST /api/v1/replenish/task（MGR 记录，经 HTTP 推大数据执行）'));
C.push(codeBlock([
  '{ "requestId":"uuid", "inspectId":9001,',
  '  "items":[ { "clusterCode":"CORE_ORDER","dbName":"order",',
  '              "backupDate":"2026-08-04","backupEdition":"DISASTER_RECOVERY" } ] }',
  '=> { "code":0, "data":{ "taskId":"REP202608050001","status":"PENDING" } }',
  '管控记录本地任务后经 [HTTP] POST /v1/replenish/notify 推大数据; 大数据重写缺失卷后回调 SUCCESS',
]));
C.push(h3('滚动归档清单下发  POST /api/v1/archive/rolling-list/generate（MGR，经 HTTP 通知大数据）'));
C.push(codeBlock([
  '{ "requestId":"uuid", "cutoffDate":"2026-08-05" }',
  '=> { "code":0, "data":{ "archiveBatchId":"ARC20260805001",',
  '      "items":[ { "archiveItemId":70001, "clusterCode":"CORE_ORDER",',
  '        "dbName":"order","backupDate":"2026-07-25","backupEdition":"DR",',
  '        "gfsPath":"/gfs-dr/.../full.sql.gz","fileSizeBytes":2684354560 } ] } }',
  '管控下发后经 [HTTP] POST /v1/archive/notify 通知大数据; 大数据自治拉GFS→写HDFS',
]));
C.push(p('去重规则：仅纳入 archive_status=NOT_ARCHIVED 且已过**该库**归档窗口的备份；ARCHIVING/ARCHIVED 一律排除。'));
C.push(h3('归档结果上报  POST /api/v1/archive/result（大数据→MGR，存在+size判定）'));
C.push(codeBlock([
  '{ "requestId":"uuid","archiveItemId":70001,"transferStatus":"TRANSFER_DONE",',
  '  "hdfsPath":"/archive/CORE_ORDER/order/20260725/dr/full.sql.gz",',
  '  "hdfsSizeBytes":2684354560,"archivedAt":"2026-07-26T05:40:00+08:00" }',
  '// 成功条件: transferStatus=DONE 且 hdfsSizeBytes 与清单记录一致(存在+size, 不做MD5)',
]));

// ===== 4 时序图 =====
C.push(h1('第四章  完整业务流转时序图'));
C.push(p('六个阶段：①备份生成上报 → ②每日巡检补发 → ③滚动归档清单 → ④大数据自治拉取 → ⑤归档结果上报(存在+size判定) → ⑥GFS自动清理。管控是调度中枢，与 GFS / 大数据全部经 HTTP 交互（不直连文件系统）。'));
C.push(diagramTable([
  [{ text:'[阶段1] 备份生成与元数据上报', fill:'D9E2F3', center:true }],
  [{ text:'DB --双卷直接写--> GFS(同城+异地两卷) : 分库全量备份 + stat自检', center:true }],
  [{ text:'DB --上报--> MGR : 备份元数据(库/日期/版本/双卷路径/size), 按hostIp反查集群', center:true }],
  [{ text:'MGR --[HTTP]sts双卷复核--> GFS ; MGR --返回--> DB : backupId, status=NOT_ARCHIVED', center:true }],
  [{ text:'[阶段2] 每日完整性巡检与补发', fill:'D9E2F3', center:true }],
  [{ text:'MGR --[HTTP]stat遍历--> GFS : 所有集群/库近窗口备份 (存在+size)', center:true }],
  [{ text:'若缺失: MGR --[HTTP]replenish/notify--> 大数据 ; 大数据重写缺失卷于 GFS', center:true }],
  [{ text:'      大数据 --回调SUCCESS--> MGR ; 重试防抖(同一维度未完成不重复下发)', center:true }],
  [{ text:'[阶段3] 滚动归档清单 (库级窗口)', fill:'D9E2F3', center:true }],
  [{ text:'MGR --过滤已归档--> 生成无重复待归档清单 (NOT_ARCHIVED且已过该库窗口)', center:true }],
  [{ text:'[阶段4] 大数据自治拉取', fill:'D9E2F3', center:true }],
  [{ text:'MGR --[HTTP]archive/notify 下发清单--> 大数据 ; 大数据自治 拉GFS→写HDFS', center:true }],
  [{ text:'[阶段5] 归档结果上报 (存在+size判定)', fill:'D9E2F3', center:true }],
  [{ text:'大数据写HDFS后 stat校验 ; 大数据 --[HTTP]archive/result--> MGR 置ARCHIVED', center:true }],
  [{ text:'[阶段6] GFS 自动清理', fill:'D9E2F3', center:true }],
  [{ text:'每日: MGR --[HTTP]fs/delete--> GFS ; 仅删【已归档+满该集群留存天数】, 未归档永久留存', center:true }],
]));

// ===== 5 状态机 =====
C.push(h1('第五章  归档状态机'));
C.push(h2('5.1 状态迁移图'));
C.push(diagramTable([
  [{ text:'NOT_ARCHIVED ──下发清单──> ARCHIVING ──HDFS存在+size通过──> ARCHIVED(终态)', center:true }],
  [{ text:'      ^                         |', center:true }],
  [{ text:'      └──── 回退下轮窗口 ◄── ARCHIVE_FAILED ◄── 拉取失败/超时', center:true }],
  [{ text:'ARCHIVE_FAILED ──重试──> ARCHIVING', center:true }],
]));
C.push(h2('5.2 状态机核心逻辑（Python）'));
C.push(codeBlock([
  'class ArchiveStateMachine:',
  '    ALLOWED = {',
  '        NOT_ARCHIVED:  {ARCHIVING, ARCHIVE_FAILED},',
  '        ARCHIVING:     {ARCHIVED, ARCHIVE_FAILED},',
  '        ARCHIVE_FAILED:{ARCHIVING, NOT_ARCHIVED},',
  '        ARCHIVED:      set(),   # 终态, 不可回退',
  '    }',
  '    def transition(self, target):',
  '        if target == self._state: return self._state   # 幂等',
  '        if target not in self.ALLOWED[self._state]: raise ValueError("非法迁移")',
  '        self._state = target; return self._state',
]));
C.push(p('关键不变量：幂等（同状态 transition 为空操作）；终态锁定（ARCHIVED 不回退，保证每份备份仅归档一次）；存在+size 判定（ARCHIVING→ARCHIVED 需 HDFS 文件存在且 size 与清单一致）；可重试（FAILED→ARCHIVING/NOT_ARCHIVED）。',{bullet:true}));
C.push(h2('5.3 补发任务状态机'));
C.push(codeBlock([
  'PENDING --下发--> RUNNING --成功--> SUCCESS',
  '                    \\--失败(≤3次重试)--> RUNNING --> FAILED(转人工)',
]));

// ===== 6 DDL =====
C.push(h1('第六章  元数据表结构设计（DDL）'));
C.push(p('数据库 MySQL 8.0 InnoDB / utf8mb4。核心唯一业务维度：(cluster_id, db_name, backup_date, backup_edition)。'));
C.push(h2('6.1 表清单'));
const tblRows = [
  ['表名','用途'],
  ['tb_cluster','MySQL集群拓扑表（异地/同城地域、窗口）'],
  ['tb_db','库绑定集群，声明备份版本(异地/同城)与错峰归档'],
  ['tb_backup_meta','备份元数据主表（每份备份一条，唯一维度唯一约束）'],
  ['tb_archive_batch','归档批次表'],
  ['tb_archive_item','归档清单条目表（传输状态）'],
  ['tb_replenish_task','补发任务表（防抖）'],
  ['tb_inspect','巡检记录表'],
  ['tb_inspect_missing','巡检缺失明细表'],
  ['tb_audit_log','操作流水表（全生命周期留痕）'],
];
C.push(new Table({
  columnWidths:[4500,5050], width:{size:9550,type:WidthType.DXA},
  rows:tblRows.map((r,ri)=>new TableRow({children:r.map((c,ci)=>new TableCell({
    width:{size:[4500,5050][ci],type:WidthType.DXA},
    shading: ri===0 ? {type:ShadingType.CLEAR,fill:'D9E2F3'} : undefined,
    children:[new Paragraph({spacing:{after:40},children:[txt(c,{size:16,bold:ri===0})]})],
  }))})),
}));
C.push(h2('6.2 关键表 DDL'));
C.push(h3('tb_cluster — 集群拓扑（维护异地/同城关系）'));
C.push(codeBlock([
  'CREATE TABLE tb_cluster (',
  '  cluster_id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,',
  '  cluster_code VARCHAR(64)  NOT NULL,',
  '  primary_region VARCHAR(32) NOT NULL,        /* 主地域 bj-a */',
  '  dr_region    VARCHAR(32)  NULL,             /* 异地备地域 sh-b */',
  '  city_region  VARCHAR(32)  NULL,             /* 同城备地域 bj-b */',
  '  editions     VARCHAR(64)  DEFAULT "DR,SAME_CITY", /* 启用的备份版本 */',
  '  retention_days SMALLINT NULL DEFAULT 10,   /* 集群级GFS留存: 0=归档后即可清, NULL=默认10 */',
  '  archive_window_days SMALLINT DEFAULT 15,    /* 归档窗口(库级可覆盖) */',
  '  PRIMARY KEY(cluster_id), UNIQUE KEY uk_cluster_code(cluster_code)',
  ');',
]));
C.push(h3('tb_backup_meta — 备份元数据主表'));
C.push(codeBlock([
  'CREATE TABLE tb_backup_meta (',
  '  backup_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,',
  '  cluster_id BIGINT UNSIGNED NOT NULL, db_id BIGINT UNSIGNED NOT NULL,',
  '  db_name VARCHAR(128) NOT NULL,',
  '  backup_date DATE NOT NULL,',
  '  backup_edition VARCHAR(32) NOT NULL, /* DISASTER_RECOVERY=异地备 / SAME_CITY=同城备 */',
  '  gfs_path VARCHAR(1024) NOT NULL,  peer_gfs_path VARCHAR(1024) NULL, /* 两独立卷路径 */',
  '  file_size_bytes BIGINT NOT NULL,  peer_file_size_bytes BIGINT NULL, /* stat实测 */',
  '  upload_verify VARCHAR(20) DEFAULT "PENDING",  /* PENDING/VERIFIED/FAILED (存在+非空) */',
  '  upload_status VARCHAR(20) DEFAULT "PENDING",  upload_fail_reason VARCHAR(20) NULL,',
  '  is_replenished TINYINT DEFAULT 0, replenish_task_id VARCHAR(64) NULL,',
  '  archive_status VARCHAR(20) DEFAULT "NOT_ARCHIVED",',
  '  archive_batch_id VARCHAR(64) NULL, archived_at DATETIME NULL,',
  '  hdfs_path VARCHAR(1024) NULL, hdfs_size_bytes BIGINT NULL,',
  '  can_delete TINYINT DEFAULT 0, deleted_at DATETIME NULL,',
  '  PRIMARY KEY(backup_id),',
  '  UNIQUE KEY uk_backup_dim (cluster_id, db_id, backup_date, backup_edition),',
  '  KEY idx_archive_status (archive_status)',
  '); /* 唯一约束 = 每份备份仅归档一次的库级保证; 判定=存在+size, 不做MD5 */',
]));
C.push(h3('tb_replenish_task — 补发任务（防抖）'));
C.push(codeBlock([
  'CREATE TABLE tb_replenish_task (',
  '  task_id VARCHAR(64) NOT NULL, inspect_id BIGINT NULL,',
  '  cluster_id BIGINT NOT NULL, db_id BIGINT NOT NULL, db_name VARCHAR(128) NOT NULL,',
  '  backup_date DATE NOT NULL, backup_edition VARCHAR(32) NOT NULL,',
  '  status VARCHAR(20) DEFAULT "PENDING", retry_count TINYINT DEFAULT 0, max_retry TINYINT DEFAULT 3,',
  '  gfs_path VARCHAR(1024) NULL, file_size_bytes BIGINT NULL,',
  '  PRIMARY KEY(task_id),',
  '  UNIQUE KEY uk_replenish_dim (cluster_id, db_id, backup_date, backup_edition)',
  '); /* 同一维度未完成前不重复下发=防抖 */',
]));

// ===== 7 部署与调度 =====
C.push(h1('第七章  部署与调度建议'));
C.push(h2('7.1 建议调度时序（配置化）'));
const sched = [
  ['任务','建议时段','说明'],
  ['备份生成上报','每日 00:00–02:00','业务低峰'],
  ['完整性巡检+补发','每日 04:00','等备份落地后'],
  ['滚动清单下发','每日 05:00','补发完成后再归档'],
  ['大数据拉取归档','每日 05:30 起','错峰、可切片'],
  ['GFS 自动清理','每日 02:00 / 07:00','仅删已归档+满该集群留存天数'],
];
C.push(new Table({
  columnWidths:[3300,3300,2950], width:{size:9550,type:WidthType.DXA},
  rows:sched.map((r,ri)=>new TableRow({children:r.map((c,ci)=>new TableCell({
    width:{size:[3300,3300,2950][ci],type:WidthType.DXA},
    shading: ri===0 ? {type:ShadingType.CLEAR,fill:'D9E2F3'} : undefined,
    children:[new Paragraph({spacing:{after:40},children:[txt(c,{size:16,bold:ri===0})]})],
  }))})),
}));
C.push(h2('7.2 可靠性保障'));
C.push(p('双卷存在性自检闭环：DB写后自查(同城+异地两卷) → 管控经 HTTP stat 复核 → 大数据写 HDFS 后 size 校验；不做 MD5。',{bullet:true}));
C.push(p('管控与 GFS / 大数据全走 HTTP，不直连文件系统——组件边界清晰，便于将内存桩替换为真实 GlusterFS / 大数据服务。',{bullet:true}));
C.push(p('归档状态机 + 唯一维度 DDL 双保险，保证幂等与终态（每份备份仅归档一次）。',{bullet:true}));
C.push(p('巡检窗口(30天) ≥ 库级归档窗口(默认15天)+缓冲，避免误判缺失。',{bullet:true}));

// ===== 8 上传完整性判定 =====
C.push(h1('第八章  文件上传完整性判定（GlusterFS）'));
C.push(p('背景：GFS 为 GlusterFS，数据库端以 FUSE 挂载为本地目录；备份文件即普通本地路径写入。判定口径：不校验 MD5，采用「文件存在 + 非空（size>最小阈值，建议10MB）」。异地备、同城备为两个独立 GlusterFS 卷，数据库把同一份备份同时写入同城卷与异地卷两个路径，各自通过存在校验后才上报元数据。'));
C.push(h2('8.1 两条链路与双卷写入'));
C.push(diagramTable([
  [{ text:'链路A(DB→GFS): 数据库 --写--> 同城卷 /gfs-city + 异地卷 /gfs-dr', center:true }],
  [{ text:'              判定: 两路径均 stat 存在且 size>阈值 → 才上报元数据', center:true }],
  [{ text:'链路B(GFS→大数据): 大数据 --读--> 指定卷 --拉取--> HDFS', center:true }],
  [{ text:'              判定: 读取完成, HDFS副本存在且size合理 → 上报归档成功', center:true }],
]));
const chainRows = [
  ['链路','谁判定','判据'],
  ['A：DB→同城/异地两卷','DB 侧(写后自检)','两路径 stat 均存在 且 size>MIN_FILE_SIZE(10MB)'],
  ['B：GFS→大数据','大数据侧','读取完成, 写HDFS后文件存在且 size 合理'],
];
C.push(new Table({
  columnWidths:[2200,1700,5650], width:{size:9550,type:WidthType.DXA},
  rows:chainRows.map((r,ri)=>new TableRow({children:r.map((c,ci)=>new TableCell({
    width:{size:[2200,1700,5650][ci],type:WidthType.DXA},
    shading: ri===0 ? {type:ShadingType.CLEAR,fill:'D9E2F3'} : undefined,
    children:[new Paragraph({spacing:{after:40},children:[txt(c,{size:15,bold:ri===0})]})],
  }))})),
}));
C.push(p('不做 MD5 的原因：GlusterFS FUSE 挂载目录无对象存储的服务端 Content-MD5 验签能力；且对 GB 级全量备份逐文件读算 MD5 的重 IO 开销不可接受，判定以「存在 + 非空」即可。'));
C.push(h2('8.2 判定流程（DB 写同城 + 异地两卷）'));
C.push(diagramTable([
  [{ text:'DB 生成全量备份(本地临时文件)', center:true }],
  [{ text:'  ▼', center:true }],
  [{ text:'同时写入 同城卷 /gfs-city/.. + 异地卷 /gfs-dr/..', center:true }],
  [{ text:'  ▼', center:true }],
  [{ text:'两侧各自 stat 校验 (size<=阈值 或 不存在 → 判EMPTY/NOT_FOUND 重写该卷)', center:true }],
  [{ text:'  ▼', center:true }],
  [{ text:'两侧均通过 → DB 才上报元数据(archive_status=NOT_ARCHIVED)', center:true }],
]));
C.push(p('判定要点：DB 必须两个卷都校验通过后才上报元数据，任一卷失败触发重写或补发；NOT_ARCHIVED 记录即「双卷文件已存在且非空」的可信前提。GlusterFS 无服务端回执，"写入成功"由 DB 侧 stat 实测确认。',{bullet:true}));
C.push(h2('8.3 链路B：GFS→大数据 判定流程'));
C.push(diagramTable([
  [{ text:'大数据按清单读取卷文件 → 写 HDFS → stat 校验(存在+size==清单记录)', center:true }],
  [{ text:'读取失败或 size 不一致 → TRANSFER_FAILED → 重试(≤3次)', center:true }],
  [{ text:'校验通过 → 上报 archive/result(DONE) → 置 ARCHIVED → 放行清理', center:true }],
]));
C.push(h2('8.4 失败处理与告警策略'));
const failRows = [
  ['失败原因','处理策略','告警级别'],
  ['CONNECTION/TIMEOUT','自动重写/重拉, 指数退避, 上限3次','重试中不告警; 超上限P1'],
  ['EMPTY / NOT_FOUND','删除残file, 重写该卷(重试无意义)','🔴 P0 数据完整性'],
  ['STORAGE(GlusterFS卷异常)','告警人工介入, 标记 BROKEN','🔴 P0'],
  ['REPLENISH 补发失败','重试≤3次, 失败转人工, 参与下轮巡检','🟠 P1'],
];
C.push(new Table({
  columnWidths:[3200,3900,2450], width:{size:9550,type:WidthType.DXA},
  rows:failRows.map((r,ri)=>new TableRow({children:r.map((c,ci)=>new TableCell({
    width:{size:[3200,3900,2450][ci],type:WidthType.DXA},
    shading: ri===0 ? {type:ShadingType.CLEAR,fill:'D9E2F3'} : undefined,
    children:[new Paragraph({spacing:{after:40},children:[txt(c,{size:15,bold:ri===0})]})],
  }))})),
}));
C.push(p('🔴 P0：EMPTY/NOT_FOUND/STORAGE → 立即告警并阻断该文件后续归档。🟠 P1：重试超限 → 转人工。🟡 P2：传输耗时超阈值 → 记录告警定位 IO 瓶颈。',{bullet:true}));
C.push(h2('8.5 管控与 GFS / 大数据组件交互（HTTP 契约，管控不直连文件系统）'));
C.push(codeBlock([
  'GFS 组件(文件载体) HTTP server:',
  '  GET    /v1/fs/stat?path=...  -> { exists, size }   巡检/上报复核/查状态用',
  '  DELETE /v1/fs/delete{path}                         清理用',
  '  (写入由 DB 直写 FUSE 挂载目录, 不归管控)',
  '大数据组件(归档目标/补发执行) HTTP server:',
  '  POST /v1/archive/notify  {batchId, items[]}  下发清单->大数据自治拉GFS写HDFS->回调',
  '  POST /v1/replenish/notify{taskId,path,size}  下发补发->大数据重写缺失卷->回调SUCCESS',
  '目录: /gfs-{city|dr}/<cluster>/<db>/<YYYYMMDD>/<edition>/full.sql.gz  (dr=异地, city=同城)',
]));
C.push(h2('8.6 元数据校验维度（更新字段）'));
C.push(codeBlock([
  '-- tb_backup_meta 更新(去MD5, 增双卷)',
  'gfs_path VARCHAR(1024) "主卷路径", peer_gfs_path VARCHAR "另一卷路径",',
  'file_size_bytes BIGINT "stat实测", peer_file_size_bytes BIGINT,',
  'upload_verify VARCHAR(20) DEFAULT "PENDING",  -- PENDING/VERIFIED/FAILED',
  'upload_fail_reason VARCHAR(20),  -- CONNECTION/TIMEOUT/EMPTY/NOT_FOUND/STORAGE/UNKNOWN',
  '-- 新增表 tb_file_verify_log: chain/volumn/stage/size_actual/exists_flag/result',
  '-- 约束: upload_verify=VERIFIED(双卷存在+非空) 是进入下游巡检/归档/清理的可信前提',
]));

// ===== 9 待确认事项 =====
C.push(h1('第九章  待确认事项（评审清单）'));
C.push(p('归档窗口口径不一致：正文/价值=15天，流程/数据库章节=14天，需统一（本方案采用15天，参数化）。',{bullet:true}));
C.push(p('数据库业务系统 2 处存量改造点（元数据上报、补发指令监听）需确认可否改造。',{bullet:true}));
C.push(p('清理归属已定：通过部署开关 -cleanup-mode 可配 —— mgr(默认/现状)=管控经 HTTP DELETE /v1/fs/delete 触发删除；gfs(原始设想)=管控仅放行判定、GFS 自治清理。出站统一指向 GFS 管理端（stat/delete、归档清单转发、补发指令——DB 代理监听同在管理端）；管控不直连大数据。',{bullet:true}));
C.push(p('确认集群拓扑字段（dr_region/city_region）的维护来源与同步方式（DB 上报 or 管理端维护）。',{bullet:true}));
C.push(p('确认存在判定的最小文件阈值（建议 MIN_FILE_SIZE=10MB）与双卷路径/挂载点命名规范。',{bullet:true}));

// ---------- build ----------
const doc = new Document({
  creator: 'DBArchiveManager 技术方案',
  title: '数据库备份策略优化方案-技术方案',
  styles: {
    default: { document: { run: { font: FONT, size: 21 } } },
  },
  numbering: { config: [ { reference:'bullets', levels:[ {level:0, format:LevelFormat.BULLET, text:'•', alignment:AlignmentType.LEFT, style:{paragraph:{indent:{left:360, hanging:200}}}} ] } ] },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 }, margin: { top:1440, bottom:1440, left:1440, right:1440 } } },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync('/Users/gracegaoya/work/project/db-bakup/技术方案/数据库备份策略优化方案-技术方案.docx', buf);
  console.log('DOCX written, bytes:', buf.length);
}).catch(e => { console.error('ERR', e); process.exit(1); });

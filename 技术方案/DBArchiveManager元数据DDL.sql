-- ============================================================================
-- DBArchiveManager 元数据表结构设计 (DDL)
-- 数据库: MySQL 5.7+ / 8.0 (管控系统元数据库)
-- 字符集: utf8mb4  引擎: InnoDB
-- 核心唯一业务维度: (cluster_id, db_name, backup_date, backup_edition)
--   backup_edition 体现 MySQL 集群「异地备 DISASTER_RECOVERY / 同城备 SAME_CITY」
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. MySQL 集群拓扑表：维护「异地备/同城备」集群关系
-- ----------------------------------------------------------------------------
CREATE TABLE tb_cluster (
    cluster_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '集群ID',
    cluster_code    VARCHAR(64)     NOT NULL COMMENT '集群编码(唯一)',
    cluster_name    VARCHAR(128)    NOT NULL COMMENT '集群名称',
    primary_region  VARCHAR(32)     NOT NULL COMMENT '主地域, 如 bj-a',
    dr_region       VARCHAR(32)     NULL     COMMENT '异地备地域(跨地域灾备), 如 sh-b',
    city_region     VARCHAR(32)     NULL     COMMENT '同城备地域(同城冗余), 如 bj-b',
    editions        VARCHAR(64)     NOT NULL DEFAULT 'DR,SAME_CITY' COMMENT '启用的备份版本: DR=异地备, SAME_CITY=同城备',
    retention_days  SMALLINT        NULL     DEFAULT 10 COMMENT '集群级GFS留存天数: 0=归档后即可清, NULL=未配置(默认10)',
    archive_window_days SMALLINT    NOT NULL DEFAULT 15  COMMENT '滚动归档窗口天数',
    path_template   VARCHAR(512)    NULL     DEFAULT NULL COMMENT '集群级GFS路径模板(空→全局默认), 占位符{root}{cluster}{db}{date}{vol}',
    enabled         TINYINT         NOT NULL DEFAULT 1   COMMENT '是否启用',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (cluster_id),
    UNIQUE KEY uk_cluster_code (cluster_code)
) ENGINE=InnoDB COMMENT='MySQL集群拓扑表(异地备/同城备关系)';

-- ----------------------------------------------------------------------------
-- 1.1 集群实例IP关联表：集群 ↔ 物理备份源IP。
--   一个IP只属于一个集群(host_ip 全表唯一)；一个集群可挂多台实例/多IP。
--   备份文件路径以IP标识(如 /gfs/<ip>/<db>/<date>/city/full.sql.gz)，上报时按IP反查集群。
-- ----------------------------------------------------------------------------
CREATE TABLE tb_cluster_instance (
    instance_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '实例ID',
    cluster_id      BIGINT UNSIGNED NOT NULL COMMENT '所属集群',
    host_ip         VARCHAR(64)     NOT NULL COMMENT '实例IP(唯一, 备份文件路径标识)',
    host_name       VARCHAR(128)    NULL     COMMENT '主机名',
    role            VARCHAR(32)     NULL     COMMENT '角色: MASTER/REPLICA 等',
    enabled         TINYINT         NOT NULL DEFAULT 1,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (instance_id),
    UNIQUE KEY uk_host_ip (host_ip),
    KEY idx_cluster_id (cluster_id)
) ENGINE=InnoDB COMMENT='集群实例IP关联表(按IP反查集群)';

-- ----------------------------------------------------------------------------
-- 2. 库表：绑定库到集群，并声明该库启用的备份版本(异地/同城)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_db (
    db_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '库ID',
    cluster_id      BIGINT UNSIGNED NOT NULL COMMENT '所属集群',
    db_name         VARCHAR(128)    NOT NULL COMMENT '库名',
    editions        VARCHAR(64)     NOT NULL DEFAULT 'DR,SAME_CITY' COMMENT '该库启用的备份版本',
    archive_window_days SMALLINT    NOT NULL DEFAULT 15 COMMENT '库级归档窗口(可覆盖集群默认)',
    archive_time    TIME            NULL     COMMENT '错峰归档时间窗口(分散IO峰值)',
    path_template   VARCHAR(512)    NULL     DEFAULT NULL COMMENT '库级GFS路径模板(优先于集群), 占位符{root}{cluster}{db}{date}{vol}',
    enabled         TINYINT         NOT NULL DEFAULT 1,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (db_id),
    UNIQUE KEY uk_cluster_db (cluster_id, db_name),
    KEY idx_db_name (db_name)
) ENGINE=InnoDB COMMENT='库表(库-集群-备份版本绑定)';

-- ----------------------------------------------------------------------------
-- 3. 备份元数据主表：每份备份文件一条记录(按 库+日期+版本 维度)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_backup_meta (
    backup_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '备份ID',
    cluster_id      BIGINT UNSIGNED NOT NULL COMMENT '集群ID',
    db_id           BIGINT UNSIGNED NOT NULL COMMENT '库ID',
    db_name         VARCHAR(128)    NOT NULL COMMENT '库名(冗余,便于查询)',
    host_ip         VARCHAR(64)     NULL     COMMENT '来源实例IP(上报按IP反查集群, 冗余便于追溯)',
    backup_date     DATE            NOT NULL COMMENT '备份数据日期',
    backup_edition  VARCHAR(32)     NOT NULL COMMENT '备份版本: DISASTER_RECOVERY=异地备 / SAME_CITY=同城备',
    -- 异地备、同城备为两个独立 GlusterFS 卷, 分别记录两卷路径
    gfs_path        VARCHAR(1024)   NOT NULL COMMENT '主卷GFS存储路径(FUSE挂载路径)',
    peer_gfs_path   VARCHAR(1024)   NULL COMMENT '另一卷GFS路径(同城备/异地备对应另一卷)',
    file_size_bytes BIGINT          NOT NULL COMMENT '文件大小(字节, stat实测)',
    peer_file_size_bytes BIGINT     NULL COMMENT '另一卷文件大小(stat实测)',
    upload_verify   VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    COMMENT '上传判定: PENDING/VERIFIED/FAILED (存在+非空, 不做MD5)',
    upload_status   VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    COMMENT '上传状态: PENDING/WRITING/DONE/FAILED',
    upload_fail_reason VARCHAR(20)  NULL
                    COMMENT '失败原因: CONNECTION/TIMEOUT/EMPTY/NOT_FOUND/STORAGE/UNKNOWN',
    upload_verified_at DATETIME     NULL COMMENT '双卷存在+size校验通过时间',
    upload_retry_count TINYINT      NOT NULL DEFAULT 0 COMMENT '上传重试次数',
    backup_start_time DATETIME      NULL COMMENT '备份开始时间',
    backup_end_time   DATETIME      NULL COMMENT '备份结束时间',
    is_replenished  TINYINT         NOT NULL DEFAULT 0 COMMENT '是否补发: 0否 1是',
    replenish_task_id VARCHAR(64)   NULL COMMENT '补发任务ID',
    archive_status  VARCHAR(20)     NOT NULL DEFAULT 'NOT_ARCHIVED'
                    COMMENT '归档状态: NOT_ARCHIVED/ARCHIVING/ARCHIVED/ARCHIVE_FAILED',
    archive_batch_id VARCHAR(64)    NULL COMMENT '归档批次ID',
    archived_at     DATETIME        NULL COMMENT '归档完成时间',
    hdfs_path       VARCHAR(1024)   NULL COMMENT 'HDFS归档路径',
    hdfs_size_bytes BIGINT          NULL COMMENT 'HDFS副本实际大小(判定一致性用)',
    can_delete      TINYINT         NOT NULL DEFAULT 0 COMMENT '是否满足GFS清理条件',
    deleted_at      DATETIME        NULL COMMENT 'GFS实际清理时间(中转删除)',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (backup_id),
    -- 唯一业务维度: 集群+库+日期+版本 (保证每份备份仅记录一次/仅归档一次)
    UNIQUE KEY uk_backup_dim (cluster_id, db_id, backup_date, backup_edition),
    KEY idx_archive_status (archive_status),
    KEY idx_upload_verify (upload_verify),
    KEY idx_backup_date (backup_date),
    KEY idx_gfs_path (gfs_path(255)),
    KEY idx_archive_batch (archive_batch_id)
) ENGINE=InnoDB COMMENT='备份元数据主表';

-- ----------------------------------------------------------------------------
-- 4. 归档批次表：一次滚动清单 = 一个批次
-- ----------------------------------------------------------------------------
CREATE TABLE tb_archive_batch (
    archive_batch_id VARCHAR(64)    NOT NULL COMMENT '归档批次ID',
    generate_time   DATETIME        NOT NULL COMMENT '清单生成时间',
    cutoff_date     DATE            NOT NULL COMMENT '窗口截止日',
    window_days     SMALLINT        NOT NULL COMMENT '窗口天数',
    item_count      INT             NOT NULL DEFAULT 0 COMMENT '清单条目数',
    status          VARCHAR(20)     NOT NULL DEFAULT 'GENERATED'
                    COMMENT 'GENERATED/PUSHED/PROCESSING/DONE/FAILED',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (archive_batch_id)
) ENGINE=InnoDB COMMENT='归档批次表';

-- ----------------------------------------------------------------------------
-- 5. 归档清单条目表：批次内的每条待归档备份(与 tb_backup_meta 关联)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_archive_item (
    archive_item_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    archive_batch_id VARCHAR(64)    NOT NULL COMMENT '所属批次',
    backup_id       BIGINT UNSIGNED NOT NULL COMMENT '关联备份ID',
    transfer_status VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    COMMENT '传输状态: PENDING/TRANSFERRING/DONE/FAILED',
    retry_count     TINYINT         NOT NULL DEFAULT 0 COMMENT '重试次数',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (archive_item_id),
    UNIQUE KEY uk_batch_backup (archive_batch_id, backup_id),
    KEY idx_transfer_status (transfer_status)
) ENGINE=InnoDB COMMENT='归档清单条目表';

-- ----------------------------------------------------------------------------
-- 6. 补发任务表：缺失备份补发的全生命周期
-- ----------------------------------------------------------------------------
CREATE TABLE tb_replenish_task (
    task_id         VARCHAR(64)     NOT NULL COMMENT '补发任务ID',
    inspect_id      BIGINT UNSIGNED NULL COMMENT '触发巡检ID',
    cluster_id      BIGINT UNSIGNED NOT NULL,
    db_id           BIGINT UNSIGNED NOT NULL,
    db_name         VARCHAR(128)    NOT NULL,
    backup_date     DATE            NOT NULL,
    backup_edition  VARCHAR(32)     NOT NULL COMMENT '异地备/同城备',
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                    COMMENT 'PENDING/RUNNING/SUCCESS/FAILED',
    retry_count     TINYINT         NOT NULL DEFAULT 0,
    max_retry       TINYINT         NOT NULL DEFAULT 3,
    gfs_path        VARCHAR(1024)   NULL COMMENT '补发后新路径',
    file_size_bytes BIGINT          NULL COMMENT '补发后文件大小(stat实测)',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (task_id),
    -- 防抖: 同一 簇+库+日期+版本 未完成前不重复下发
    UNIQUE KEY uk_replenish_dim (cluster_id, db_id, backup_date, backup_edition),
    KEY idx_status (status)
) ENGINE=InnoDB COMMENT='补发任务表';

-- ----------------------------------------------------------------------------
-- 7. 巡检记录表：完整性巡检结果留痕(全流程可追溯)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_inspect (
    inspect_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    inspect_date    DATE            NOT NULL COMMENT '巡检日期',
    target_date     DATE            NOT NULL COMMENT '巡检目标备份日期窗口起点',
    scope           VARCHAR(64)     NOT NULL DEFAULT 'ALL' COMMENT '巡检范围: ALL/ClusterId/DBKey',
    total_slots     INT             NOT NULL COMMENT '总应备槽位数(簇*库*日期*版本)',
    missing_count   INT             NOT NULL DEFAULT 0 COMMENT '缺失数',
    broken_count    INT             NOT NULL DEFAULT 0 COMMENT '损坏数',
    ok_count        INT             NOT NULL DEFAULT 0 COMMENT '正常数',
    status          VARCHAR(20)     NOT NULL DEFAULT 'RUNNING' COMMENT 'RUNNING/DONE',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (inspect_id),
    KEY idx_inspect_date (inspect_date)
) ENGINE=InnoDB COMMENT='巡检记录表';

-- ----------------------------------------------------------------------------
-- 8. 巡检缺失明细表
-- ----------------------------------------------------------------------------
CREATE TABLE tb_inspect_missing (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    inspect_id      BIGINT UNSIGNED NOT NULL,
    cluster_id      BIGINT UNSIGNED NOT NULL,
    db_id           BIGINT UNSIGNED NOT NULL,
    db_name         VARCHAR(128)    NOT NULL,
    backup_date     DATE            NOT NULL,
    backup_edition  VARCHAR(32)     NOT NULL COMMENT '异地备/同城备',
    missing_type    VARCHAR(20)     NOT NULL COMMENT 'MISSING=缺失/BROKEN=损坏',
    replenish_task_id VARCHAR(64)   NULL COMMENT '已下发补发任务ID',
    PRIMARY KEY (id),
    KEY idx_inspect (inspect_id)
) ENGINE=InnoDB COMMENT='巡检缺失明细表';

-- ----------------------------------------------------------------------------
-- 9. 操作流水表：全生命周期留痕(备份/补发/归档/清理/状态变更)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_audit_log (
    log_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    backup_id       BIGINT UNSIGNED NULL COMMENT '关联备份ID',
    action          VARCHAR(40)     NOT NULL COMMENT '动作: META_REPORT/REPLENISH/ROLLING/ARCHIVE/CLEAN',
    detail          JSON            NULL COMMENT '动作详情(JSON, 如前后状态)',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (log_id),
    KEY idx_backup_id (backup_id),
    KEY idx_action (action)
) ENGINE=InnoDB COMMENT='操作流水表';

-- ----------------------------------------------------------------------------
-- 10. 文件存在性校验记录表：两条链路的「存在 + size」判定留痕
--    (GlusterFS 判定口径, 不做 MD5)
-- ----------------------------------------------------------------------------
CREATE TABLE tb_file_verify_log (
    verify_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    backup_id       BIGINT UNSIGNED NOT NULL COMMENT '关联备份ID',
    chain           VARCHAR(20)     NOT NULL COMMENT '链路: WRITE=DB写双卷 / ARCHIVE=GFS->大数据',
    volumn          VARCHAR(20)     NULL COMMENT '卷: SAME_CITY/异地卷(对应 edition 另一卷)',
    stage           VARCHAR(20)     NOT NULL COMMENT '阶段: STAT_EXIST/STAT_SIZE/ARCHIVE_SIZE',
    size_expected   BIGINT          NULL COMMENT '期望大小(阈值或清单记录)',
    size_actual     BIGINT          NULL COMMENT 'stat 实测大小',
    exists_flag     TINYINT         NOT NULL DEFAULT 0 COMMENT '文件是否存在: 0否 1是',
    result          VARCHAR(20)     NOT NULL COMMENT 'PENDING/VERIFIED/FAILED',
    fail_reason     VARCHAR(20)     NULL COMMENT 'CONNECTION/TIMEOUT/EMPTY/NOT_FOUND/STORAGE/UNKNOWN',
    retry_count     TINYINT         NOT NULL DEFAULT 0 COMMENT '重试次数',
    verified_at     DATETIME        NULL COMMENT '校验完成时间',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (verify_id),
    KEY idx_backup_id (backup_id),
    KEY idx_chain_result (chain, result),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB COMMENT='文件存在性校验表(上传/归档存在+size判定留痕)';

-- ============================================================================
-- 索引与数据口径说明
-- ============================================================================
-- 1. tb_backup_meta.uk_backup_dim 唯一约束是「每份备份仅归档一次」的数据库级保证,
--    配合状态机(ARCHIVED 终态)双保险。
-- 2. 异地备/同城备 作为 backup_edition 独立成行, 各自上报/巡检/归档/清理;
--    同城备、异地备是两个独立 GlusterFS 卷, DB 各写一份, gfs_path/peer_gfs_path 分列。
-- 3. 巡检窗口(默认30天) >= 归档窗口(15天)+缓冲, 避免窗口内未归档文件被误判缺失。
-- 4. tb_replenish_task.uk_replenish_dim 实现补发防抖(同一维度未完成不重复下发)。
-- 5. 归档状态迁移与唯一维度约束在应用层由 ArchiveStateMachine 与 DDL 双重保障。
-- 6. 判定口径(GlusterFS/FUSE挂载): 不做MD5, 以「文件存在 + size>最小阈值(建议10MB)」
--    为准。file_size_bytes 为 stat 实测; 双卷均通过才 upload_verify=VERIFIED。
-- 7. tb_backup_meta.upload_verify=VERIFIED (双卷存在+非空) 是本记录可进入下游
--    巡检/归档/清理的可信前提; 未满足的记录不参与归档清单。
-- 8. tb_file_verify_log 记录每次「存在+size」判定(链路A/B), 供审计与故障溯源。



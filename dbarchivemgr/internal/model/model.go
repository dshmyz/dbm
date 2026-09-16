// Package model 定义与元数据表结构(DDL)对应的 GORM 模型。
// 对应《DBArchiveManager元数据DDL.sql》中的 10 张表。
package model

import "time"

// IntPtr 返回 int 的指针，用于可选 int 字段（如 RetentionDays，区分未配置与显式 0）。
func IntPtr(v int) *int { return &v }

// 归档状态 ArchivedStatus
type ArchiveStatus string

const (
	StatusNotArchived   ArchiveStatus = "NOT_ARCHIVED"
	StatusArchiving     ArchiveStatus = "ARCHIVING"
	StatusArchived      ArchiveStatus = "ARCHIVED"
	StatusArchiveFailed ArchiveStatus = "ARCHIVE_FAILED"
)

// 备份版本 BackupEdition：异地备 / 同城备（两个独立 GlusterFS 卷）
type BackupEdition string

const (
	EditionDisasterRecovery BackupEdition = "DISASTER_RECOVERY" // 异地备
	EditionSameCity         BackupEdition = "SAME_CITY"         // 同城备
)

// 上传判定结果 UploadVerify（GlusterFS 存在+非空判定，不做 MD5）
type UploadVerify string

const (
	VerifyPending  UploadVerify = "PENDING"
	VerifyVerified UploadVerify = "VERIFIED"
	VerifyFailed   UploadVerify = "FAILED"
)

// 传输状态 TransferStatus
type TransferStatus string

const (
	TransferPending TransferStatus = "PENDING"
	Transferring    TransferStatus = "TRANSFERRING"
	TransferDone    TransferStatus = "DONE"
	TransferFailed  TransferStatus = "FAILED"
)

// Cluster 对应 tb_cluster：MySQL 集群拓扑（异地备/同城备地域、窗口）
type Cluster struct {
	ID            uint   `gorm:"primaryKey" json:"clusterId"`
	ClusterCode   string `gorm:"uniqueIndex;size:64" json:"clusterCode"`
	ClusterName   string `json:"clusterName"`
	PrimaryRegion string `json:"primaryRegion"`
	DRRegion      string `json:"drRegion"`
	CityRegion    string `json:"cityRegion"`
	Editions      string `gorm:"size:64;default:DR,SAME_CITY" json:"editions"`
	// RetentionDays GFS 归档后可留存天数：0 = 归档后即可清理，nil = 未配置(默认10)。
	// 用指针区分「未配置」与「显式 0」，避免 GORM default 覆盖显式 0。
	RetentionDays     *int   `gorm:"default:10" json:"retentionDays"`
	ArchiveWindowDays int    `gorm:"default:15" json:"archiveWindowDays"`
	PathTemplate      string `gorm:"size:512;default:''" json:"pathTemplate"` // 集群级GFS路径模板，空→默认
	Enabled           bool   `gorm:"default:true" json:"enabled"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// DB 对应 tb_db：库绑定集群并声明启用的备份版本
type DB struct {
	ID                uint   `gorm:"primaryKey" json:"dbId"`
	ClusterID         uint   `json:"clusterId"`
	DBName            string `gorm:"size:128" json:"dbName"`
	Editions          string `gorm:"size:64;default:DR,SAME_CITY" json:"editions"`
	ArchiveWindowDays int    `gorm:"default:15" json:"archiveWindowDays"`
	ArchiveTime       string `gorm:"size:8" json:"archiveTime"`               // 错峰归档时间 HH:MM
	PathTemplate      string `gorm:"size:512;default:''" json:"pathTemplate"` // 库级GFS路径模板，优先于集群
	Enabled           bool   `gorm:"default:true" json:"enabled"`
	CreatedAt         time.Time
	UpdatedAt         time.Time

	Cluster Cluster `gorm:"foreignKey:ClusterID"`
}

// ClusterInstance 对应 tb_cluster_instance：集群 ↔ 实例 IP 关联。
// 一个 IP 只属于一个集群(HostIP 全表唯一)；一个集群可挂多台实例/多 IP。
type ClusterInstance struct {
	ID        uint   `gorm:"primaryKey" json:"instanceId"`
	ClusterID uint   `gorm:"index" json:"clusterId"`
	HostIP    string `gorm:"uniqueIndex;size:64" json:"hostIp"` // 备份文件路径/文件名中的 IP
	HostName  string `gorm:"size:128" json:"hostName"`
	Role      string `gorm:"size:32" json:"role"` // 如 MASTER/REPLICA
	Enabled   bool   `gorm:"default:true" json:"enabled"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

// BackupMeta 对应 tb_backup_meta：备份元数据主表。
// 唯一业务维度 (cluster_id, db_id, backup_date, backup_edition)。
type BackupMeta struct {
	ID                uint          `gorm:"primaryKey" json:"backupId"`
	ClusterID         uint          `json:"clusterId"`
	DBID              uint          `json:"dbId"`
	DBName            string        `gorm:"size:128" json:"dbName"`
	HostIP            string        `gorm:"size:64" json:"hostIp"` // 来源实例 IP（上报时按 IP 反查集群）
	BackupDate        time.Time     `json:"backupDate"`
	BackupEdition     BackupEdition `gorm:"size:32" json:"backupEdition"`
	GFSPath           string        `gorm:"size:1024" json:"gfsPath"`     // 主卷路径
	PeerGFSPath       string        `gorm:"size:1024" json:"peerGfsPath"` // 另一卷路径
	FileSizeBytes     int64         `json:"fileSizeBytes"`                // stat 实测
	PeerFileSizeBytes int64         `json:"peerFileSizeBytes"`
	UploadVerify      UploadVerify  `gorm:"size:20;default:PENDING" json:"uploadVerify"`
	UploadStatus      string        `gorm:"size:20;default:PENDING" json:"uploadStatus"`
	UploadFailReason  string        `gorm:"size:20" json:"uploadFailReason"`
	UploadVerifiedAt  *time.Time    `json:"uploadVerifiedAt"`
	IsReplenished     bool          `gorm:"default:false" json:"isReplenished"`
	ReplenishTaskID   string        `gorm:"size:64" json:"replenishTaskId"`
	ArchiveStatus     ArchiveStatus `gorm:"size:20;default:NOT_ARCHIVED" json:"archiveStatus"`
	ArchiveBatchID    string        `gorm:"size:64" json:"archiveBatchId"`
	ArchivedAt        *time.Time    `json:"archivedAt"`
	HDFSPath          string        `gorm:"size:1024" json:"hdfsPath"`
	HDFSSizeBytes     int64         `json:"hdfsSizeBytes"`
	CanDelete         bool          `gorm:"default:false" json:"canDelete"`
	DeletedAt         *time.Time    `json:"deletedAt"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// ArchiveBatch 对应 tb_archive_batch：一次滚动清单 = 一个批次
type ArchiveBatch struct {
	ID           string    `gorm:"primaryKey;size:64" json:"archiveBatchId"`
	GenerateTime time.Time `json:"generateTime"`
	CutoffDate   time.Time `json:"cutoffDate"`
	WindowDays   int       `json:"windowDays"`
	ItemCount    int       `json:"itemCount"`
	Status       string    `gorm:"size:20;default:GENERATED" json:"status"`
	CreatedAt    time.Time
}

// ArchiveItem 对应 tb_archive_item：批次内每条待归档备份
type ArchiveItem struct {
	ID             uint           `gorm:"primaryKey" json:"archiveItemId"`
	ArchiveBatchID string         `gorm:"size:64;index" json:"archiveBatchId"`
	BackupID       uint           `json:"backupId"`
	TransferStatus TransferStatus `gorm:"size:20;default:PENDING" json:"transferStatus"`
	RetryCount     int            `gorm:"default:0" json:"retryCount"`
	CreatedAt      time.Time
	UpdatedAt      time.Time

	Backup BackupMeta `gorm:"foreignKey:BackupID"`
}

// ReplenishTask 对应 tb_replenish_task：补发任务（防抖）
type ReplenishTask struct {
	TaskID        string        `gorm:"primaryKey;size:64" json:"taskId"`
	InspectID     uint          `json:"inspectId"`
	ClusterID     uint          `json:"clusterId"`
	DBID          uint          `json:"dbId"`
	DBName        string        `gorm:"size:128" json:"dbName"`
	BackupDate    time.Time     `json:"backupDate"`
	BackupEdition BackupEdition `gorm:"size:32" json:"backupEdition"`
	Status        string        `gorm:"size:20;default:PENDING" json:"status"` // PENDING/RUNNING/SUCCESS/FAILED
	RetryCount    int           `gorm:"default:0" json:"retryCount"`
	MaxRetry      int           `gorm:"default:3" json:"maxRetry"`
	GFSPath       string        `gorm:"size:1024" json:"gfsPath"`
	FileSizeBytes int64         `json:"fileSizeBytes"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Inspect 对应 tb_inspect：巡检记录
type Inspect struct {
	ID           uint      `gorm:"primaryKey" json:"inspectId"`
	InspectDate  time.Time `json:"inspectDate"`
	TargetDate   time.Time `json:"targetDate"`
	Scope        string    `gorm:"size:64;default:ALL" json:"scope"`
	TotalSlots   int       `json:"totalSlots"`
	MissingCount int       `json:"missingCount"`
	BrokenCount  int       `json:"brokenCount"`
	OKCount      int       `json:"okCount"`
	Status       string    `gorm:"size:20;default:RUNNING" json:"status"`
	CreatedAt    time.Time
}

// InspectMissing 对应 tb_inspect_missing：巡检缺失明细
type InspectMissing struct {
	ID              uint          `gorm:"primaryKey" json:"id"`
	InspectID       uint          `json:"inspectId"`
	ClusterID       uint          `json:"clusterId"`
	DBID            uint          `json:"dbId"`
	DBName          string        `gorm:"size:128" json:"dbName"`
	BackupDate      time.Time     `json:"backupDate"`
	BackupEdition   BackupEdition `gorm:"size:32" json:"backupEdition"`
	MissingType     string        `gorm:"size:20" json:"missingType"` // MISSING/BROKEN(EMPTY)
	ReplenishTaskID string        `gorm:"size:64" json:"replenishTaskId"`
}

// FileVerifyLog 对应 tb_file_verify_log：文件存在性校验留痕（存在+size，不做 MD5）
type FileVerifyLog struct {
	ID           uint       `gorm:"primaryKey" json:"verifyId"`
	BackupID     uint       `json:"backupId"`
	Chain        string     `gorm:"size:20" json:"chain"`  // WRITE / ARCHIVE
	Volumn       string     `gorm:"size:20" json:"volumn"` // SAME_CITY / DR
	Stage        string     `gorm:"size:20" json:"stage"`  // STAT_EXIST / STAT_SIZE / ARCHIVE_SIZE
	SizeExpected int64      `json:"sizeExpected"`
	SizeActual   int64      `json:"sizeActual"`
	ExistsFlag   bool       `json:"existsFlag"`
	Result       string     `gorm:"size:20" json:"result"` // PENDING/VERIFIED/FAILED
	FailReason   string     `gorm:"size:20" json:"failReason"`
	RetryCount   int        `gorm:"default:0" json:"retryCount"`
	VerifiedAt   *time.Time `json:"verifiedAt"`
	CreatedAt    time.Time
}

// GfsEndpoint 多机房 GFS 管理端配置（机房→管理端地址），管控按机房分发 stat/delete。
// Region 须与 Cluster.cityRegion（同城卷）/drRegion（异地卷）填写一致；
// Region="*" 为未匹配机房的兜底管理端。界面/接口可增删改，改动即时生效（短缓存）。
type GfsEndpoint struct {
	ID        uint      `gorm:"primaryKey" json:"endpointId"`
	Region    string    `gorm:"size:64;uniqueIndex" json:"region"`
	BaseURL   string    `gorm:"size:255" json:"baseUrl"`
	Remark    string    `gorm:"size:255" json:"remark"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

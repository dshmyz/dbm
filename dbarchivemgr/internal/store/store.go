// Package store 负责元数据库初始化（GORM + SQLite）与表结构迁移。
// 对应《DBArchiveManager元数据DDL.sql》。
package store

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
)

// New 打开 SQLite 元数据库并自动迁移全部表。
// dsn 如 ":memory:" 或 "file:dbarchivemgr.db"。
func New(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("打开元数据库失败: %w", err)
	}

	// 自动迁移（对应 DDL 的 10 张表）
	if err := db.AutoMigrate(
		&model.Cluster{},
		&model.ClusterInstance{},
		&model.DB{},
		&model.BackupMeta{},
		&model.ArchiveBatch{},
		&model.ArchiveItem{},
		&model.ReplenishTask{},
		&model.Inspect{},
		&model.InspectMissing{},
		&model.FileVerifyLog{},
		&model.AuditLog{},
		&model.GfsEndpoint{},
	); err != nil {
		return nil, fmt.Errorf("元数据建表失败: %w", err)
	}

	return db, nil
}

// Package scheduler 提供定时调度：把每日日常作业（完整性巡检+缺失补发、滚动归档清单）
// 挂到 cron 上自动触发，替换手动 curl。仅编排既有 service 方法，不承载新业务逻辑。
package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
)

// LastRun 某任务最近一次运行的摘要（供状态查询接口展示）。
type LastRun struct {
	Spec      string     `json:"spec"`
	LastAt    *time.Time `json:"lastAt"`
	LastCount int        `json:"lastCount"`
	LastOK    bool       `json:"lastOK"`
}

// 定时作业规格（包内常量，无需配置）。
// 扫描排在滚窗之前：超时置失败后，当天滚窗即可重试。
const (
	inspectProdSpec    = "0 0 3 * * *"  // 巡检+补发: 生产每日3点
	inspectDemoSpec    = "@every 15s"   // 演示15s
	rollingProdSpec    = "0 0 2 * * *"  // 滚动清单: 生产每日2点
	rollingDemoSpec    = "@every 30s"   // 演示30s
	sweepSpec          = "0 0 1 * * *"  // 归档超时扫描: 生产每日1点
	inspectHorizonDays = 30             // 巡检扫描天数(≥归档窗口, 见 DDL 口径说明)
)

// Scheduler 编排日常定时作业。
type Scheduler struct {
	svc *service.Service
	db  *gorm.DB
	log *log.Logger

	inspectSpec string
	rollingSpec string

	mu   sync.Mutex
	last map[string]LastRun
}

// New 构造调度器。demo=true 用秒级演示节奏，false 用每日凌晨生产节奏。
// GFS 清理由 GFS 自治（删前自查 canDelete 状态接口），管控不挂清理作业。
func New(svc *service.Service, db *gorm.DB, demo bool) *Scheduler {
	is, rs := inspectProdSpec, rollingProdSpec
	if demo {
		is, rs = inspectDemoSpec, rollingDemoSpec
	}
	return &Scheduler{
		svc: svc, db: db, log: log.New(log.Writer(), "[sched] ", log.LstdFlags),
		inspectSpec: is, rollingSpec: rs,
		last: make(map[string]LastRun),
	}
}

// Start 把所有作业注册到 cron 并开始执行。
func (s *Scheduler) Start(c *cron.Cron) error {
	if _, err := c.AddFunc(s.inspectSpec, s.runInspectAndReplenish); err != nil {
		return err
	}
	if _, err := c.AddFunc(s.rollingSpec, s.runRollingList); err != nil {
		return err
	}
	if _, err := c.AddFunc(sweepSpec, s.runSweepArchive); err != nil {
		return err
	}
	c.Start()
	s.log.Printf("定时作业已启动: 巡检=%s 滚动=%s 超时扫描=%s(巡检天数=%d)",
		s.inspectSpec, s.rollingSpec, sweepSpec, inspectHorizonDays)
	return nil
}

// record 记录一次运行结果。
func (s *Scheduler) record(name, spec string, count int, ok bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last[name] = LastRun{Spec: spec, LastAt: &now, LastCount: count, LastOK: ok}
}

// Snapshot 返回各任务最近一次运行摘要（副本，安全并发读）。
func (s *Scheduler) Snapshot() map[string]LastRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]LastRun, len(s.last))
	for k, v := range s.last {
		out[k] = v
	}
	return out
}

// runInspectAndReplenish 每日完整性巡检 + 缺失自动下发补发。
// 巡检（RunInspect）产出缺失明细；逐条 CreateReplenishTask 下发补发（已有防抖，重复幂等）。
func (s *Scheduler) runInspectAndReplenish() {
	res, err := s.svc.RunInspect(time.Now(), inspectHorizonDays)
	if err != nil {
		s.log.Printf("巡检失败: %v", err)
		s.record("inspect", s.inspectSpec, -1, false)
		return
	}
	replenished := 0
	for _, m := range res.MissingList {
		if _, err := s.svc.CreateReplenishTask(m.ClusterID, m.DBID, m.DBName, m.BackupDate, m.BackupEdition); err != nil {
			s.log.Printf("下发补发失败 cluster=%d db=%s date=%s: %v", m.ClusterID, m.DBName, m.BackupDate, err)
			continue
		}
		replenished++
	}
	if res.MissingCount > 0 {
		s.log.Printf("巡检完成 total=%d ok=%d missing=%d，下发补发 %d 条", res.TotalSlots, res.OKCount, res.MissingCount, replenished)
	}
	s.record("inspect", s.inspectSpec, res.MissingCount, true)
}

// runRollingList 滚动归档清单生成：按每个库自身的归档窗口，将已过窗的 NOT_ARCHIVED 备份纳入批次。
func (s *Scheduler) runRollingList() {
	batchID, items, err := s.svc.GenerateRollingList(time.Now())
	if err != nil {
		s.log.Printf("滚动清单生成失败: %v", err)
		s.record("rolling", s.rollingSpec, -1, false)
		return
	}
	s.log.Printf("滚动清单生成 batch=%s items=%d", batchID, len(items))
	s.record("rolling", s.rollingSpec, len(items), true)
}

// runSweepArchive 归档超时兜底：卡死/回调丢失的归档置失败，下轮清单直接重试。
func (s *Scheduler) runSweepArchive() {
	timedOut, err := s.svc.SweepArchive()
	if err != nil {
		s.log.Printf("归档扫描失败: %v", err)
		s.record("sweep", sweepSpec, -1, false)
		return
	}
	if timedOut > 0 {
		s.log.Printf("归档扫描: 超时标记=%d", timedOut)
	}
	s.record("sweep", sweepSpec, timedOut, true)
}

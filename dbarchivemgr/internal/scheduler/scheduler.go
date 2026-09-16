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

// Scheduler 编排日常定时作业。
type Scheduler struct {
	svc *service.Service
	db  *gorm.DB
	log *log.Logger

	windowDays  int
	inspectSpec string
	rollingSpec string

	mu   sync.Mutex
	last map[string]LastRun
}

// New 构造调度器。windowDays 为巡检与滚窗共用的归档窗口天数。
func New(svc *service.Service, db *gorm.DB, windowDays int, inspectSpec, rollingSpec string) *Scheduler {
	return &Scheduler{
		svc: svc, db: db, log: log.New(log.Writer(), "[sched] ", log.LstdFlags),
		windowDays: windowDays, inspectSpec: inspectSpec, rollingSpec: rollingSpec,
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
	c.Start()
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
	res, err := s.svc.RunInspect(time.Now(), s.windowDays)
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

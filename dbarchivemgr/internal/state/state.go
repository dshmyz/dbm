// Package state 实现归档状态机与补发任务状态机。
// 对应《DBArchiveManager时序图与状态机》第 5 章。
package state

import (
	"fmt"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
)

// ArchiveStateMachine 库级归档元数据状态机。
// 唯一业务维度: (cluster, db, backup_date, edition)。
//
// 迁移图:
//
//	NOT_ARCHIVED ──下发清单──> ARCHIVING ──HDFS存在+size通过──> ARCHIVED(终态)
//	     ^                         |
//	     └──── 回退下轮窗口 ◄── ARCHIVE_FAILED ◄── 拉取失败/超时
//	ARCHIVE_FAILED ──重试──> ARCHIVING
type ArchiveStateMachine struct {
	state model.ArchiveStatus
}

// allowed 记录每个状态可达的目标状态集合。
var allowed = map[model.ArchiveStatus]map[model.ArchiveStatus]bool{
	model.StatusNotArchived: {
		model.StatusArchiving:     true,
		model.StatusArchiveFailed: true,
	},
	model.StatusArchiving: {
		model.StatusArchived:      true,
		model.StatusArchiveFailed: true,
	},
	model.StatusArchiveFailed: {
		model.StatusArchiving:   true,
		model.StatusNotArchived: true,
	},
	// StatusArchived 为终态，不可回退
}

// NewArchiveStateMachine 构造状态机。
func NewArchiveStateMachine(initial model.ArchiveStatus) *ArchiveStateMachine {
	return &ArchiveStateMachine{state: initial}
}

// State 返回当前状态。
func (m *ArchiveStateMachine) State() model.ArchiveStatus { return m.state }

// Can 判断到 target 的迁移是否合法。
func (m *ArchiveStateMachine) Can(target model.ArchiveStatus) bool {
	return allowed[m.state][target]
}

// Transition 执行状态迁移；幂等：同状态不动作；非法迁移返回 error。
func (m *ArchiveStateMachine) Transition(target model.ArchiveStatus) (model.ArchiveStatus, error) {
	if target == m.state {
		return m.state, nil // 幂等
	}
	if !m.Can(target) {
		return m.state, fmt.Errorf("非法状态迁移: %s -> %s", m.state, target)
	}
	m.state = target
	return m.state, nil
}

// --- 业务事件驱动的封装 ---

// OnMetaReport 元数据入库/补发完成后置为 NOT_ARCHIVED（强制复位新记录）。
func OnMetaReport(meta *model.BackupMeta) {
	meta.ArchiveStatus = model.StatusNotArchived
}

// OnRollingSubmit 下发归档清单：NOT_ARCHIVED -> ARCHIVING。
func OnRollingSubmit(meta *model.BackupMeta) error {
	sm := NewArchiveStateMachine(meta.ArchiveStatus)
	s, err := sm.Transition(model.StatusArchiving)
	if err != nil {
		return err
	}
	meta.ArchiveStatus = s
	return nil
}

// OnArchiveResult 大数据归档结果回调。
// sizeValid=true（拉取成功、校验通过）：信任实际结果，从 NOT_ARCHIVED/ARCHIVE_FAILED
// 经 ARCHIVING 收敛到 ARCHIVED；ARCHIVED 幂等。避免「判超时后大数据其实成功、结果却被拒」。
// sizeValid=false：置 ARCHIVE_FAILED。
func OnArchiveResult(meta *model.BackupMeta, sizeValid bool) error {
	if sizeValid {
		sm := NewArchiveStateMachine(meta.ArchiveStatus)
		if meta.ArchiveStatus != model.StatusArchiving && meta.ArchiveStatus != model.StatusArchived {
			s, err := sm.Transition(model.StatusArchiving)
			if err != nil {
				return err
			}
			meta.ArchiveStatus = s
		}
		if meta.ArchiveStatus == model.StatusArchived {
			return nil // 幂等
		}
		s, err := sm.Transition(model.StatusArchived)
		if err != nil {
			return err
		}
		meta.ArchiveStatus = s
		return nil
	}
	sm := NewArchiveStateMachine(meta.ArchiveStatus)
	s, err := sm.Transition(model.StatusArchiveFailed)
	if err != nil {
		return err
	}
	meta.ArchiveStatus = s
	return nil
}

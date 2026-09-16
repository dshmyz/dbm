package state

import (
	"testing"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
)

func TestLegalTransitions(t *testing.T) {
	cases := []struct {
		from, to model.ArchiveStatus
		wantErr  bool
	}{
		{model.StatusNotArchived, model.StatusArchiving, false},
		{model.StatusNotArchived, model.StatusArchiveFailed, false},
		{model.StatusArchiving, model.StatusArchived, false},
		{model.StatusArchiving, model.StatusArchiveFailed, false},
		{model.StatusArchiveFailed, model.StatusArchiving, false},
		{model.StatusArchiveFailed, model.StatusNotArchived, false},
		// 终态不可回退
		{model.StatusArchived, model.StatusNotArchived, true},
		{model.StatusArchived, model.StatusArchiving, true},
		{model.StatusArchived, model.StatusArchiveFailed, true},
		// 跳级非法
		{model.StatusNotArchived, model.StatusArchived, true},
	}
	for _, c := range cases {
		sm := NewArchiveStateMachine(c.from)
		_, err := sm.Transition(c.to)
		if (err != nil) != c.wantErr {
			t.Errorf("%s->%s: got err=%v, want err=%v", c.from, c.to, err, c.wantErr)
		}
	}
}

func TestIdempotent(t *testing.T) {
	sm := NewArchiveStateMachine(model.StatusNotArchived)
	s, err := sm.Transition(model.StatusNotArchived)
	if err != nil || s != model.StatusNotArchived {
		t.Fatalf("同状态迁移应幂等无副作用, got %s err=%v", s, err)
	}
}

func TestArchiveResultSizeValid(t *testing.T) {
	meta := &model.BackupMeta{ArchiveStatus: model.StatusArchiving}
	if err := OnArchiveResult(meta, true); err != nil {
		t.Fatal(err)
	}
	if meta.ArchiveStatus != model.StatusArchived {
		t.Fatalf("size 校验通过应置 ARCHIVED, got %s", meta.ArchiveStatus)
	}
}

func TestArchiveResultSizeInvalid(t *testing.T) {
	meta := &model.BackupMeta{ArchiveStatus: model.StatusArchiving}
	if err := OnArchiveResult(meta, false); err != nil {
		t.Fatal(err)
	}
	if meta.ArchiveStatus != model.StatusArchiveFailed {
		t.Fatalf("size 校验失败应置 ARCHIVE_FAILED, got %s", meta.ArchiveStatus)
	}
}

func TestOnRollingFromFailed(t *testing.T) {
	// 从 ARCHIVE_FAILED 不应直接下发到 ARCHIVING 之外的状态
	meta := &model.BackupMeta{ArchiveStatus: model.StatusArchiveFailed}
	if err := OnRetry(meta); err != nil {
		t.Fatal(err)
	}
	if meta.ArchiveStatus != model.StatusArchiving {
		t.Fatalf("重试应到 ARCHIVING, got %s", meta.ArchiveStatus)
	}
}

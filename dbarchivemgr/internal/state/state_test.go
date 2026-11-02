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

func TestSuccessFromFailureState(t *testing.T) {
	// 判超时/失败后大数据其实成功：成功结果应从 ARCHIVE_FAILED 收敛到 ARCHIVED，不被拒绝
	meta := &model.BackupMeta{ArchiveStatus: model.StatusArchiveFailed}
	if err := OnArchiveResult(meta, true); err != nil {
		t.Fatalf("成功结果应被接受, got err=%v", err)
	}
	if meta.ArchiveStatus != model.StatusArchived {
		t.Fatalf("应收敛到 ARCHIVED, got %s", meta.ArchiveStatus)
	}
}

func TestSuccessIdempotentOnArchived(t *testing.T) {
	meta := &model.BackupMeta{ArchiveStatus: model.StatusArchived}
	if err := OnArchiveResult(meta, true); err != nil {
		t.Fatal(err)
	}
	if meta.ArchiveStatus != model.StatusArchived {
		t.Fatalf("ARCHIVED 重复成功应幂等, got %s", meta.ArchiveStatus)
	}
}

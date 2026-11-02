package scheduler

import (
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// seedWithDB 在给定 db 上构造 1 集群 + 双卷近3天备份，并删掉 2 天前异地卷制造缺失。
func seedWithDB(t *testing.T, db *gorm.DB) *service.Service {
	t.Helper()
	gfs := sim.NewGFS("/gfs")
	svc := service.New(db, &gfsclient.MemFileStore{GFS: gfs})
	now := time.Now()

	cl, err := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-SCH", RetentionDays: model.IntPtr(0)})
	if err != nil {
		t.Fatal(err)
	}
	// 窗口设 1 天：仅 1 天前的备份过窗，滚动 batch 可确定纳入非空
	dbm, _ := svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "pay", ArchiveWindowDays: 1})
	hostIP := "10.3.3.3"
	if _, err := svc.AddClusterInstance(cl.ID, hostIP, "db-pay", "MASTER"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		day := now.AddDate(0, 0, -i)
		city, dr := sim.BuildPaths("", hostIP, dbm.DBName, day.Format("2006-01-02"), string(model.EditionSameCity))
		_ = gfs.WriteFull(city, 64*1024*1024)
		_ = gfs.WriteFull(dr, 64*1024*1024)
		if _, err := svc.ReportBackupMeta(hostIP, dbm.DBName, day, model.EditionSameCity, city, dr, 0); err != nil {
			t.Fatal(err)
		}
	}
	// 制造缺失：2 天前删异地卷
	missDay := now.AddDate(0, 0, -2)
	_, drPath := sim.BuildPaths("", hostIP, dbm.DBName, missDay.Format("2006-01-02"), string(model.EditionSameCity))
	gfs.Delete(drPath)
	return svc
}

// TestInspectReplenish 巡检 job 自动下发补发：存在的缺失得到 CreateReplenishTask（防抖幂等）。
func TestInspectReplenish(t *testing.T) {
	db, _ := store.New(":memory:")
	svc := seedWithDB(t, db)
	s := New(svc, db, true)
	s.runInspectAndReplenish()

	snap := s.Snapshot()
	lr, ok := snap["inspect"]
	if !ok || !lr.LastOK || lr.LastCount != 1 {
		t.Fatalf("巡检应记录1条缺失: snap=%+v", snap)
	}

	// 应为缺失项下发补发任务（RUNNING）
	var tasks []model.ReplenishTask
	if err := db.Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "RUNNING" {
		t.Fatalf("应下发1条补发任务: %+v", tasks)
	}
}

// TestRollingList 滚动 job 生成批次：过窗口的 NOT_ARCHIVED 备份被纳入。
func TestRollingList(t *testing.T) {
	db, _ := store.New(":memory:")
	svc := seedWithDB(t, db)
	s := New(svc, db, true)
	s.runRollingList()

	snap := s.Snapshot()
	lr, ok := snap["rolling"]
	if !ok || !lr.LastOK || lr.LastCount == 0 {
		t.Fatalf("滚动清单应生成非空批次: snap=%+v", snap)
	}
}

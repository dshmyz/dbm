package service

// 滚动归档清单机制的专项测试：
// 窗口边界、候选过滤（状态×校验）、重复触发不重复下发、批次号回填、
// 推送失败恢复闭环、归档 size 不匹配重试、并发触发抢占不重不漏。

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/notify"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// newRollingSvc 构造滚动测试环境：1 集群 + 1 库（窗口 window）+ 实例 IP。
// 注意：DB 行必须显式配窗口——GORM 的 default:15 会在建行时写死 15，
// 未显式配窗口的库永远生效 15 天，集群级窗口对它无效。
func newRollingSvc(t *testing.T, window int) (*Service, *sim.GFS, *model.Cluster, *model.DB) {
	t.Helper()
	svc, gfs := newSvc(t)
	cl, err := svc.RegisterCluster(&model.Cluster{
		ClusterCode: "CL-ROLL", ArchiveWindowDays: window, RetentionDays: model.IntPtr(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddClusterInstance(cl.ID, "10.0.0.7", "a", "MASTER"); err != nil {
		t.Fatal(err)
	}
	dbm, err := svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "orders", ArchiveWindowDays: window})
	if err != nil {
		t.Fatal(err)
	}
	return svc, gfs, cl, dbm
}

// seedBackupAt 在指定日期（精确 UTC 日）为 hostIP 上报一份双卷备份。
func seedBackupAt(t *testing.T, svc *Service, gfs *sim.GFS, hostIP, dbName string, date time.Time) {
	t.Helper()
	d := date.UTC().Truncate(24 * time.Hour)
	city, dr := sim.BuildPaths("", hostIP, dbName, d.Format("2006-01-02"), string(model.EditionSameCity))
	gfs.WriteFull(city, 64*1024*1024)
	gfs.WriteFull(dr, 64*1024*1024)
	if _, err := svc.ReportBackupMeta(hostIP, dbName, d, model.EditionSameCity, city, dr, 0); err != nil {
		t.Fatalf("seed %s %s 失败: %v", dbName, d.Format("2006-01-02"), err)
	}
}

// TestRollingWindowBoundary 验证窗口边界：备份日期恰为 cutoff-窗口天数 → 已过窗纳入；
// 差一天未满窗 → 不纳入。口径与「备份日期过了窗口才纳入」一致。
func TestRollingWindowBoundary(t *testing.T) {
	svc, gfs, _, _ := newRollingSvc(t, 3)
	cutoff := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", cutoff.AddDate(0, 0, -3)) // 恰在窗口边界 → 纳入
	seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", cutoff.AddDate(0, 0, -2)) // 未满 3 天 → 不纳入

	_, items, err := svc.GenerateRollingList(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("应仅纳入边界上的1份, got %d", len(items))
	}
	want := cutoff.AddDate(0, 0, -3)
	if !items[0].BackupDate.Equal(want) {
		t.Fatalf("纳入的应是恰过窗的备份, got %v want %v", items[0].BackupDate, want)
	}
}

// TestRollingCandidateFilter 验证候选过滤：仅 未归档/归档失败 且上传校验通过 的备份纳入；
// 校验未通过、已归档、归档中的备份一律不进入清单（每份只归档一次的去重保证）。
func TestRollingCandidateFilter(t *testing.T) {
	svc, gfs, _, _ := newRollingSvc(t, 1)
	now := time.Now()
	// 5 份过窗备份，各自改为不同状态
	for day := 1; day <= 5; day++ {
		seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", now.AddDate(0, 0, -day))
	}
	upd := func(day int, field string, val any) {
		d := now.AddDate(0, 0, -day).UTC().Truncate(24 * time.Hour)
		if err := svc.db.Model(&model.BackupMeta{}).
			Where("cluster_id = ? AND db_name = ? AND backup_date = ?", 1, "orders", d).
			Update(field, val).Error; err != nil {
			t.Fatalf("更新第%d天 %s 失败: %v", day, field, err)
		}
	}
	upd(1, "upload_verify", model.VerifyFailed)   // 校验未通过 → 不纳入
	upd(2, "archive_status", model.StatusArchived) // 已归档 → 不纳入
	upd(3, "archive_status", model.StatusArchiving) // 归档中 → 不纳入
	upd(4, "archive_status", model.StatusArchiveFailed) // 上次失败 → 纳入重试
	// 第5天保持 NOT_ARCHIVED+VERIFIED → 纳入

	_, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it.BackupDate.Format("2006-01-02")] = true
	}
	day4 := now.AddDate(0, 0, -4).UTC().Truncate(24 * time.Hour).Format("2006-01-02")
	day5 := now.AddDate(0, 0, -5).UTC().Truncate(24 * time.Hour).Format("2006-01-02")
	if len(items) != 2 || !got[day4] || !got[day5] {
		t.Fatalf("应仅纳入 失败重试(day4)+未归档(day5), got items=%v", got)
	}
}

// TestRollingDoubleTriggerNoDuplicate 验证重复触发不重复下发 + 批次号回填：
// 第二次生成清单时已抢占(ARCHIVING)的备份不再纳入；零项不建空批次；
// 每个被纳入备份的元数据回填了所属批次号。
func TestRollingDoubleTriggerNoDuplicate(t *testing.T) {
	svc, gfs, _, _ := newRollingSvc(t, 1)
	now := time.Now()
	for day := 1; day <= 3; day++ {
		seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", now.AddDate(0, 0, -day))
	}

	batchID, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("首轮应纳入3份, got %d", len(items))
	}
	// 批次号回填到元数据
	var metas []model.BackupMeta
	svc.db.Where("archive_batch_id = ?", batchID).Find(&metas)
	if len(metas) != 3 {
		t.Fatalf("应回填3条元数据的批次号, got %d", len(metas))
	}
	for _, m := range metas {
		if m.ArchiveStatus != model.StatusArchiving {
			t.Fatalf("纳入后应置 ARCHIVING, got %s", m.ArchiveStatus)
		}
	}

	// 立即重复触发：全部已抢占 → 零项、不建空批次
	batchID2, items2, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items2) != 0 || batchID2 != "" {
		t.Fatalf("重复触发应零项, got items=%d batch=%q", len(items2), batchID2)
	}
	var batchCount int64
	svc.db.Model(&model.ArchiveBatch{}).Count(&batchCount)
	if batchCount != 1 {
		t.Fatalf("零项不应新建批次, got %d 个批次", batchCount)
	}
	// 首个批次状态应为 PUSHED（未配通知出口时也置位）
	var batch model.ArchiveBatch
	svc.db.First(&batch, "id = ?", batchID)
	if batch.Status != "PUSHED" {
		t.Fatalf("批次应 PUSHED, got %s", batch.Status)
	}
}

// flipNotifier 可翻转成败的归档通知桩：fail=true 时模拟推送失败。
type flipNotifier struct {
	mu       sync.Mutex
	fail     bool
	received []string
}

func (f *flipNotifier) NotifyArchive(batchID string, items []notify.ArchiveCandidate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("模拟网络故障")
	}
	f.received = append(f.received, batchID)
	return nil
}

// TestRollingPushFailureRecovers 验证推送失败后的完整恢复闭环：
// 推送失败 → 批次 FAILED + 备份 ARCHIVE_FAILED → 下轮清单重新纳入 → 推送成功 →
// 归档回调 → ARCHIVED。整条失败-重试-成功链路端到端走通。
func TestRollingPushFailureRecovers(t *testing.T) {
	svc, gfs, _, _ := newRollingSvc(t, 1)
	now := time.Now()
	seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", now.AddDate(0, 0, -1))

	fn := &flipNotifier{fail: true}
	svc.ArchiveNotifier = fn

	// 首轮：推送失败 → 返回错误（按约定返回空 batchID），批次 FAILED，备份 ARCHIVE_FAILED
	_, _, err := svc.GenerateRollingList(now)
	if err == nil {
		t.Fatal("推送失败应返回错误")
	}
	var batch1 model.ArchiveBatch
	if err := svc.db.First(&batch1, "status = ?", "FAILED").Error; err != nil {
		t.Fatalf("推送失败应留下 FAILED 批次记录: %v", err)
	}
	var meta model.BackupMeta
	if err := svc.db.First(&meta).Error; err != nil {
		t.Fatal(err)
	}
	if meta.ArchiveStatus != model.StatusArchiveFailed {
		t.Fatalf("推送失败后备份应 ARCHIVE_FAILED, got %s", meta.ArchiveStatus)
	}

	// 次轮：故障恢复 → 失败项重新纳入 → 推送成功 → 回调 → ARCHIVED
	fn.fail = false
	batchID2, items2, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items2) != 1 || items2[0].BackupID != meta.ID {
		t.Fatalf("失败项应被次轮重新纳入, got %+v", items2)
	}
	if batchID2 == batch1.ID {
		t.Fatal("重试应产生新批次")
	}
	if err := svc.ArchiveResult(items2[0].ArchiveItemID, "/hdfs/x/0.sql.gz", 64*1024*1024); err != nil {
		t.Fatal(err)
	}
	svc.db.First(&meta, meta.ID)
	if meta.ArchiveStatus != model.StatusArchived {
		t.Fatalf("回调成功后应 ARCHIVED, got %s", meta.ArchiveStatus)
	}
	var batch2 model.ArchiveBatch
	svc.db.First(&batch2, "id = ?", batchID2)
	if batch2.Status != "PUSHED" {
		t.Fatalf("重试批次应 PUSHED, got %s", batch2.Status)
	}
}

// TestArchiveResultMismatchRetried 验证归档回调 size 不匹配的重试闭环：
// 回调 size 与清单不一致 → ARCHIVE_FAILED + 传输条目 FAILED → 下轮清单重新纳入。
func TestArchiveResultMismatchRetried(t *testing.T) {
	svc, gfs, _, _ := newRollingSvc(t, 1)
	now := time.Now()
	seedBackupAt(t, svc, gfs, "10.0.0.7", "orders", now.AddDate(0, 0, -1))

	_, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	// 回调 size 少 1 字节 → 校验不过
	if err := svc.ArchiveResult(items[0].ArchiveItemID, "/hdfs/x/0.sql.gz", 64*1024*1024-1); err != nil {
		t.Fatal(err)
	}
	var meta model.BackupMeta
	svc.db.First(&meta, items[0].BackupID)
	if meta.ArchiveStatus != model.StatusArchiveFailed {
		t.Fatalf("size 不匹配应 ARCHIVE_FAILED, got %s", meta.ArchiveStatus)
	}
	var item model.ArchiveItem
	svc.db.First(&item, items[0].ArchiveItemID)
	if item.TransferStatus != model.TransferFailed {
		t.Fatalf("传输条目应 FAILED, got %s", item.TransferStatus)
	}

	// 下轮清单应重新纳入该备份
	_, items2, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items2) != 1 || items2[0].BackupID != meta.ID {
		t.Fatalf("size 不匹配的备份应被重新纳入, got %+v", items2)
	}
}

// TestRollingConcurrentNoDuplicate 验证并发触发的抢占正确性：
// 两个并发生成清单，每份备份最多被一个批次抢占（条件 UPDATE 防重复下发）。
// 注：SQLite 测试库对并发写只串行化单事务，另一方可返回忙错误（回滚后无任何抢占）——
// 两种结局都合法，断言只保证"不重不漏"这一不变量。
func TestRollingConcurrentNoDuplicate(t *testing.T) {
	db, err := store.New("file:rolling-conc?mode=memory&cache=shared&_busy_timeout=10000")
	if err != nil {
		t.Fatal(err)
	}
	gfs := sim.NewGFS("/gfs")
	svc := New(db, &gfsclient.MemFileStore{GFS: gfs})

	cl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-CONC", ArchiveWindowDays: 1})
	svc.AddClusterInstance(cl.ID, "10.0.0.8", "a", "MASTER")
	svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "orders", ArchiveWindowDays: 1})
	now := time.Now()
	for day := 1; day <= 4; day++ {
		seedBackupAt(t, svc, gfs, "10.0.0.8", "orders", now.AddDate(0, 0, -day))
	}

	type result struct {
		batchID string
		items   []ArchiveItemFlat
		err     error
	}
	var wg sync.WaitGroup
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, it, e := svc.GenerateRollingList(now)
			results <- result{b, it, e}
		}()
	}
	wg.Wait()
	close(results)

	seen := map[uint]bool{}
	total := 0
	okCalls := 0
	for r := range results {
		if r.err != nil {
			// SQLite 忙冲突：事务回滚，无任何抢占——合法结局
			t.Logf("并发触发之一被库忙冲突回滚（合法）: %v", r.err)
			continue
		}
		okCalls++
		for _, it := range r.items {
			if seen[it.BackupID] {
				t.Fatalf("备份 %d 被两个批次同时纳入（重复下发）", it.BackupID)
			}
			seen[it.BackupID] = true
			total++
		}
	}
	if okCalls == 0 {
		t.Fatal("两次并发全部失败")
	}
	// 不漏：archive_item 恰好 total 条，被抢占备份恰为 total 份 ARCHIVING
	var itemCount int64
	db.Model(&model.ArchiveItem{}).Count(&itemCount)
	if itemCount != int64(total) {
		t.Fatalf("archive_item=%d 应等于抢占总数=%d", itemCount, total)
	}
	var claiming int64
	db.Model(&model.BackupMeta{}).Where("archive_status = ?", model.StatusArchiving).Count(&claiming)
	if claiming != int64(total) {
		t.Fatalf("ARCHIVING=%d 应等于抢占总数=%d", claiming, total)
	}
}

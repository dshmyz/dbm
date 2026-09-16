package service

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// newSvc 构造管控核心服务（GFS 用内存桩 FileStore）。
func newSvc(t *testing.T) (*Service, *sim.GFS) {
	t.Helper()
	db, err := store.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	gfs := sim.NewGFS("/gfs")
	svc := New(db, &gfsclient.MemFileStore{GFS: gfs})
	return svc, gfs
}

// TestFullLink 用纯内存桩跑通核心全链路：上报→巡检→补发→滚动清单→归档→清理。
func TestFullLink(t *testing.T) {
	gfs := sim.NewGFS("/gfs")
	db, err := store.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	svc := New(db, &gfsclient.MemFileStore{GFS: gfs})
	now := time.Now()

	// 1) 注册集群 + 绑定库 + 挂实例 IP
	cl, err := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-01", ClusterName: "演示集群", RetentionDays: model.IntPtr(0)})
	if err != nil {
		t.Fatal(err)
	}
	dbm, err := svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	hostIP := "10.0.0.1"
	if _, err := svc.AddClusterInstance(cl.ID, hostIP, "db-01", "MASTER"); err != nil {
		t.Fatal(err)
	}

	// 2) 上报 15 天双卷元数据（按 IP 反查集群）
	for i := 1; i <= 15; i++ {
		day := now.AddDate(0, 0, -i)
		city, dr := sim.BuildPaths("", hostIP, dbm.DBName, day.Format("2006-01-02"), string(model.EditionSameCity))
		_ = gfs.WriteFull(city, 64*1024*1024)
		_ = gfs.WriteFull(dr, 64*1024*1024)
		meta, err := svc.ReportBackupMeta(hostIP, dbm.DBName, day, model.EditionSameCity, city, dr, 0)
		if err != nil || meta.ArchiveStatus != model.StatusNotArchived {
			t.Fatalf("上报失败: err=%v status=%s", err, meta.ArchiveStatus)
		}
	}

	// 3) 巡检：正常应 15 槽全 OK
	ins, err := svc.RunInspect(now, 15)
	if err != nil {
		t.Fatal(err)
	}
	if ins.TotalSlots != 15 || ins.OKCount != 15 || ins.MissingCount != 0 {
		t.Fatalf("巡检异常: total=%d ok=%d missing=%d", ins.TotalSlots, ins.OKCount, ins.MissingCount)
	}

	// 4) 制造缺失：删除 2 天前异地卷并重新巡检 → 应发现 1 条缺失
	missDay := now.AddDate(0, 0, -2)
	_, drPath := sim.BuildPaths("", hostIP, dbm.DBName, missDay.Format("2006-01-02"), string(model.EditionSameCity))
	gfs.Delete(drPath)
	ins2, err := svc.RunInspect(now, 15)
	if err != nil {
		t.Fatal(err)
	}
	if ins2.MissingCount != 1 {
		t.Fatalf("应发现1条缺失, got %d", ins2.MissingCount)
	}

	// 5) 补发：下发任务并成功回调
	task, err := svc.CreateReplenishTask(cl.ID, dbm.ID, dbm.DBName, missDay, model.EditionSameCity)
	if err != nil {
		t.Fatal(err)
	}
	_, drPath2 := sim.BuildPaths("", hostIP, dbm.DBName, missDay.Format("2006-01-02"), string(model.EditionSameCity))
	_ = gfs.WriteFull(drPath2, 64*1024*1024) // 重新补齐从卷
	if err := svc.CompleteReplenish(task.TaskID, true, drPath2, 64*1024*1024); err != nil {
		t.Fatal(err)
	}

	// 6) 滚动清单：cutoff=今天，按每库自身的归档窗口（该集群默认15）取已过窗的备份
	batchID, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 {
		t.Fatal("滚动清单不应为空")
	}
	if err := svc.ArchiveResult(items[0].ArchiveItemID, "/hdfs/archive/"+batchID+"/0.sql.gz", 64*1024*1024); err != nil {
		t.Fatal(err)
	}

	// 7) 清理：已归档项可从 GFS 删除
	n, err := svc.Cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatal("应清理至少1份已归档备份")
	}
}

// TestClusterIPAssociation 覆盖「集群 ↔ 实例 IP」关联：反查、未绑定报错、多IP、
// IP唯一冲突、非法IP。
func TestClusterIPAssociation(t *testing.T) {
	svc, _ := newSvc(t)

	cl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-A"})
	clB, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-B"})

	// 多 IP → 同一集群
	if _, err := svc.AddClusterInstance(cl.ID, "10.0.0.10", "a-1", "MASTER"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddClusterInstance(cl.ID, "10.0.0.11", "a-2", "REPLICA"); err != nil {
		t.Fatal(err)
	}

	// IP 反查正确命中所属集群（多 IP 均命中）
	for _, ip := range []string{"10.0.0.10", "10.0.0.11"} {
		got, err := svc.ResolveClusterByIP(ip)
		if err != nil || got.ID != cl.ID {
			t.Fatalf("IP %s 应命中集群CL-A: got=%+v err=%v", ip, got, err)
		}
	}

	// 未绑定 IP 应报错
	if _, err := svc.ResolveClusterByIP("10.9.9.9"); err == nil {
		t.Fatal("未绑定的 IP 应报错")
	}

	// 一个 IP 只能属于一个集群：重复绑定到另一集群应失败
	if _, err := svc.AddClusterInstance(clB.ID, "10.0.0.10", "b-1", "MASTER"); err == nil {
		t.Fatal("同一 IP 挂到另一集群应失败")
	}

	// 非法 IP 应被拒绝
	if _, err := svc.AddClusterInstance(clB.ID, "not-an-ip", "x", "MASTER"); err == nil {
		t.Fatal("非法 IP 应被拒绝")
	}
}

// TestPathTemplate 覆盖 GFS 路径模板：生成、反向解析、库/集群/全局回退。
func TestPathTemplate(t *testing.T) {
	svc, _ := newSvc(t)

	clNoTmpl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-DEFAULT"})
	clTmpl, _ := svc.RegisterCluster(&model.Cluster{
		ClusterCode: "CL-CUSTOM", PathTemplate: "{root}/c/{cluster}/{db}/{date}/{vol}.bak",
	})
	dbClT, _ := svc.BindDB(&model.DB{ClusterID: clTmpl.ID, DBName: "db1"}) // 用集群模板
	dbDbT, _ := svc.BindDB(&model.DB{                                      // 用库级模板
		ClusterID: clTmpl.ID, DBName: "db2",
		PathTemplate: "{root}/prod/{cluster}/{db}/{date}/{vol}/f.sql.gz",
	})

	// 回退：库/集群未配 → 全局默认
	city, dr := svc.BuildPathForDB(&model.DB{DBName: "x"}, clNoTmpl, "10.0.0.1", "2026-08-01", model.EditionSameCity)
	if want := "/gfs/10.0.0.1/x/2026-08-01/city/full.sql.gz"; city != want {
		t.Errorf("默认模板生成错误: got=%s want=%s", city, want)
	}
	_ = dr

	// 集群级模板（{cluster} 由 hostIP 标识）
	cc, cd := svc.BuildPathForDB(dbClT, clTmpl, "10.0.0.2", "2026-08-01", model.EditionSameCity)
	if cc != "/gfs/c/10.0.0.2/db1/2026-08-01/city.bak" || cd != "/gfs/c/10.0.0.2/db1/2026-08-01/dr.bak" {
		t.Errorf("集群模板生成错误: city=%s dr=%s", cc, cd)
	}

	// 库级模板优先于集群
	qc, qd := svc.BuildPathForDB(dbDbT, clTmpl, "10.0.0.2", "2026-08-01", model.EditionSameCity)
	if qc != "/gfs/prod/10.0.0.2/db2/2026-08-01/city/f.sql.gz" || qd != "/gfs/prod/10.0.0.2/db2/2026-08-01/dr/f.sql.gz" {
		t.Errorf("库级模板生成错误: city=%s dr=%s", qc, qd)
	}

	// 反向解析：从库级模板生成的城市卷路径反解回字段
	f, ok := sim.ParsePath(dbDbT.PathTemplate, qc)
	if !ok || f.Cluster != "10.0.0.2" || f.DB != "db2" || f.Date != "2026-08-01" || f.Vol != "city" {
		t.Errorf("反向解析失败: %+v ok=%v", f, ok)
	}
	// 反向解析默认模板
	fd, ok := sim.ParsePath("", "/gfs/10.0.0.1/orders/2026-08-01/city/full.sql.gz")
	if !ok || fd.Cluster != "10.0.0.1" || fd.DB != "orders" || fd.Date != "2026-08-01" || fd.Vol != "city" {
		t.Errorf("默认模板反向解析失败: %+v ok=%v", fd, ok)
	}
}

// seedDay 在 svc 中为 hostIP 下的某个库上报第 n 天前的双卷备份。
// gfs 为同实例 GFS（DB 侧写双卷，不归管控）；svc 经其 FileStore 访问。
func seedDay(t *testing.T, svc *Service, gfs *sim.GFS, hostIP, dbName string, day int, now time.Time) {
	t.Helper()
	d := now.AddDate(0, 0, -day)
	city, dr := sim.BuildPaths("", hostIP, dbName, d.Format("2006-01-02"), string(model.EditionSameCity))
	gfs.WriteFull(city, 64*1024*1024)
	gfs.WriteFull(dr, 64*1024*1024)
	if _, err := svc.ReportBackupMeta(hostIP, dbName, d, model.EditionSameCity, city, dr, 0); err != nil {
		t.Fatalf("上报失败(%s): %v", dbName, err)
	}
}

// TestRollingPerDBWindow 验证滚动清单按「每个库自身的归档窗口」纳入：
// 同一备份日期，窗口小的库纳入、窗口大的库不纳入。
func TestRollingPerDBWindow(t *testing.T) {
	svc, gfs := newSvc(t)
	now := time.Now()

	cl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-WIN"})
	svc.AddClusterInstance(cl.ID, "10.0.0.1", "a", "MASTER")
	// 库 A 窗口 1 天，库 B 窗口 3 天
	svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "dbA", ArchiveWindowDays: 1})
	svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "dbB", ArchiveWindowDays: 3})

	for day := 1; day <= 3; day++ {
		seedDay(t, svc, gfs, "10.0.0.1", "dbA", day, now)
		seedDay(t, svc, gfs, "10.0.0.1", "dbB", day, now)
	}

	_, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatal(err)
	}
	// 期望：dbA(窗1) 3份全过窗；dbB(窗3) 仅第3天过窗。合计 4 份。
	// 键统一用「UTC 截断日」——与产品存储的 backup_date 一致，避免本地时区跨日偏差。
	got := map[string]int{}
	for _, it := range items {
		got[it.DBName+"/"+it.BackupDate.Format("2006-01-02")]++
	}
	dayDate := func(day int) string {
		return now.AddDate(0, 0, -day).UTC().Truncate(24 * time.Hour).Format("2006-01-02")
	}
	t.Logf("rolling items: %v", got)
	if len(items) != 4 {
		t.Fatalf("应纳入4份, got %d: %v", len(items), got)
	}
	for _, d := range []int{1, 2, 3} {
		key := "dbA/" + dayDate(d)
		if got[key] != 1 {
			t.Errorf("dbA 第%d天应过窗, got %d", d, got[key])
		}
	}
	// 同一日期第2天：dbA 纳入必须、dbB 不纳入
	if got["dbB/"+dayDate(2)] != 0 {
		t.Error("dbB 第2天(未满窗3)不应纳入")
	}
	if got["dbB/"+dayDate(3)] != 1 {
		t.Error("dbB 第3天(满窗3)应纳入")
	}
}

// archiveOne 新建独立(内存库,集群,库)，上报昨日备份→归档为 ARCHIVED，返回 svc 与该 meta 主键。
func archiveOne(t *testing.T, retention *int) (*Service, uint) {
	t.Helper()
	gfs := sim.NewGFS("/gfs")
	db, _ := store.New(":memory:")
	svc := New(db, &gfsclient.MemFileStore{GFS: gfs})
	now := time.Now()
	cl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL", RetentionDays: retention})
	svc.AddClusterInstance(cl.ID, "10.0.0.9", "a", "MASTER")
	svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "dbX", ArchiveWindowDays: 1})
	seedDay(t, svc, gfs, "10.0.0.9", "dbX", 1, now)
	bid, items, _ := svc.GenerateRollingList(now)
	if len(items) != 1 {
		t.Fatalf("应1项: %d", len(items))
	}
	if err := svc.ArchiveResult(items[0].ArchiveItemID, "/hdfs/a/"+bid+"/0.sql.gz", 64*1024*1024); err != nil {
		t.Fatal(err)
	}
	return svc, items[0].BackupID
}

// TestRetentionEffective 验证 GFS 留存天数真正生效：归档后未满留存不清、满留存才清，
// QueryArchiveStatus 的 canDelete 按留存如实计算；显式 0 = 立即清。
func TestRetentionEffective(t *testing.T) {
	now := time.Now()

	// 留存 2 天
	svcR, metaID := archiveOne(t, model.IntPtr(2))

	// 未满留存：可删判定应 false，清理 0
	st, canDel, _, pending, err := svcR.QueryArchiveStatus("CL", "dbX", now.AddDate(0, 0, -1), model.EditionSameCity)
	if err != nil || st != string(model.StatusArchived) {
		t.Fatalf("状态异常: st=%s err=%v", st, err)
	}
	if canDel {
		t.Error("未满留存2天不应可删")
	}
	if pending == nil {
		t.Error("pendingDeleteAt 应为非空(archivedAt+retention)")
	}
	n, _ := svcR.Cleanup()
	if n != 0 {
		t.Fatalf("未满留存不应清理, got %d", n)
	}

	// 回拨 ArchivedAt 到 3 天前 → 已满留存 2 天 → 清理 + 可删
	var meta model.BackupMeta
	if err := svcR.db.First(&meta, metaID).Error; err != nil {
		t.Fatal(err)
	}
	old := now.AddDate(0, 0, -3)
	meta.ArchivedAt = &old
	svcR.db.Save(&meta)

	_, canDel2, _, _, _ := svcR.QueryArchiveStatus("CL", "dbX", now.AddDate(0, 0, -1), model.EditionSameCity)
	if !canDel2 {
		t.Error("已满留存2天应可删")
	}
	n2, _ := svcR.Cleanup()
	if n2 != 1 {
		t.Fatalf("已满留存应清理1份, got %d", n2)
	}

	// 显式留存 0：归档后立即清理（独立库）
	svcZ, _ := archiveOne(t, model.IntPtr(0))
	n3, _ := svcZ.Cleanup()
	if n3 != 1 {
		t.Fatalf("留存0应归档后立即清理, got %d", n3)
	}
}

// TestCleanupMode 验证清理触发方可配置：
//   - mgr（现状）：管控遍历已归档、满留存的备份经 fs.Delete 实际删 GFS 文件；
//   - gfs（原始设想）：管控仅做放行判定，不实际删除，满足条件的文件仍留在 GFS（由外部 GFS 自治清理）。
func TestCleanupMode(t *testing.T) {
	// gfs 模式：归档满留存（留存0即满）但管控不删
	svcG, _ := archiveOne(t, model.IntPtr(0))
	svcG.CleanupMode = "gfs"
	var metaG model.BackupMeta
	if err := svcG.db.Take(&metaG).Error; err != nil {
		t.Fatal(err)
	}
	if !svcG.fs.Exists(metaG.GFSPath) {
		t.Fatal("清理前 GFS 应存在文件")
	}
	if n, _ := svcG.Cleanup(); n != 0 {
		t.Fatalf("gfs 模式管控不应删文件, got %d", n)
	}
	if !svcG.fs.Exists(metaG.GFSPath) {
		t.Fatal("gfs 模式管控不应删除 GFS 文件（交外部 GFS 自治）")
	}

	// mgr 模式：管控实际删除
	svcM, _ := archiveOne(t, model.IntPtr(0))
	svcM.CleanupMode = "mgr"
	var metaM model.BackupMeta
	if err := svcM.db.Take(&metaM).Error; err != nil {
		t.Fatal(err)
	}
	if n, _ := svcM.Cleanup(); n != 1 {
		t.Fatalf("mgr 模式应清理1份, got %d", n)
	}
	if svcM.fs.Exists(metaM.GFSPath) {
		t.Fatal("mgr 模式清理后 GFS 文件应被删除")
	}
}

// TestDBRegionEndpoints 验证「机房端点界面配置」：tb_gfs_endpoint 库表配置
// 优先于静态映射，机房路由即时生效（写后失效缓存），未匹配机房回落 "*" 兜底。
func TestDBRegionEndpoints(t *testing.T) {
	gfsA := sim.NewGFS("/gfs") // 杭州
	srvA := httptest.NewServer(gfsA.Handler())
	defer srvA.Close()
	gfsB := sim.NewGFS("/gfs") // 乌兰察布
	srvB := httptest.NewServer(gfsB.Handler())
	defer srvB.Close()

	svc, _ := newSvc(t)
	// 未配置任何端点 → 单地址模式（回落默认访问面）
	if src := svc.regionSource(); src != nil {
		t.Fatalf("未配置端点时 regionSource 应为 nil, got %T", src)
	}

	// 配置两个机房端点（A=杭州，B=乌兰察布，且 B 同时为 "*" 兜底）
	if _, err := svc.CreateGfsEndpoint(&model.GfsEndpoint{Region: "杭州", BaseURL: srvA.URL}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateGfsEndpoint(&model.GfsEndpoint{Region: "乌兰察布", BaseURL: srvB.URL}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateGfsEndpoint(&model.GfsEndpoint{Region: "*", BaseURL: srvB.URL}); err != nil {
		t.Fatal(err)
	}

	src := svc.regionSource()
	if src == nil {
		t.Fatal("配置端点后 regionSource 不应为 nil")
	}
	if fs, ok := src.For("杭州").(*gfsclient.HTTPFileStore); !ok || fs.Base != srvA.URL {
		t.Fatalf("杭州 应路由到 A, got %v", src.For("杭州"))
	}
	if fs, ok := src.For("乌兰察布").(*gfsclient.HTTPFileStore); !ok || fs.Base != srvB.URL {
		t.Fatalf("乌兰察布 应路由到 B, got %v", src.For("乌兰察布"))
	}
	if fs, ok := src.For("不存在的机房").(*gfsclient.HTTPFileStore); !ok || fs.Base != srvB.URL {
		t.Fatal("未匹配机房应回落 * 兜底")
	}

	// 重复 region 应报错
	if _, err := svc.CreateGfsEndpoint(&model.GfsEndpoint{Region: "杭州", BaseURL: srvA.URL}); err == nil {
		t.Fatal("重复机房应报错")
	}

	// 更新杭州端点指向 B（写后缓存失效 → 立即生效）
	eps, _ := svc.ListGfsEndpoints()
	var hzID uint
	for _, e := range eps {
		if e.Region == "杭州" {
			hzID = e.ID
		}
	}
	if _, err := svc.UpdateGfsEndpoint(hzID, &model.GfsEndpoint{BaseURL: srvB.URL}); err != nil {
		t.Fatal(err)
	}
	src = svc.regionSource()
	if fs, ok := src.For("杭州").(*gfsclient.HTTPFileStore); !ok || fs.Base != srvB.URL {
		t.Fatal("更新后杭州应路由到 B（缓存已失效）")
	}

	// 删除兜底 → 未匹配机房回落 nil → volStore 回落默认访问面
	var starID uint
	eps, _ = svc.ListGfsEndpoints()
	for _, e := range eps {
		if e.Region == "*" {
			starID = e.ID
		}
	}
	if err := svc.DeleteGfsEndpoint(starID); err != nil {
		t.Fatal(err)
	}
	if fs := svc.regionSource().For("不存在的机房"); fs != nil {
		t.Fatalf("无兜底时未匹配机房应回落 nil, got %T", fs)
	}
}

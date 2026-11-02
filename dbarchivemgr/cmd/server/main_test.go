package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/bigdata"
	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// TestBootstrapExternalWiring 验证「外部 GFS 模式」：
// 通过 DBAM_GFS_MAP（含 "*" 兜底）配置管理端后，管控不起内置桩、不注入种子，
// 出站（stat/delete + 归档清单 + 补发指令）按机房路由打管理端；管控不直连大数据。
func TestBootstrapExternalWiring(t *testing.T) {
	// 外部 GFS 网关（/v1/fs/* 为 GFS 管理端；/v1/archive、/v1/replenish 转发大数据桩——模拟管理端转发）
	gfs := sim.NewGFS("/gfs")
	bd := bigdata.New(gfs, sim.NewHDFS())
	gfsGw := &combinedHandler{gfs: gfs.Handler(), bd: bd.Handler()}
	gfsSrv := httptest.NewServer(gfsGw)
	defer gfsSrv.Close()

	db, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	// 外部模式：DBAM_GFS_MAP 配 "*" 兜底（等价于界面「机房管理」配一条 * 行）
	c := bootstrap("http://dbam:8080", "mgr", map[string]string{"*": gfsSrv.URL}, db)
	defer c.closeStubs()

	if !c.usesExternalGFS {
		t.Fatal("期望 usesExternalGFS=true（已配置机房映射）")
	}
	if c.bd != nil {
		t.Fatal("外部模式不应起内置大数据桩")
	}
	if c.controlBase != "http://dbam:8080" {
		t.Fatalf("controlBase 应取 -public-url，got %q", c.controlBase)
	}
	// 未匹配/空机房一律回落 "*" 兜底管理端
	if c.svc.Regions.For("任何机房").(*gfsclient.HTTPFileStore).Base != gfsSrv.URL {
		t.Fatal("未匹配机房应回落 \"*\" 兜底管理端")
	}
	// 外部模式不注入种子
	var n int64
	if err := db.Table("backup_meta").Count(&n).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("外部模式不应注入种子数据, got %d", n)
	}
}

// TestBootstrapDemoDefaults 验证缺省（演示）模式：自动起内置桩并注入种子。
func TestBootstrapDemoDefaults(t *testing.T) {
	db, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	c := bootstrap("", "mgr", nil, db)
	defer c.closeStubs()
	if c.usesExternalGFS {
		t.Fatal("缺省模式不应 usesExternalGFS")
	}
	// 缺省模式注入了种子：应至少有一条备份元数据（demo 造了 15 份）。
	var n int64
	if err := db.Table("backup_meta").Count(&n).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if n == 0 {
		t.Fatalf("缺省模式应注入种子数据，got %d", n)
	}
}

// combinedHandler 把 GFS 与大数据两组接口挂到同一个 path 空间（模拟一个统一网关地址）。
type combinedHandler struct {
	gfs http.Handler
	bd  http.Handler
}

func (c *combinedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/fs/"):
		c.gfs.ServeHTTP(w, r)
	default: // /v1/archive/* 与 /v1/replenish/*
		c.bd.ServeHTTP(w, r)
	}
}

// TestEnvOr 验证环境变量回退逻辑。
func TestEnvOr(t *testing.T) {
	t.Setenv("DBAM_PUBLIC_URL", "http://gfs:1")
	if got := envOr("DBAM_PUBLIC_URL", "def"); got != "http://gfs:1" {
		t.Fatalf("envOr 应取环境变量, got %q", got)
	}
	t.Setenv("DBAM_PUBLIC_URL", "")
	if got := envOr("DBAM_PUBLIC_URL", "def"); got != "def" {
		t.Fatalf("envOr 为空时应回退缺省, got %q", got)
	}
}

// TestParseGFSMap 验证机房映射解析：内联 JSON / @file / 非法输入。
func TestParseGFSMap(t *testing.T) {
	m, err := parseGFSMap("")
	if err != nil || m != nil {
		t.Fatalf("空 spec 应得 nil, got %v err=%v", m, err)
	}
	m, err = parseGFSMap(`{"杭州":"http://a:1","*":"http://d:1"}`)
	if err != nil || m["杭州"] != "http://a:1" || m["*"] != "http://d:1" {
		t.Fatalf("内联 JSON 解析错误: %v err=%v", m, err)
	}
	f := t.TempDir() + "/map.json"
	os.WriteFile(f, []byte(`{"乌兰":"http://b:2"}`), 0o644)
	m, err = parseGFSMap("@" + f)
	if err != nil || m["乌兰"] != "http://b:2" {
		t.Fatalf("@file 解析错误: %v err=%v", m, err)
	}
	if _, err := parseGFSMap("not-json"); err == nil {
		t.Fatal("非法输入应报错")
	}
}

// TestBootstrapRegionRouting 验证「多机房 GFS 映射」：配置 -gfs-map 后，
// 同城卷 stat 分发到 cityRegion 机房的管理端、异地卷到 drRegion 机房的管理端，
// 双卷分属两个独立 GFS server 时上报/巡检仍能正确判定。
func TestBootstrapRegionRouting(t *testing.T) {
	// 两个机房各一个独立 GFS 管理端（数据互不可见）
	gfsHZ := sim.NewGFS("/gfs") // 杭州（同城）
	hzSrv := httptest.NewServer(gfsHZ.Handler())
	defer hzSrv.Close()
	gfsWL := sim.NewGFS("/gfs") // 乌兰察布（异地）
	wlSrv := httptest.NewServer(gfsWL.Handler())
	defer wlSrv.Close()

	db, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	c := bootstrap("", "mgr",
		map[string]string{"杭州": hzSrv.URL, "乌兰察布": wlSrv.URL}, db)
	defer c.closeStubs()

	if !c.usesExternalGFS {
		t.Fatal("配置机房映射后 usesExternalGFS 应=true")
	}
	if c.svc.Regions == nil {
		t.Fatal("配置机房映射后 svc.Regions 不应为 nil")
	}
	// 未匹配机房兜底取映射中字典序首个机房（"乌兰察布"）
	if c.svc.Regions.For("").(*gfsclient.HTTPFileStore).Base != wlSrv.URL {
		t.Fatal("未匹配机房应回落字典序首个机房的管理端")
	}

	svc := c.svc
	cl, err := svc.RegisterCluster(&model.Cluster{
		ClusterCode: "CL-R", ClusterName: "多机房集群",
		CityRegion: "杭州", DRRegion: "乌兰察布", RetentionDays: model.IntPtr(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	dbm, err := svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	hostIP := "10.9.9.9"
	if _, err := svc.AddClusterInstance(cl.ID, hostIP, "db-r-01", "MASTER"); err != nil {
		t.Fatal(err)
	}

	// 同城卷写进杭州机房、异地卷写进乌兰察布机房（同城备主卷=city）
	day := time.Now().AddDate(0, 0, -1).UTC().Truncate(24 * time.Hour)
	cityPath, drPath := sim.BuildPaths("", hostIP, dbm.DBName, day.Format("2006-01-02"), string(model.EditionSameCity))
	if err := gfsHZ.WriteFull(cityPath, 64*1024*1024); err != nil {
		t.Fatal(err)
	}
	if err := gfsWL.WriteFull(drPath, 64*1024*1024); err != nil {
		t.Fatal(err)
	}

	// 双卷分属两机房 → 上报应通过（若路由失效，任一卷 stat 不到即失败）
	meta, err := svc.ReportBackupMeta(hostIP, dbm.DBName, day, model.EditionSameCity, cityPath, drPath, 0)
	if err != nil {
		t.Fatalf("多机房双卷上报应通过: %v", err)
	}
	if meta.FileSizeBytes != 64*1024*1024 || meta.PeerFileSizeBytes != 64*1024*1024 {
		t.Fatalf("双卷 size 应分属两机房实测回填, got main=%d peer=%d", meta.FileSizeBytes, meta.PeerFileSizeBytes)
	}

	// 负例：同城卷写错机房（只写在乌兰、杭州没有）→ 上报应拒绝（元数据不落库）
	day2 := day.AddDate(0, 0, -1)
	city2, dr2 := sim.BuildPaths("", hostIP, dbm.DBName, day2.Format("2006-01-02"), string(model.EditionSameCity))
	_ = gfsWL.WriteFull(city2, 64*1024*1024) // 写错机房
	_ = gfsWL.WriteFull(dr2, 64*1024*1024)
	if _, err := svc.ReportBackupMeta(hostIP, dbm.DBName, day2, model.EditionSameCity, city2, dr2, 0); err == nil {
		t.Fatal("同城卷写错机房时应上报失败")
	}

	// 巡检按机房分发：先正常上报 day2（双卷写对机房），再删掉杭州机房的同城卷模拟缺失
	_ = gfsHZ.WriteFull(city2, 64*1024*1024)
	if _, err := svc.ReportBackupMeta(hostIP, dbm.DBName, day2, model.EditionSameCity, city2, dr2, 0); err != nil {
		t.Fatalf("day2 正常上报应通过: %v", err)
	}
	gfsHZ.Delete(city2)
	res, err := svc.RunInspect(time.Now(), 15)
	if err != nil {
		t.Fatal(err)
	}
	if res.OKCount != 1 || res.MissingCount != 1 {
		t.Fatalf("巡检应 1 OK + 1 缺失, got ok=%d missing=%d", res.OKCount, res.MissingCount)
	}
}

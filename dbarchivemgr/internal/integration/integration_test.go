// Package integration 提供跨包的 HTTP 全链路集成测试：
// 验证「管控全走 HTTP 与 GFS/大数据交互」——GFS / 大数据均为独立 httptest server，
// 管控用真实 http.Client 访问，不再内联文件操作；大数据自治拉取写 HDFS 后回调管控。
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/api"
	"github.com/dbarchivemgr/dbarchivemgr/internal/bigdata"
	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/notify"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
)

// TestHTTPFullChain 全链路 HTTP 交互验证（上报→巡检→补发→归档→清理）。
func TestHTTPFullChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, _ := store.New(":memory:")

	// 组件：GFS / 大数据各起独立 HTTP server
	gfs := sim.NewGFS("/gfs")
	gfsSrv := httptest.NewServer(gfs.Handler())
	defer gfsSrv.Close()

	bd := bigdata.New(gfs, sim.NewHDFS())
	bdSrv := httptest.NewServer(bd.Handler())
	defer bdSrv.Close()

	// 管控：经 HTTP 客户端访问 GFS/大数据
	svc := service.New(db, gfsclient.NewHTTP(gfsSrv.URL))
	svc.Notifier = notify.NewHTTP(bdSrv.URL)
	svc.ArchiveNotifier = notify.NewHTTPArchive(bdSrv.URL)

	// 管控 API server；大数据回调总线指到它
	router := gin.New()
	(&api.Handler{Svc: svc, DB: db}).Register(router)
	apiSrv := httptest.NewServer(router)
	defer apiSrv.Close()
	bd.SetControl(apiSrv.URL)

	now := time.Now()
	cl, _ := svc.RegisterCluster(&model.Cluster{ClusterCode: "CL-HTTP", RetentionDays: model.IntPtr(0)})
	svc.AddClusterInstance(cl.ID, "10.5.5.5", "a", "MASTER")
	dbm, _ := svc.BindDB(&model.DB{ClusterID: cl.ID, DBName: "pay", ArchiveWindowDays: 1})

	// 上报 3 天双卷（DB 侧直写 GFS，管控经 HTTP 复核存在）
	for i := 1; i <= 3; i++ {
		day := now.AddDate(0, 0, -i)
		city, dr := sim.BuildPaths("", "10.5.5.5", dbm.DBName, day.Format("2006-01-02"), string(model.EditionSameCity))
		_ = gfs.WriteFull(city, 64*1024*1024)
		_ = gfs.WriteFull(dr, 64*1024*1024)
		if _, err := svc.ReportBackupMeta("10.5.5.5", dbm.DBName, day, model.EditionSameCity, city, dr, 0); err != nil {
			t.Fatalf("上报失败: %v", err)
		}
	}

	// 制造缺失：删 2 天前异地卷
	missDay := now.AddDate(0, 0, -2)
	_, drPath := sim.BuildPaths("", "10.5.5.5", dbm.DBName, missDay.Format("2006-01-02"), string(model.EditionSameCity))
	gfs.Delete(drPath)

	// 巡检发现缺失 → 下发补发（指令经 HTTP 推大数据，大数据补卷后回调 SUCCESS）
	ins, _ := svc.RunInspect(now, 15)
	if ins.MissingCount != 1 {
		t.Fatalf("应发现1条缺失, got %d", ins.MissingCount)
	}
	mm := ins.MissingList[0]
	if _, err := svc.CreateReplenishTask(mm.ClusterID, mm.DBID, mm.DBName, mm.BackupDate, mm.BackupEdition); err != nil {
		t.Fatalf("下发补发失败: %v", err)
	}
	waitReplenishSuccess(t, db, mm.ClusterID, mm.DBID, mm.BackupDate, mm.BackupEdition)

	// 滚动清单 → 归档通知大数据自治拉取 → 回调 → ARCHIVED
	_, items, err := svc.GenerateRollingList(now)
	if err != nil {
		t.Fatalf("滚动清单失败: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("滚动清单不应为空")
	}
	// 轮询等待大数据回调后 archive_status=ARCHIVED
	deadline := time.Now().Add(3 * time.Second)
	archived := false
	for {
		var meta model.BackupMeta
		if db.First(&meta, items[0].BackupID).Error == nil && meta.ArchiveStatus == model.StatusArchived {
			archived = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !archived {
		t.Fatalf("大数据回调后应置 ARCHIVED, backup=%d", items[0].BackupID)
	}

	// 清理：管控经 HTTP 调 GFS 删除已归档备份
	if n, _ := svc.Cleanup(); n < 1 {
		t.Fatalf("应经 HTTP 清理至少1份, got n=%d", n)
	}
	if exists, _ := gfsclient.NewHTTP(gfsSrv.URL).Stat(items[0].GFSPath); exists {
		t.Fatal("归档后清理应已从 GFS 删除主卷，但 stat 仍存在")
	}

	// 管理后台只读列表接口：验证均可 HTTP 访问且返回合法 JSON 结构
	wantStatuses := map[string]int{
		"/api/v1/clusters":         200,
		"/api/v1/dbs":              200,
		"/api/v1/backup/meta":      200,
		"/api/v1/inspects?limit=5": 200,
		"/api/v1/replenish/tasks":  200,
		"/api/v1/archive/batches":  200,
	}
	for path, want := range wantStatuses {
		r, err := http.Get(apiSrv.URL + path)
		if err != nil {
			t.Fatalf("GET %s 请求失败: %v", path, err)
		}
		if r.StatusCode != want {
			r.Body.Close()
			t.Fatalf("GET %s 状态码=%d, want %d", path, r.StatusCode, want)
		}
		blob, _ := io.ReadAll(r.Body)
		r.Body.Close()
		var body struct {
			Code int             `json:"code"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(blob, &body); err != nil || body.Code != 0 || len(body.Data) == 0 {
			t.Fatalf("GET %s 响应异常: %s", path, blob)
		}
	}
	// 过滤断言：备份元数据按集群+状态过滤应返回数据
	r, err := http.Get(apiSrv.URL + "/api/v1/backup/meta?archiveStatus=" + string(model.StatusArchived))
	if err != nil {
		t.Fatal(err)
	}
	blob, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if len(blob) == 0 {
		t.Fatal("按归档状态过滤备份元数据返回为空")
	}
}

// waitReplenishSuccess 轮询补发任务状态直到 SUCCESS（大数据回调后置位）。
func waitReplenishSuccess(t *testing.T, db *gorm.DB, clusterID, dbID uint, date time.Time, edition model.BackupEdition) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var tsk model.ReplenishTask
		if db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ?",
			clusterID, dbID, date, edition).First(&tsk).Error == nil && tsk.Status == "SUCCESS" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("补发任务未在超时内被大数据回调为 SUCCESS")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestCSVImportAndCRUD 验证集群 CSV 批量导入（幂等）+ 编辑/删除 HTTP 接口。
func TestCSVImportAndCRUD(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, _ := store.New(":memory:")
	svc := service.New(db, &gfsclient.MemFileStore{GFS: sim.NewGFS("/gfs")})
	router := gin.New()
	(&api.Handler{Svc: svc, DB: db}).Register(router)
	ts := httptest.NewServer(router)
	defer ts.Close()

	header := []string{"type", "clusterCode", "clusterName", "primaryRegion", "drRegion", "cityRegion", "archiveWindowDays", "retentionDays", "pathTemplate", "dbName", "dbEditions", "dbWindowDays", "dbArchiveTime", "dbPathTemplate", "hostIp", "hostName", "role"}
	/* ----------------------------------------------------------------------------------
	   列索引: 0=type 1=clusterCode 2=clusterName 3=primaryRegion 4=drRegion 5=cityRegion
	            6=archiveWindowDays 7=retentionDays 8=pathTemplate 9=dbName 10=dbEditions
	            11=dbWindowDays 12=dbArchiveTime 13=dbPathTemplate 14=hostIp 15=hostName 16=role
	----------------------------------------------------------------------------------- */
	row := func(vals map[int]string) string {
		fields := make([]string, len(header))
		for k, v := range vals {
			fields[k] = v
		}
		return strings.Join(fields, ",")
	}
	csv := strings.Join([]string{
		strings.Join(header, ","),
		row(map[int]string{0: "cluster", 1: "CL-IMP", 2: "导入集群", 3: "北京A", 4: "北京B", 5: "北京同城", 6: "15", 7: "0"}),
		row(map[int]string{0: "db", 1: "CL-IMP", 9: "orders", 10: "DR;SAME_CITY", 11: "7", 12: "08:00"}),
		row(map[int]string{0: "db", 1: "CL-IMP", 9: "pay", 10: "DR", 11: "3", 12: "10:00", 13: "{root}/pay/{db}/{date}/{vol}/full.sql.gz"}),
		row(map[int]string{0: "instance", 1: "CL-IMP", 14: "10.1.1.10", 15: "db-imp-01", 16: "MASTER"}),
	}, "\n")

	// 上传 CSV（multipart: file）
	res := postCSV(t, ts.URL+"/api/v1/cluster/import", csv)
	var imp struct {
		Code int `json:"code"`
		Data struct {
			Cluster  int      `json:"cluster"`
			DB       int      `json:"db"`
			Instance int      `json:"instance"`
			Errors   []string `json:"errors"`
		} `json:"data"`
	}
	json.NewDecoder(res.Body).Decode(&imp)
	res.Body.Close()
	if imp.Code != 0 || len(imp.Data.Errors) > 0 || imp.Data.Cluster != 1 || imp.Data.DB != 2 || imp.Data.Instance != 1 {
		t.Fatalf("导入汇总不符: code=%d cluster=%d db=%d inst=%d errors=%v",
			imp.Code, imp.Data.Cluster, imp.Data.DB, imp.Data.Instance, imp.Data.Errors)
	}

	// 重复导入 → 幂等覆盖，计数不增、无错误
	res = postCSV(t, ts.URL+"/api/v1/cluster/import", csv)
	json.NewDecoder(res.Body).Decode(&imp)
	res.Body.Close()
	if imp.Code != 0 || len(imp.Data.Errors) > 0 || imp.Data.Cluster != 1 || imp.Data.DB != 2 || imp.Data.Instance != 1 {
		t.Fatalf("重复导入未幂等: cluster=%d db=%d inst=%d errors=%v",
			imp.Data.Cluster, imp.Data.DB, imp.Data.Instance, imp.Data.Errors)
	}

	// 反查集群 id，并确认库/实例结构落库
	cl := getData(t, ts.URL+"/api/v1/clusters")
	var clusters []map[string]interface{}
	json.Unmarshal(cl, &clusters)
	var clID uint64
	for _, c := range clusters {
		if c["clusterCode"] == "CL-IMP" {
			clID = uint64(c["clusterId"].(float64))
		}
	}
	if clID == 0 {
		t.Fatalf("未在 clusters 列表中找到 CL-IMP")
	}
	dbs := getData(t, fmt.Sprintf("%s/api/v1/dbs?clusterId=%d", ts.URL, clID))
	var dbList []struct {
		DBID        uint   `json:"dbId"`
		DBName      string `json:"dbName"`
		Editions    string `json:"editions"`
		ArchiveTime string `json:"archiveTime"`
	}
	json.Unmarshal(dbs, &dbList)
	if len(dbList) != 2 {
		t.Fatalf("导入库数量不符: want 2 got %d", len(dbList))
	}
	insts := getData(t, fmt.Sprintf("%s/api/v1/cluster/%d/instances", ts.URL, clID))
	var instList []struct {
		ID     uint   `json:"instanceId"`
		HostIP string `json:"hostIp"`
	}
	json.Unmarshal(insts, &instList)
	if len(instList) != 1 || instList[0].HostIP != "10.1.1.10" {
		t.Fatalf("导入实例不符: %+v", instList)
	}

	// 编辑集群：改名称与窗口
	orig := getData(t, fmt.Sprintf("%s/api/v1/cluster/%d", ts.URL, clID))
	var cluster map[string]interface{}
	json.Unmarshal(orig, &cluster)
	cluster["clusterName"] = "导入集群-改名"
	cluster["archiveWindowDays"] = float64(9)
	b, _ := json.Marshal(cluster)
	req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/api/v1/cluster/%d", ts.URL, clID), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != 200 {
		t.Fatalf("PUT cluster 失败: code=%d err=%v", statusOf(res), err)
	}
	got := getData(t, fmt.Sprintf("%s/api/v1/cluster/%d", ts.URL, clID))
	var upd map[string]interface{}
	json.Unmarshal(got, &upd)
	if upd["clusterName"] != "导入集群-改名" || uint(upd["archiveWindowDays"].(float64)) != uint(9) {
		t.Fatalf("编辑未生效: %v", upd["clusterName"])
	}

	// 删除实例、库、集群（级联）
	instID := instList[0].ID
	do, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/instance/%d", ts.URL, instID), nil)
	http.DefaultClient.Do(do)
	dbID := dbList[0].DBID
	do, _ = http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/db/%d", ts.URL, dbID), nil)
	http.DefaultClient.Do(do)
	do, _ = http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/cluster/%d", ts.URL, clID), nil)
	http.DefaultClient.Do(do)

	// 删除后：集群 GET 应 404
	res404, err := http.Get(fmt.Sprintf("%s/api/v1/cluster/%d", ts.URL, clID))
	if err != nil {
		t.Fatalf("GET deleted cluster: %v", err)
	}
	defer res404.Body.Close()
	if res404.StatusCode != 404 {
		t.Fatalf("删除集群后应 404, got %d", res404.StatusCode)
	}
}

// postCSV 以 multipart 上报 CSV 文件。
func postCSV(t *testing.T, url, content string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "clusters.csv")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	fw.Write([]byte(content))
	mw.Close()
	req, _ := http.NewRequest("POST", url, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return res
}

func getJSON(t *testing.T, url string) []byte {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("GET %s: code=%d", url, res.StatusCode)
	}
	b, _ := io.ReadAll(res.Body)
	return b
}

// getData 请求并解包 {code,message,data} 包装，返回 data 的原始字节。
func getData(t *testing.T, url string) json.RawMessage {
	t.Helper()
	var wrap struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(getJSON(t, url), &wrap); err != nil || wrap.Code != 0 {
		t.Fatalf("GET %s 解包失败: err=%v", url, err)
	}
	return wrap.Data
}

func statusOf(res *http.Response) int {
	if res == nil {
		return 0
	}
	return res.StatusCode
}

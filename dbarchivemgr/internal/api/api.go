// Package api 提供 Gin 的 HTTP 接口层，将 core Service 暴露为 REST 端点。
// 对应《DBArchiveManager接口设计文档》。
package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/scheduler"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
)

// Handler 聚合核心服务并绑定路由。
type Handler struct {
	Svc   *service.Service
	DB    *gorm.DB
	Sched *scheduler.Scheduler
}

// Register 注册全部路由。
// Auth 鉴权在真实环境中校验 Bearer token；此处骨架留空。
func (h *Handler) Register(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	{
		v1.POST("/cluster", h.registerCluster)
		v1.POST("/cluster/import", h.importClusterCSV) // CSV 批量导入（multipart: file）
		v1.PUT("/cluster/:id", h.updateCluster)
		v1.DELETE("/cluster/:id", h.deleteCluster)
		v1.POST("/cluster/:id/db", h.bindDB)
		v1.GET("/cluster/:id", h.getCluster)
		v1.POST("/cluster/:id/instance", h.addClusterInstance)
		v1.GET("/cluster/:id/instances", h.listClusterInstances)
		v1.PUT("/instance/:id", h.updateClusterInstance)
		v1.DELETE("/instance/:id", h.deleteClusterInstance)
		v1.PUT("/db/:id", h.updateDB)
		v1.DELETE("/db/:id", h.deleteDB)

		// 管理后台只读列表查询
		v1.GET("/clusters", h.listClusters)
		v1.GET("/dbs", h.listDBs)
		v1.GET("/backup/meta", h.listBackupMeta)
		v1.GET("/inspects", h.listInspects)
		v1.GET("/replenish/tasks", h.listReplenishTasks)
		v1.GET("/archive/batches", h.listArchiveBatches)

		v1.POST("/backup/meta/report", h.reportBackupMeta)

		v1.POST("/inspect/run", h.runInspect)
		v1.GET("/inspect/:id", h.getInspect)

		v1.POST("/replenish/task", h.createReplenish)
		v1.POST("/replenish/task/status", h.completeReplenish)

		v1.POST("/archive/rolling-list/generate", h.generateRollingList)
		v1.POST("/archive/result", h.archiveResult)
		v1.GET("/backup/archive-status", h.queryArchiveStatus)
		v1.GET("/transfer/status", h.transferStatus)

		v1.POST("/cleanup", h.cleanup)
		v1.GET("/meta/reconciliation", h.reconciliation)
		v1.GET("/schedule/status", h.scheduleStatus)

		// 多机房 GFS 管理端配置（界面「机房管理」）
		v1.GET("/gfs/endpoints", h.listGfsEndpoints)
		v1.POST("/gfs/endpoints", h.createGfsEndpoint)
		v1.PUT("/gfs/endpoints/:id", h.updateGfsEndpoint)
		v1.DELETE("/gfs/endpoints/:id", h.deleteGfsEndpoint)
	}
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "UP"}) })
}

// resp 统一响应结构。
func resp(c *gin.Context, code int, data interface{}) {
	c.JSON(code, gin.H{"code": 0, "message": "success", "data": data})
}

func respErr(c *gin.Context, httpCode int, err error) {
	c.JSON(httpCode, gin.H{"code": httpCode, "message": err.Error(), "data": nil})
}

func (h *Handler) registerCluster(c *gin.Context) {
	var in model.Cluster
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	out, err := h.Svc.RegisterCluster(&in)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"clusterId": out.ID})
}

func (h *Handler) bindDB(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in model.DB
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	in.ClusterID = uint(id)
	out, err := h.Svc.BindDB(&in)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"dbKey": out.ClusterID, "dbName": out.DBName})
}

func (h *Handler) getCluster(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	cl, err := h.Svc.GetCluster(uint(id))
	if err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, cl)
}

type instanceReq struct {
	HostIP   string `json:"hostIp"`
	HostName string `json:"hostName"`
	Role     string `json:"role"`
}

func (h *Handler) addClusterInstance(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in instanceReq
	if err := c.ShouldBindJSON(&in); err != nil || in.HostIP == "" {
		respErr(c, 400, errors.New("hostIp 必填"))
		return
	}
	inst, err := h.Svc.AddClusterInstance(uint(id), in.HostIP, in.HostName, in.Role)
	if err != nil {
		respErr(c, 422, err)
		return
	}
	resp(c, 200, gin.H{"instanceId": inst.ID, "clusterId": inst.ClusterID, "hostIp": inst.HostIP})
}

func (h *Handler) listClusterInstances(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	list, err := h.Svc.ListClusterInstances(uint(id))
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

// --- 集群配置管理（CSV 导入 / 编辑 / 删除） ---

// importClusterCSV 批量导入集群/库/实例（multipart 字段 file，最大 4MB）。
func (h *Handler) importClusterCSV(c *gin.Context) {
	fh, err := c.FormFile("file")
	if err != nil {
		respErr(c, 400, errors.New("缺少 CSV 文件字段 file"))
		return
	}
	if fh.Size > 4<<20 {
		respErr(c, 400, errors.New("CSV 文件过大（上限 4MB）"))
		return
	}
	f, err := fh.Open()
	if err != nil {
		respErr(c, 400, err)
		return
	}
	defer f.Close()
	sum, err := h.Svc.ImportClustersCSV(f)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, sum)
}

func (h *Handler) updateCluster(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in model.Cluster
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	if err := h.Svc.UpdateCluster(uint(id), &in); err != nil {
		respErr(c, 409, err)
		return
	}
	resp(c, 200, gin.H{"updated": true})
}

func (h *Handler) deleteCluster(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if err := h.Svc.DeleteCluster(uint(id)); err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, gin.H{"deleted": true})
}

func (h *Handler) updateDB(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in model.DB
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	if err := h.Svc.UpdateDB(uint(id), &in); err != nil {
		respErr(c, 409, err)
		return
	}
	resp(c, 200, gin.H{"updated": true})
}

func (h *Handler) deleteDB(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if err := h.Svc.DeleteDB(uint(id)); err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, gin.H{"deleted": true})
}

func (h *Handler) updateClusterInstance(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in instanceReq
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	if err := h.Svc.UpdateClusterInstance(uint(id), &model.ClusterInstance{
		HostIP: in.HostIP, HostName: in.HostName, Role: in.Role,
	}); err != nil {
		respErr(c, 409, err)
		return
	}
	resp(c, 200, gin.H{"updated": true})
}

func (h *Handler) deleteClusterInstance(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if err := h.Svc.DeleteClusterInstance(uint(id)); err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, gin.H{"deleted": true})
}

// --- 管理后台只读列表查询 ---

func (h *Handler) listClusters(c *gin.Context) {
	list, err := h.Svc.ListClusters()
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

func (h *Handler) listDBs(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Query("clusterId"), 10, 64)
	list, err := h.Svc.ListDBs(uint(id))
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

func (h *Handler) listBackupMeta(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Query("clusterId"), 10, 64)
	list, err := h.Svc.ListBackupMeta(uint(id), c.Query("dbName"), c.Query("archiveStatus"))
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

func (h *Handler) listInspects(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	list, err := h.Svc.ListInspects(limit)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

func (h *Handler) listReplenishTasks(c *gin.Context) {
	list, err := h.Svc.ListReplenishTasks()
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

func (h *Handler) listArchiveBatches(c *gin.Context) {
	list, err := h.Svc.ListArchiveBatches()
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, list)
}

type reportReq struct {
	HostIP      string `json:"hostIp"`
	DBName      string `json:"dbName"`
	BackupDate  string `json:"backupDate"` // YYYY-MM-DD
	Edition     string `json:"backupEdition"`
	GFSPath     string `json:"gfsPath"`
	PeerGFSPath string `json:"peerGfsPath"`
}

func (h *Handler) reportBackupMeta(c *gin.Context) {
	var in reportReq
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	date, err := time.Parse("2006-01-02", in.BackupDate)
	if err != nil {
		respErr(c, 400, err)
		return
	}
	meta, err := h.Svc.ReportBackupMeta(in.HostIP, in.DBName, date,
		model.BackupEdition(in.Edition), in.GFSPath, in.PeerGFSPath, 0)
	if err != nil {
		respErr(c, 422, err)
		return
	}
	// 从 GFS 实测回填 size
	_, sz := h.Svc.GfsStatSize(meta.ClusterID, model.BackupEdition(in.Edition), in.GFSPath)
	resp(c, 200, gin.H{"backupId": meta.ID, "clusterId": meta.ClusterID, "hostIp": meta.HostIP,
		"archiveStatus": meta.ArchiveStatus, "fileSizeBytes": sz})
}

func (h *Handler) runInspect(c *gin.Context) {
	var in struct {
		TargetDate string `json:"targetDate"`
		WindowDays int    `json:"windowDays"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	target, _ := time.Parse("2006-01-02", in.TargetDate)
	if in.WindowDays <= 0 {
		in.WindowDays = 30
	}
	res, err := h.Svc.RunInspect(target, in.WindowDays)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, res)
}

func (h *Handler) getInspect(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var ins model.Inspect
	if err := h.DB.First(&ins, id).Error; err != nil {
		respErr(c, 404, err)
		return
	}
	var missing []model.InspectMissing
	h.DB.Where("inspect_id = ?", id).Find(&missing)
	resp(c, 200, gin.H{"inspect": ins, "missing": missing})
}

type replenishReq struct {
	ClusterID  uint   `json:"clusterId"`
	DBID       uint   `json:"dbId"`
	DBName     string `json:"dbName"`
	BackupDate string `json:"backupDate"`
	Edition    string `json:"backupEdition"`
}

func (h *Handler) createReplenish(c *gin.Context) {
	var in replenishReq
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	date, _ := time.Parse("2006-01-02", in.BackupDate)
	t, err := h.Svc.CreateReplenishTask(in.ClusterID, in.DBID, in.DBName, date, model.BackupEdition(in.Edition))
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"taskId": t.TaskID, "status": t.Status})
}

func (h *Handler) completeReplenish(c *gin.Context) {
	var in struct {
		TaskID   string `json:"taskId"`
		Status   string `json:"status"`
		GFSPath  string `json:"gfsPath"`
		FileSize int64  `json:"fileSizeBytes"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	err := h.Svc.CompleteReplenish(in.TaskID, in.Status == "SUCCESS", in.GFSPath, in.FileSize)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"ok": true})
}

func (h *Handler) generateRollingList(c *gin.Context) {
	var in struct {
		CutoffDate string `json:"cutoffDate"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	cutoff, _ := time.Parse("2006-01-02", in.CutoffDate)
	batchID, items, err := h.Svc.GenerateRollingList(cutoff)
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"archiveBatchId": batchID, "items": items})
}

// archiveResult 归档结果上报（大数据组件回调）。大数据自治写 HDFS 后携带
// transferStatus + hdfsPath + hdfsSizeBytes 回调；管控仅核对 size 置 ARCHIVED。
func (h *Handler) archiveResult(c *gin.Context) {
	var in struct {
		ArchiveItemID  uint   `json:"archiveItemId"`
		TransferStatus string `json:"transferStatus"`
		HDFSPath       string `json:"hdfsPath"`
		HDFSSizeBytes  int64  `json:"hdfsSizeBytes"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	if err := h.Svc.ArchiveResult(in.ArchiveItemID, in.HDFSPath, in.HDFSSizeBytes); err != nil {
		respErr(c, 422, err)
		return
	}
	resp(c, 200, gin.H{"ok": true})
}

func (h *Handler) queryArchiveStatus(c *gin.Context) {
	cluster := c.Query("clusterCode")
	dbName := c.Query("dbName")
	dateStr := c.Query("backupDate")
	edition := c.Query("backupEdition")
	date, _ := time.Parse("2006-01-02", dateStr)
	status, canDelete, archivedAt, pending, err := h.Svc.QueryArchiveStatus(cluster, dbName, date, model.BackupEdition(edition))
	if err != nil {
		respErr(c, 404, err)
		return
	}
	pendingStr := ""
	if pending != nil {
		pendingStr = pending.Format("2006-01-02")
	}
	resp(c, 200, gin.H{
		"archiveStatus": status, "canDelete": canDelete,
		"archivedAt": archivedAt, "pendingDeleteAt": pendingStr,
	})
}

func (h *Handler) transferStatus(c *gin.Context) {
	itemID, _ := strconv.ParseUint(c.Query("archiveItemId"), 10, 64)
	var it model.ArchiveItem
	if err := h.DB.First(&it, itemID).Error; err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, gin.H{"archiveItemId": it.ID, "transferStatus": it.TransferStatus})
}

func (h *Handler) cleanup(c *gin.Context) {
	n, err := h.Svc.Cleanup()
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"deleted": n})
}

func (h *Handler) reconciliation(c *gin.Context) {
	var metas []model.BackupMeta
	h.DB.Find(&metas)
	archived, not, failed := 0, 0, 0
	for _, m := range metas {
		switch m.ArchiveStatus {
		case model.StatusArchived:
			archived++
		case model.StatusNotArchived:
			not++
		case model.StatusArchiveFailed:
			failed++
		}
	}
	resp(c, 200, gin.H{
		"total": len(metas), "archived": archived, "notArchived": not, "archiveFailed": failed,
	})
}

// scheduleStatus 查询定时调度状态（各任务 cron 规格 + 最近运行摘要）。
func (h *Handler) scheduleStatus(c *gin.Context) {
	if h.Sched == nil {
		respErr(c, 404, errors.New("调度器未启用"))
		return
	}
	resp(c, 200, h.Sched.Snapshot())
}

// --- 多机房 GFS 管理端配置（tb_gfs_endpoint，界面「机房管理」） ---

func (h *Handler) listGfsEndpoints(c *gin.Context) {
	eps, err := h.Svc.ListGfsEndpoints()
	if err != nil {
		respErr(c, 500, err)
		return
	}
	resp(c, 200, gin.H{"endpoints": eps})
}

func (h *Handler) createGfsEndpoint(c *gin.Context) {
	var in model.GfsEndpoint
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	e, err := h.Svc.CreateGfsEndpoint(&in)
	if err != nil {
		respErr(c, 409, err)
		return
	}
	resp(c, 200, e)
}

func (h *Handler) updateGfsEndpoint(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var in model.GfsEndpoint
	if err := c.ShouldBindJSON(&in); err != nil {
		respErr(c, 400, err)
		return
	}
	e, err := h.Svc.UpdateGfsEndpoint(uint(id), &in)
	if err != nil {
		respErr(c, 409, err)
		return
	}
	resp(c, 200, e)
}

func (h *Handler) deleteGfsEndpoint(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if err := h.Svc.DeleteGfsEndpoint(uint(id)); err != nil {
		respErr(c, 404, err)
		return
	}
	resp(c, 200, gin.H{"deleted": id})
}

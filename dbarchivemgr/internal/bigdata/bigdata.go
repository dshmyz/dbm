// Package bigdata 提供大数据归档组件的 HTTP server 桩。
//
// 大数据是独立组件：管控只下发归档清单与补发指令，大数据**自治**从 GFS 拉取文件、
// 写 HDFS，再回调管控的接口上报结果。管控不再自己读 GFS / 写 HDFS。
//
// 组件接口：
//   - POST /v1/archive/notify   管控下发归档清单，大数据自治拉取→写HDFS→回调管控 /api/v1/archive/result
//   - POST /v1/replenish/notify 管控下发补发指令，大数据在 GFS 重写缺失卷→回调管控 /api/v1/replenish/task/status
package bigdata

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
)

// BigData 内存模拟大数据组件。
type BigData struct {
	gfs  *sim.GFS  // 从 GFS 拉取文件的载体（真实为 GlusterFS 挂载目录）
	hdfs *sim.HDFS // 大数据自己的 HDFS 归档存储
	log  *log.Logger

	// ControlBase 管控系统地址，用于回调归档结果/补发状态。
	ControlBase string
}

// New 构造大数据组件桩。gfs 为共享的 GFS 实例（写卷用），hdfs 归大数据所有。
func New(gfs *sim.GFS, hdfs *sim.HDFS) *BigData {
	return &BigData{
		gfs: gfs, hdfs: hdfs,
		log: log.New(log.Writer(), "[bigdata] ", log.LstdFlags),
	}
}

// SetControl 设置管控系统回调地址（回调 /api/v1/archive/result、/api/v1/replenish/task/status）。
func (b *BigData) SetControl(base string) { b.ControlBase = base }

// Handler 暴露大数据组件 HTTP 接口。
func (b *BigData) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/archive/notify", b.handleArchiveNotify)
	mux.HandleFunc("/v1/replenish/notify", b.handleReplenishNotify)
	return mux
}

// archiveItem 归档清单条目（来自管控下发的 notify）。
type archiveItem struct {
	ArchiveItemID uint   `json:"archiveItemId"`
	BackupID      uint   `json:"backupId"`
	GFSPath       string `json:"gfsPath"`
	FileSizeBytes int64  `json:"fileSizeBytes"`
	HDFSPath      string `json:"hdfsPath"` // 大数据自定归档目标路径
}

func (b *BigData) handleArchiveNotify(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ArchiveBatchID string        `json:"archiveBatchId"`
		Items          []archiveItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.log.Printf("收到归档清单 batch=%s items=%d，开始自治拉取", in.ArchiveBatchID, len(in.Items))
	for i := range in.Items {
		it := &in.Items[i]
		b.archiveOne(it)
	}
	w.WriteHeader(http.StatusNoContent)
}

// archiveOne 大数据自治归档单条：读 GFS 卷 → 写 HDFS → 回调管控 /api/v1/archive/result。
func (b *BigData) archiveOne(it *archiveItem) {
	data, ok := b.gfs.Read(it.GFSPath)
	if !ok {
		b.log.Printf("归档失败: GFS 源不存在 %s", it.GFSPath)
		b.callbackArchiveResult(it.ArchiveItemID, "TRANSFER_FAILED", "", 0)
		return
	}
	hdfsPath := it.HDFSPath
	if hdfsPath == "" {
		hdfsPath = "/hdfs/archive/" + it.GFSPath
	}
	if err := b.hdfs.Write(hdfsPath, data); err != nil {
		b.log.Printf("归档失败: 写 HDFS %s: %v", hdfsPath, err)
		b.callbackArchiveResult(it.ArchiveItemID, "TRANSFER_FAILED", "", 0)
		return
	}
	hf, ok := b.hdfs.Stat(hdfsPath)
	if !ok || len(data) == 0 {
		b.log.Printf("归档失败: HDFS 副本存在校验失败 %s", hdfsPath)
		b.callbackArchiveResult(it.ArchiveItemID, "TRANSFER_FAILED", hdfsPath, 0)
		return
	}
	b.log.Printf("归档完成: %s → %s size=%d", it.GFSPath, hdfsPath, hf.Size)
	b.callbackArchiveResult(it.ArchiveItemID, "TRANSFER_DONE", hdfsPath, hf.Size)
}

// callbackArchiveResult 回调管控 /api/v1/archive/result。
func (b *BigData) callbackArchiveResult(itemID uint, transferStatus, hdfsPath string, hdfsSize int64) {
	payload := map[string]any{
		"archiveItemId":  itemID,
		"transferStatus": transferStatus,
		"hdfsPath":       hdfsPath,
		"hdfsSizeBytes":  hdfsSize,
		"archivedAt":     time.Now().Format(time.RFC3339),
	}
	b.postControl("/api/v1/archive/result", payload)
}

func (b *BigData) handleReplenishNotify(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TaskID        string `json:"taskId"`
		ClusterID     uint   `json:"clusterId"`
		DBID          uint   `json:"dbId"`
		DBName        string `json:"dbName"`
		BackupDate    string `json:"backupDate"`
		BackupEdition string `json:"backupEdition"`
		Path          string `json:"path"`
		SizeBytes     int64  `json:"sizeBytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.log.Printf("收到补发指令 task=%s db=%s date=%s edition=%s path=%s",
		in.TaskID, in.DBName, in.BackupDate, in.BackupEdition, in.Path)
	// 大数据在 GFS 重写缺失卷文件（模拟 DB/代理按模板补卷），完成后回调管控补发状态。
	size := in.SizeBytes
	if size <= 0 {
		size = 64 * 1024 * 1024
	}
	if err := b.gfs.WriteFull(in.Path, size); err != nil {
		b.postControl("/api/v1/replenish/task/status", map[string]any{
			"taskId": in.TaskID, "status": "FAILED",
		})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b.postControl("/api/v1/replenish/task/status", map[string]any{
		"taskId": in.TaskID, "status": "SUCCESS",
		"gfsPath": in.Path, "fileSizeBytes": size,
	})
	w.WriteHeader(http.StatusNoContent)
}

// postControl 向管控系统 POST 一个 JSON payload。
func (b *BigData) postControl(path string, payload map[string]any) {
	if b.ControlBase == "" {
		b.log.Printf("管控地址未配置，跳过回调 %s", path)
		return
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(b.ControlBase+path, "application/json", bytes.NewReader(body))
	if err != nil {
		b.log.Printf("回调管控失败 %s: %v", path, err)
		return
	}
	resp.Body.Close()
	b.log.Printf("已回调管控 %s -> %d", path, resp.StatusCode)
}

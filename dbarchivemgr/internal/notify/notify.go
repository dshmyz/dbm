// Package notify 提供管控向外部组件（DB/大数据代理）推送指令的 HTTP 出口。
//
// 管控发现巡检缺失后，经 HTTP 把补发指令推送给外部执行方，外部补完后再回调结果；
// 管控不自己在本进程内写卷补发。桩实现（MemReplenishNotifier）供单元测试记录收到指令。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
)

// ReplenishCandidate 一条待补发指令。Path 为缺失卷的 GFS 目标路径，FileSizeBytes 为应写大小。
type ReplenishCandidate struct {
	TaskID        string
	ClusterID     uint
	DBID          uint
	DBName        string
	BackupDate    time.Time
	BackupEdition model.BackupEdition
	Path          string // 缺失卷路径（外部据此重写该卷）
	FileSizeBytes int64  // 应写文件大小（外部按此落盘）
}

// ArchiveCandidate 归档清单中的一条（管控下发大数据，大数据据此自治拉取）。
type ArchiveCandidate struct {
	ArchiveItemID uint
	BackupID      uint
	GFSPath       string
	FileSizeBytes int64
}

// ReplenishNotifier 补发指令下发的抽象；真实现走 HTTP，桩实现供单测。
type ReplenishNotifier interface {
	Notify(item ReplenishCandidate) error
}

// ArchiveNotifier 归档清单下发的抽象：管控生成清单后经 HTTP 推给大数据自治拉取。
type ArchiveNotifier interface {
	NotifyArchive(batchID string, items []ArchiveCandidate) error
}

// MemArchiveNotifier 内存桩实现：记录收到每次归档清单（单测直用）。
type MemArchiveNotifier struct {
	Received []ArchiveCandidate
}

// NotifyArchive 记录收到清单条目。
func (m *MemArchiveNotifier) NotifyArchive(batchID string, items []ArchiveCandidate) error {
	m.Received = append(m.Received, items...)
	return nil
}

// HTTPReplenishNotifier 管控向大数据组件的 HTTP 推送实现（真实现）。
// 打 POST /v1/replenish/notify 。
type HTTPReplenishNotifier struct {
	Client *http.Client
	Base   string // 大数据组件服务地址
	log    *log.Logger
}

// DefaultTimeout 出站 HTTP 默认超时。
const DefaultTimeout = 30 * time.Second

// NewHTTP 构造打大数据组件 /v1/replenish/notify 的推送实现（默认超时）。
func NewHTTP(base string) *HTTPReplenishNotifier {
	return NewHTTPTimeout(base, DefaultTimeout)
}

// NewHTTPTimeout 携带指定超时；timeout<=0 回落默认。
func NewHTTPTimeout(base string, timeout time.Duration) *HTTPReplenishNotifier {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &HTTPReplenishNotifier{
		Client: &http.Client{Timeout: timeout}, Base: base,
		log: log.New(log.Writer(), "[HTTP 管控→大数据] ", log.LstdFlags),
	}
}

// Notify 推送一条补发指令给大数据组件。
func (h *HTTPReplenishNotifier) Notify(item ReplenishCandidate) error {
	payload := map[string]any{
		"taskId":        item.TaskID,
		"clusterId":     item.ClusterID,
		"dbId":          item.DBID,
		"dbName":        item.DBName,
		"backupDate":    item.BackupDate.Format("2006-01-02"),
		"backupEdition": string(item.BackupEdition),
		"path":          item.Path,
		"sizeBytes":     item.FileSizeBytes,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, h.Base+"/v1/replenish/notify", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	h.log.Printf("notify 补发 task=%s db=%s date=%s -> status=%d", item.TaskID, item.DBName, payload["backupDate"], resp.StatusCode)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("补发指令推送失败: status=%d", resp.StatusCode)
	}
	return nil
}

// HTTPArchiveNotifier 管控向大数据组件推送归档清单的实现（真实现）。
// 打 POST /v1/archive/notify 。
type HTTPArchiveNotifier struct {
	Client *http.Client
	Base   string // 大数据组件服务地址
	log    *log.Logger
}

// NewHTTPArchive 构造打大数据组件 /v1/archive/notify 的推送实现（默认超时）。
func NewHTTPArchive(base string) *HTTPArchiveNotifier {
	return NewHTTPArchiveTimeout(base, DefaultTimeout)
}

// NewHTTPArchiveTimeout 携带指定超时；timeout<=0 回落默认。
func NewHTTPArchiveTimeout(base string, timeout time.Duration) *HTTPArchiveNotifier {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &HTTPArchiveNotifier{
		Client: &http.Client{Timeout: timeout}, Base: base,
		log: log.New(log.Writer(), "[HTTP 管控→大数据] ", log.LstdFlags),
	}
}

// NotifyArchive 推送归档清单给大数据组件，令其自治拉取→写HDFS→回调结果。
func (h *HTTPArchiveNotifier) NotifyArchive(batchID string, items []ArchiveCandidate) error {
	type it struct {
		ArchiveItemID uint   `json:"archiveItemId"`
		BackupID      uint   `json:"backupId"`
		GFSPath       string `json:"gfsPath"`
		FileSizeBytes int64  `json:"fileSizeBytes"`
	}
	its := make([]it, 0, len(items))
	for _, c := range items {
		its = append(its, it(c)) // 字段一一对应，直接转换（标签随目标类型生效）
	}
	payload := map[string]any{"archiveBatchId": batchID, "items": its}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, h.Base+"/v1/archive/notify", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	h.log.Printf("notify 归档清单 batch=%s items=%d -> status=%d", batchID, len(items), resp.StatusCode)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("归档清单推送失败: status=%d", resp.StatusCode)
	}
	return nil
}

// MemReplenishNotifier 内存桩实现：记录收到每条补发指令（单测直用）。
type MemReplenishNotifier struct {
	Received []ReplenishCandidate
}

// Notify 记录收到指令，模拟成功。
func (m *MemReplenishNotifier) Notify(item ReplenishCandidate) error {
	m.Received = append(m.Received, item)
	return nil
}

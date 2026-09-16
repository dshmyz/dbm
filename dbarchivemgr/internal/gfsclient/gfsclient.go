// Package gfsclient 提供管控系统对 GFS 组件的只读访问客户端。
//
// 管控是「统一调度枢纽，不传输文件」：对 GFS 只做 stat / delete 两类操作，
// 且全部经 HTTP 交互（不再内联直连文件系统）。真实现打 GFS 组件的 HTTP 接口；
// MemFileStore 为内存桩的薄封装，供单元测试不经 server 直用。
package gfsclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"

	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
)

// FileStore 管控对 GFS 的访问面（只读 + 删除）。
type FileStore interface {
	// Stat 返回路径文件是否存在及实测 size（不做判定）。
	Stat(path string) (exists bool, size int64)
	// Exists 判定文件存在且非空（存在+size>阈值，对应上传/巡检判定口径）。
	Exists(path string) bool
	// Delete 从 GFS 删除文件（清理已满足留存条件的已归档备份）。
	Delete(path string)
}

// statResp GFS 组件 /v1/fs/stat 的响应体。
type statResp struct {
	Exists bool  `json:"exists"`
	Size   int64 `json:"size"`
}

// HTTPFileStore 管控对 GFS 组件的 HTTP 客户端实现（真实现）。
type HTTPFileStore struct {
	Client *http.Client
	Base   string // GFS 组件服务地址，如 http://127.0.0.1:8091
	log    *log.Logger
}

// NewHTTP 构造打 GFS 组件 HTTP 接口的 FileStore。
func NewHTTP(base string) *HTTPFileStore {
	return &HTTPFileStore{
		Client: http.DefaultClient, Base: base,
		log: log.New(log.Writer(), "[HTTP 管控→GFS] ", log.LstdFlags),
	}
}

// Stat 打 GET /v1/fs/stat?path=... ，返回 {exists,size}。
func (h *HTTPFileStore) Stat(path string) (bool, int64) {
	url := fmt.Sprintf("%s/v1/fs/stat?path=%s", h.Base, url.QueryEscape(path))
	resp, err := h.Client.Get(url)
	if err != nil {
		h.log.Printf("stat %s -> err=%v", url, err)
		return false, 0
	}
	defer resp.Body.Close()
	var sr statResp
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		h.log.Printf("stat %s -> 解析失败 %v", path, err)
		return false, 0
	}
	h.log.Printf("stat %s -> exists=%v size=%d", path, sr.Exists, sr.Size)
	return sr.Exists, sr.Size
}

// Exists 判定存在 + 非空（size>最小阈值）。size<=阈值的半截文件视为不存在/不达标。
func (h *HTTPFileStore) Exists(path string) bool {
	exists, size := h.Stat(path)
	return exists && size > sim.MinFileSize
}

// Delete 打 DELETE /v1/fs/delete ，body 携带 {path}。
func (h *HTTPFileStore) Delete(path string) {
	body, _ := json.Marshal(struct {
		Path string `json:"path"`
	}{path})
	req, err := http.NewRequest(http.MethodDelete, h.Base+"/v1/fs/delete", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.Client.Do(req)
	if err != nil {
		h.log.Printf("delete %s -> err=%v", path, err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	h.log.Printf("delete %s -> done", path)
}

// RegionSource 按机房(region)名取对应 GFS 访问面的抽象（多机房部署用）。
type RegionSource interface {
	// For 返回该机房对应的访问面；未配置的机房返回兜底访问面。
	For(region string) FileStore
}

// RegionRouter 按机房名分发到不同 GFS 管理端的多地址路由器。
// 适用场景：各机房独立部署 GlusterFS 集群及管理端，管控按集群的
// cityRegion / drRegion 将 stat/delete 分发到对应机房，未匹配机房回落 Default。
type RegionRouter struct {
	// Default 未匹配机房（含 region 为空）时的兜底访问面。
	Default FileStore
	regions map[string]FileStore
}

// NewRegionRouter 构造机房路由器。defaultURL 为兜底管理端（可空）；
// regionURLs 为 机房名→管理端地址 映射，键 "*" 显式指定兜底。
// 兜底均未给时，取映射中字典序第一个机房作为兜底。
func NewRegionRouter(defaultURL string, regionURLs map[string]string) *RegionRouter {
	r := &RegionRouter{regions: make(map[string]FileStore, len(regionURLs))}
	if defaultURL != "" {
		r.Default = NewHTTP(defaultURL)
	}
	for region, u := range regionURLs {
		if region == "*" {
			if r.Default == nil {
				r.Default = NewHTTP(u)
			}
			continue
		}
		r.regions[region] = NewHTTP(u)
	}
	if r.Default == nil {
		for _, region := range sortedKeys(regionURLs) {
			if region != "*" {
				r.Default = r.regions[region]
				break
			}
		}
	}
	return r
}

// For 按机房名分发；未配置的机房返回兜底（无兜底则返回「一律视为不存在」的空实现，
// 与 HTTP 出错同语义：stat 视为缺失、照常进补发流程，不 panic）。
func (r *RegionRouter) For(region string) FileStore {
	if region != "" {
		if fs, ok := r.regions[region]; ok {
			return fs
		}
	}
	if r.Default != nil {
		return r.Default
	}
	return missingStore{}
}

// missingStore 未匹配机房且无兜底时的空实现：一切文件视为不存在。
type missingStore struct{}

func (missingStore) Stat(string) (bool, int64) { return false, 0 }
func (missingStore) Exists(string) bool        { return false }
func (missingStore) Delete(string)             {}

// sortedKeys 返回 map 键的字典序（小工具，避免引入 slices 包依赖差异）。
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// MemFileStore 内存桩实现，薄封装 sim.GFS 的同名方法（单测直用，不启 server）。
type MemFileStore struct{ GFS *sim.GFS }

// Stat 委托 sim.GFS.Stat。
func (m *MemFileStore) Stat(path string) (bool, int64) {
	f, ok := m.GFS.Stat(path)
	if !ok {
		return false, 0
	}
	return true, f.Size
}

// Exists 委托 sim.GFS.Exists。
func (m *MemFileStore) Exists(path string) bool { return m.GFS.Exists(path) }

// Delete 委托 sim.GFS.Delete。
func (m *MemFileStore) Delete(path string) { m.GFS.Delete(path) }

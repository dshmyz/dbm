// Package sim 提供纯内存模拟桩，让全链路在无真实数据库/GlusterFS/大数据环境时跑通。
// 包含三个模拟模块：DB（数据库备份程序）、GFS（GlusterFS 双卷）、BigData（HDFS 归档）。
package sim

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// MinFileSize 存在判定的最小文件阈值（模拟业务 10MB 判定口径）。
const MinFileSize int64 = 10 * 1024 * 1024 // 10MB

// 路径模板占位符与默认值。
// 模板可含 {root} {cluster} {db} {date} {vol}，其中 {vol} 为双卷标识(city 同城/dr 异地)。
// 不同集群/库可配置各自模板(库级优先→集群→默认)。
const (
	DefaultRoot         = "/gfs" // 挂载前缀
	VolCity             = "city" // 同城备卷
	VolDR               = "dr"   // 异地备卷
	DefaultPathTemplate = "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz"
)

// pathPlaceholders 模板支持的占位符（{root} 仅作挂载前缀，不参与反解析业务字段）。
var pathPlaceholders = []string{"{root}", "{cluster}", "{db}", "{date}", "{vol}"}

// PathFields 从备份路径按模板反解析出的字段。
type PathFields struct {
	Cluster string
	DB      string
	Date    string
	Vol     string
}

// tokenize 把模板切成交替字面/占位符段，如
// "{root}/{cluster}/{db}/{date}/{vol}/full.sql.gz" →
//
//	[{root} "/" {cluster} "/" {db} "/" {date} "/" {vol} "/" "full.sql.gz"]。
func tokenize(tmpl string) []string {
	var parts []string
	rest := tmpl
	for rest != "" {
		next, ph := -1, ""
		for _, p := range pathPlaceholders {
			if idx := strings.Index(rest, p); idx >= 0 && (next == -1 || idx < next) {
				next, ph = idx, p
			}
		}
		if next == -1 {
			parts = append(parts, rest)
			break
		}
		if next > 0 {
			parts = append(parts, rest[:next])
		}
		parts = append(parts, ph)
		rest = rest[next+len(ph):]
	}
	return parts
}

// buildOne 按模板生成单条卷路径。
func buildOne(tmpl, cluster, db, date, vol string) string {
	repl := map[string]string{
		"{root}": DefaultRoot, "{cluster}": cluster,
		"{db}": db, "{date}": date, "{vol}": vol,
	}
	var b strings.Builder
	for _, s := range tokenize(tmpl) {
		if v, ok := repl[s]; ok {
			b.WriteString(v)
		} else {
			b.WriteString(s)
		}
	}
	return b.String()
}

// BuildPaths 按路径模板生成同城卷与异地卷两条路径。
// edition 决定主卷：SAME_CITY→{vol}=city，DISASTER_RECOVERY→{vol}=dr；
// 返回始终为 (cityPath, drPath)。template 为空则用默认。
func BuildPaths(template, cluster, db, date, edition string) (cityPath, drPath string) {
	if template == "" {
		template = DefaultPathTemplate
	}
	mainVol := VolCity
	if edition == "DISASTER_RECOVERY" {
		mainVol = VolDR
	}
	peerVol := VolCity
	if mainVol == VolCity {
		peerVol = VolDR
	}
	return buildOne(template, cluster, db, date, mainVol), buildOne(template, cluster, db, date, peerVol)
}

// ParsePath 按模板从一条路径反解析出 集群/库/日期/卷。
// 原理：把模板与路径按 '/' 切为段序列，逐段锁定——占位符取同位置真实段，
// 字面段须与路径段一致；{root} 为挂载前缀段，忽略匹配。
// 需满足「每个占位符独占一个路径段」的模板约定。结构不符返回 ok=false。
func ParsePath(template, path string) (PathFields, bool) {
	if template == "" {
		template = DefaultPathTemplate
	}
	tSegs := strings.Split(strings.Trim(template, "/"), "/")
	pSegs := strings.Split(strings.Trim(path, "/"), "/")
	if len(tSegs) != len(pSegs) {
		return PathFields{}, false
	}
	f := PathFields{}
	for i, t := range tSegs {
		switch t {
		case "{root}":
			// 挂载前缀段，忽略匹对
		case "{cluster}":
			f.Cluster = pSegs[i]
		case "{db}":
			f.DB = pSegs[i]
		case "{date}":
			f.Date = pSegs[i]
		case "{vol}":
			f.Vol = pSegs[i]
		default:
			if t != pSegs[i] {
				return PathFields{}, false
			}
		}
	}
	return f, true
}

// File 表示 GFS/HDFS 上的一个模拟文件。
type File struct {
	Path    string
	Size    int64
	Data    []byte
	ModTime time.Time
}

// GFS 模拟 GlusterFS：同城卷 /gfs-city 与 异地卷 /gfs-dr 两个独立卷。
// FUSE 挂载本地目录语义：Write/Stat/Exists/List/Delete 均对本地目录操作。
type GFS struct {
	mu    sync.Mutex
	root  string
	files map[string]*File // key: path
}

// NewGFS 创建内存 GlusterFS 桩。root 仅作展示用。
func NewGFS(root string) *GFS {
	return &GFS{root: root, files: map[string]*File{}}
}

// Write 写入文件（模拟 DB 写挂载目录）。
func (g *GFS) Write(path string, data []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	sz := len(data)
	if int64(sz) < MinFileSize {
		// 模拟半截文件：仍写入小文件，由 Stat/校验判定为 EMPTY
		sz = 3
		data = []byte("half")
	}
	g.files[path] = &File{Path: path, Size: int64(sz), Data: data, ModTime: time.Now()}
	return nil
}

// WriteFull 写入一个满足阈值的大文件，用于模拟正常备份。
func (g *GFS) WriteFull(path string, size int64) error {
	// 用整型序列模拟内容
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.files[path] = &File{Path: path, Size: size, Data: buf, ModTime: time.Now()}
	return nil
}

// Stat 取得文件信息（存在 + size）。
func (g *GFS) Stat(path string) (*File, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := g.files[path]
	if !ok {
		return nil, false
	}
	cp := *f
	return &cp, true
}

// Exists 判断文件是否存在且 size>MinFileSize（对应存在+非空判定）。
func (g *GFS) Exists(path string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := g.files[path]
	return ok && f.Size > MinFileSize
}

// Verify 存在+非空校验：返回存在性与是否达标。
func (g *GFS) Verify(path string) (exists bool, sizeOK bool, size int64) {
	f, ok := g.Stat(path)
	if !ok {
		return false, false, 0
	}
	return true, f.Size > MinFileSize, f.Size
}

// List 列出某目录前缀下的所有文件路径。
func (g *GFS) List(prefix string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []string
	for p := range g.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// Read 读取文件数据（模拟大数据拉取）。
func (g *GFS) Read(path string) ([]byte, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := g.files[path]
	if !ok {
		return nil, false
	}
	return f.Data, true
}

// Delete 删除文件（模拟 GFS 清理已归档满10天）。
func (g *GFS) Delete(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.files, path)
}

// Handler 暴露 GFS 作为独立 HTTP 组件（管控经 HTTP 与其交互，不直连文件系统）。
//   - GET    /v1/fs/stat?path=... → {"exists":bool,"size":int}
//   - DELETE /v1/fs/delete  body={"path":...}
func (g *GFS) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/fs/stat", func(w http.ResponseWriter, r *http.Request) {
		f, ok := g.Stat(r.URL.Query().Get("path"))
		exists := ok
		size := int64(0)
		if ok {
			size = f.Size
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"exists": exists, "size": size})
	})
	mux.HandleFunc("/v1/fs/delete", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Path string `json:"path"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		g.Delete(body.Path)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// HDFS 模拟大数据归档系统 HDFS 存储。
type HDFS struct {
	mu    sync.Mutex
	files map[string]*File
}

// NewHDFS 创建 HDFS 桩。
func NewHDFS() *HDFS {
	return &HDFS{files: map[string]*File{}}
}

// Write 写入 HDFS 文件。
func (h *HDFS) Write(path string, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files[path] = &File{Path: path, Size: int64(len(data)), Data: data, ModTime: time.Now()}
	return nil
}

// Stat 取得 HDFS 文件信息。
func (h *HDFS) Stat(path string) (*File, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, ok := h.files[path]
	if !ok {
		return nil, false
	}
	cp := *f
	return &cp, true
}

// Delete 删除 HDFS 文件。
func (h *HDFS) Delete(path string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.files, path)
}

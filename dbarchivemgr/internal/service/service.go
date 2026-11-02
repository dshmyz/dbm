// Package service 实现 DBArchiveManager 核心业务逻辑：
// 元数据上报、完整性巡检、缺失补发、滚动归档清单生成、归档结果处理、清理判定。
package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/notify"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/state"
)

// Service DBArchiveManager 核心服务。
type Service struct {
	db *gorm.DB
	fs gfsclient.FileStore // 管控对 GFS 的访问面（只读+删除，走 HTTP 或内存桩）

	// Regions 多机房 GFS 路由（可选，来自启动参数 -gfs-map 的静态映射）。
	// 配置后按集群 cityRegion/drRegion 把同城卷/异地卷的 stat/delete 分发到对应机房的
	// GFS 管理端；nil=单地址模式。库表 tb_gfs_endpoint（界面配置）优先于该静态映射。
	Regions gfsclient.RegionSource

	// DB 机房端点表（tb_gfs_endpoint，界面可管）的短缓存；写操作后立即失效。
	regionMu    sync.Mutex
	regionCache *dbRegionSource
	regionAt    time.Time

	// Notifier 补发指令下发出口（走 HTTP 推送给外部组件）。为 nil 时不下发。
	Notifier notify.ReplenishNotifier
	// ArchiveNotifier 归档清单下发出口（走 HTTP 推给大数据自治拉取）。为 nil 时不下发。
	// 多机房配置（tb_gfs_endpoint）时按主卷机房分发，此项为兜底/单地址出口。
	ArchiveNotifier notify.ArchiveNotifier

	// 参数
	minFileSize int64 // 存在判定阈值
	retryMax    int   // 补发/传输最大重试

	// CleanupMode 清理触发方：mgr=管控触发删除(默认/现状)；gfs=GFS 自治清理(原始设想，
	// 管控只做放行判定不退删)。空字符串按 mgr 处理。
	CleanupMode string
}

// New 构造核心服务。fs 为管控对 GFS 的访问面（HTTP 客户端或内存桩）。
// 管控不持有文件系统具体实现：文件读写发生在独立组件，管控只经 fs 与其交互。
func New(db *gorm.DB, fs gfsclient.FileStore) *Service {
	return &Service{
		db: db, fs: fs,
		minFileSize: sim.MinFileSize,
		retryMax:    3,
	}
}

// endpointTimeout 出站超时：机房端点显式配置（秒）优先，否则用默认 30s。
func (s *Service) endpointTimeout(sec int) time.Duration {
	if sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return gfsclient.DefaultTimeout
}

// audit 写一条操作流水（META_REPORT/REPLENISH/ROLLING/ARCHIVE/CLEAN/SWEEP）。
// 留痕为尽力而为：写入失败不阻断主流程。
func (s *Service) audit(action string, backupID uint, detail string) {
	s.db.Create(&model.AuditLog{Action: action, BackupID: backupID, Detail: detail})
}

// logVerify 写一条「存在 + size」校验留痕（tb_file_verify_log）。
// chain=WRITE(DB写双卷)/ARCHIVE(GFS→大数据)；vol 为 city/dr；stage 为 STAT_EXIST/STAT_SIZE/ARCHIVE_SIZE。
func (s *Service) logVerify(backupID uint, chain, vol, stage string, expected, actual int64, result string) {
	vl := model.FileVerifyLog{
		BackupID: backupID, Chain: chain, Volumn: strings.ToUpper(vol), Stage: stage,
		SizeExpected: expected, SizeActual: actual,
		ExistsFlag: actual > 0, Result: result,
	}
	if result == "VERIFIED" {
		now := time.Now()
		vl.VerifiedAt = &now
	}
	s.db.Create(&vl)
}

// --- 机房路由（多 GFS 管理端） ---

// dbRegionSource 以元数据库 tb_gfs_endpoint（界面可管）为配置源的机房路由。
type dbRegionSource struct {
	stores map[string]gfsclient.FileStore // region → 访问面（含 "*" 兜底）
}

// For 按机房取访问面；未匹配回落 "*" 兜底；无兜底返回 nil（volStore 再回落默认访问面）。
func (d *dbRegionSource) For(region string) gfsclient.FileStore {
	if region != "" {
		if fs, ok := d.stores[region]; ok {
			return fs
		}
	}
	return d.stores["*"]
}

// regionSource 取当前生效的机房路由源，优先级：
// 库表 tb_gfs_endpoint（界面配置，30s 缓存/写后失效） > 启动参数 -gfs-map 静态映射 >
// nil（单地址模式，统一回落 s.fs）。
func (s *Service) regionSource() gfsclient.RegionSource {
	s.regionMu.Lock()
	defer s.regionMu.Unlock()
	if s.regionCache != nil && time.Since(s.regionAt) < 30*time.Second {
		return s.regionCache
	}
	var eps []model.GfsEndpoint
	if err := s.db.Find(&eps).Error; err != nil {
		return s.Regions // 查库异常时回落静态映射（不影响主流程）
	}
	if len(eps) == 0 {
		s.regionCache = nil
		return s.Regions
	}
	stores := make(map[string]gfsclient.FileStore, len(eps))
	for _, e := range eps {
		if e.BaseURL != "" {
			stores[e.Region] = gfsclient.NewHTTPTimeout(e.BaseURL, s.endpointTimeout(e.TimeoutSec))
		}
	}
	if len(stores) == 0 {
		s.regionCache = nil
		return s.Regions
	}
	s.regionCache = &dbRegionSource{stores: stores}
	s.regionAt = time.Now()
	return s.regionCache
}

// invalidateRegions 机房端点增删改后立即使缓存失效。
func (s *Service) invalidateRegions() {
	s.regionMu.Lock()
	s.regionCache = nil
	s.regionMu.Unlock()
}

// mainVolOf 备份主卷标识：同城备→city，异地备→dr（与 sim.BuildPaths 口径一致）。
func mainVolOf(ed model.BackupEdition) string {
	if ed == model.EditionDisasterRecovery {
		return sim.VolDR
	}
	return sim.VolCity
}

// peerVolOf 备份对卷标识（主卷的另一卷）。
func peerVolOf(ed model.BackupEdition) string {
	if ed == model.EditionDisasterRecovery {
		return sim.VolCity
	}
	return sim.VolDR
}

// volStore 返回某集群某卷(city/dr)对应的 GFS 访问面：
// 配置了机房路由（库表优先、其次启动参数映射）时按「同城卷→cityRegion、异地卷→drRegion」分发，
// 未配置（单地址模式）、机房未匹配且无兜底时统一回落默认访问面。
func (s *Service) volStore(cl *model.Cluster, vol string) gfsclient.FileStore {
	if src := s.regionSource(); src != nil {
		region := cl.CityRegion
		if vol == sim.VolDR {
			region = cl.DRRegion
		}
		if fs := src.For(region); fs != nil {
			return fs
		}
	}
	return s.fs
}

// --- 机房端点配置（界面/接口管理 tb_gfs_endpoint） ---

// ListGfsEndpoints 列出机房→GFS 管理端配置（按机房名排序）。
func (s *Service) ListGfsEndpoints() ([]model.GfsEndpoint, error) {
	var eps []model.GfsEndpoint
	err := s.db.Order("region").Find(&eps).Error
	return eps, err
}

// CreateGfsEndpoint 新增机房管理端（region 唯一；"*" 为兜底端点）。
func (s *Service) CreateGfsEndpoint(e *model.GfsEndpoint) (*model.GfsEndpoint, error) {
	if e.Region == "" || e.BaseURL == "" {
		return nil, errors.New("region 与 baseUrl 必填")
	}
	var cnt int64
	s.db.Model(&model.GfsEndpoint{}).Where("region = ?", e.Region).Count(&cnt)
	if cnt > 0 {
		return nil, fmt.Errorf("机房 %s 已存在", e.Region)
	}
	if err := s.db.Create(e).Error; err != nil {
		return nil, err
	}
	s.invalidateRegions()
	return e, nil
}

// UpdateGfsEndpoint 更新机房管理端（region 唯一性校验排除自身）。
func (s *Service) UpdateGfsEndpoint(id uint, in *model.GfsEndpoint) (*model.GfsEndpoint, error) {
	var e model.GfsEndpoint
	if err := s.db.First(&e, id).Error; err != nil {
		return nil, err
	}
	if in.Region != "" && in.Region != e.Region {
		var cnt int64
		s.db.Model(&model.GfsEndpoint{}).Where("region = ? AND id != ?", in.Region, id).Count(&cnt)
		if cnt > 0 {
			return nil, fmt.Errorf("机房 %s 已存在", in.Region)
		}
		e.Region = in.Region
	}
	if in.BaseURL != "" {
		e.BaseURL = in.BaseURL
	}
	e.TimeoutSec = in.TimeoutSec
	e.Remark = in.Remark
	if err := s.db.Save(&e).Error; err != nil {
		return nil, err
	}
	s.invalidateRegions()
	return &e, nil
}

// DeleteGfsEndpoint 删除机房管理端。
func (s *Service) DeleteGfsEndpoint(id uint) error {
	if err := s.db.Delete(&model.GfsEndpoint{}, id).Error; err != nil {
		return err
	}
	s.invalidateRegions()
	return nil
}

// --- 集群 / 库 ---

// RegisterCluster 注册集群（对应 POST /api/v1/cluster）。
func (s *Service) RegisterCluster(c *model.Cluster) (*model.Cluster, error) {
	if err := s.db.Create(c).Error; err != nil {
		return nil, err
	}
	return c, nil
}

// BindDB 绑定库到集群（对应 POST /api/v1/cluster/{id}/db）。
func (s *Service) BindDB(d *model.DB) (*model.DB, error) {
	if err := s.db.Create(d).Error; err != nil {
		return nil, err
	}
	return d, nil
}

// GetCluster 查询集群拓扑（对应 GET /api/v1/cluster/{id}）。
func (s *Service) GetCluster(id uint) (*model.Cluster, error) {
	var c model.Cluster
	if err := s.db.First(&c, id).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

// AddClusterInstance 给集群挂一台实例 IP（对应 POST /api/v1/cluster/{id}/instance）。
// HostIP 全表唯一，保证「一个 IP 只属于一个集群」。
func (s *Service) AddClusterInstance(clusterID uint, ip, hostName, role string) (*model.ClusterInstance, error) {
	if !validIP(ip) {
		return nil, errors.New("非法 IP 地址")
	}
	var cl model.Cluster
	if err := s.db.First(&cl, clusterID).Error; err != nil {
		return nil, fmt.Errorf("集群不存在: %w", err)
	}
	inst := model.ClusterInstance{ClusterID: clusterID, HostIP: ip, HostName: hostName, Role: role}
	if err := s.db.Create(&inst).Error; err != nil {
		return nil, fmt.Errorf("IP 已被其他集群占用或重复: %w", err)
	}
	return &inst, nil
}

// ListClusterInstances 查询集群下的全部实例 IP（对应 GET /api/v1/cluster/{id}/instances）。
func (s *Service) ListClusterInstances(clusterID uint) ([]model.ClusterInstance, error) {
	var list []model.ClusterInstance
	if err := s.db.Where("cluster_id = ?", clusterID).Order("id").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// --- 只读列表查询（管理后台数据源） ---

// ListClusters 集群列表。
func (s *Service) ListClusters() ([]model.Cluster, error) {
	var list []model.Cluster
	if err := s.db.Order("id").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListDBs 库列表；clusterID 为 0 时返回全部。
func (s *Service) ListDBs(clusterID uint) ([]model.DB, error) {
	var list []model.DB
	q := s.db
	if clusterID > 0 {
		q = q.Where("cluster_id = ?", clusterID)
	}
	if err := q.Order("id").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListBackupMeta 备份元数据列表；clusterID/dbName/archiveStatus 任一为空则不过滤该项。
func (s *Service) ListBackupMeta(clusterID uint, dbName, archiveStatus string) ([]model.BackupMeta, error) {
	var list []model.BackupMeta
	q := s.db.Order("backup_date DESC").Order("id DESC")
	if clusterID > 0 {
		q = q.Where("cluster_id = ?", clusterID)
	}
	if dbName != "" {
		q = q.Where("db_name = ?", dbName)
	}
	if archiveStatus != "" {
		q = q.Where("archive_status = ?", archiveStatus)
	}
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListInspects 巡检历史，按巡检 ID 倒序，limit 控制条数（<=0 全部）。
func (s *Service) ListInspects(limit int) ([]model.Inspect, error) {
	var list []model.Inspect
	q := s.db.Order("id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListReplenishTasks 补发任务列表（新→旧）。
func (s *Service) ListReplenishTasks() ([]model.ReplenishTask, error) {
	var list []model.ReplenishTask
	if err := s.db.Order("created_at DESC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListArchiveBatches 归档批次列表（新→旧）。
func (s *Service) ListArchiveBatches() ([]model.ArchiveBatch, error) {
	var list []model.ArchiveBatch
	if err := s.db.Order("generate_time DESC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// --- 集群配置管理（CSV 导入 / 编辑 / 删除） ---

// ImportSummary CSV 导入汇总（逐行容错：错误行计入 Errors，不中断整体）。
type ImportSummary struct {
	Cluster  int      `json:"cluster"`
	DB       int      `json:"db"`
	Instance int      `json:"instance"`
	Errors   []string `json:"errors"`
}

// csvClusterCode 定位以 clusterCode 为关联键的集群；不存在返回错误。
func (s *Service) clusterByCode(code string) (*model.Cluster, error) {
	if code == "" {
		return nil, errors.New("clusterCode 为空")
	}
	var cl model.Cluster
	if err := s.db.Where("cluster_code = ?", code).First(&cl).Error; err != nil {
		return nil, fmt.Errorf("clusterCode %q 不存在（请先导入该集群行）", code)
	}
	return &cl, nil
}

// cell 取记录中第 idx 个单元格的去除首尾空白值（越界返回 ""）。
func cell(r []string, idx int) string {
	if idx < 0 || idx >= len(r) {
		return ""
	}
	return strings.TrimSpace(r[idx])
}

// intCell 解析非空整数单元格；为空返回 ok=false（不覆盖既有值）。
func intCell(r []string, idx int) (v int, ok bool) {
	t := cell(r, idx)
	if t == "" {
		return 0, false
	}
	n, err := strconv.Atoi(t)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (s *Service) applyClusterRow(r []string) error {
	code := cell(r, 1)
	if code == "" {
		return errors.New("clusterCode 必填")
	}
	var cl model.Cluster
	if err := s.db.Where("cluster_code = ?", code).First(&cl).Error; err != nil {
		cl = model.Cluster{ClusterCode: code}
	}
	if v := cell(r, 2); v != "" {
		cl.ClusterName = v
	}
	if v := cell(r, 3); v != "" {
		cl.PrimaryRegion = v
	}
	if v := cell(r, 4); v != "" {
		cl.DRRegion = v
	}
	if v := cell(r, 5); v != "" {
		cl.CityRegion = v
	}
	if v, ok := intCell(r, 6); ok {
		cl.ArchiveWindowDays = v
	}
	// retentionDays：0=立即清，正数=留存N天，空=不覆盖（新集群用默认10）
	if t := cell(r, 7); t == "0" {
		z := 0
		cl.RetentionDays = &z
	} else if v, ok := intCell(r, 7); ok {
		cl.RetentionDays = &v
	} else if cl.ID == 0 {
		cl.RetentionDays = nil // 新集群缺省走 DB 默认 10
	}
	if v := cell(r, 8); v != "" {
		cl.PathTemplate = v
	}
	if cl.ID == 0 {
		return s.db.Create(&cl).Error
	}
	return s.db.Save(&cl).Error
}

func (s *Service) applyDBRow(r []string) error {
	cl, err := s.clusterByCode(cell(r, 1))
	if err != nil {
		return err
	}
	dbName := cell(r, 9)
	if dbName == "" {
		return errors.New("dbName 必填")
	}
	var dbm model.DB
	if err := s.db.Where("cluster_id = ? AND db_name = ?", cl.ID, dbName).First(&dbm).Error; err != nil {
		dbm = model.DB{ClusterID: cl.ID, DBName: dbName}
	}
	if v := cell(r, 10); v != "" {
		dbm.Editions = strings.ReplaceAll(v, ";", ",") // 队内逗号分隔；CSV 中避免逗号冲突用分号
	}
	if v, ok := intCell(r, 11); ok {
		dbm.ArchiveWindowDays = v
	}
	if v := cell(r, 12); v != "" {
		dbm.ArchiveTime = v
	}
	if v := cell(r, 13); v != "" {
		dbm.PathTemplate = v
	}
	if dbm.ID == 0 {
		dbm.ClusterID = cl.ID
		return s.db.Create(&dbm).Error
	}
	return s.db.Save(&dbm).Error
}

func (s *Service) applyInstanceRow(r []string) error {
	cl, err := s.clusterByCode(cell(r, 1))
	if err != nil {
		return err
	}
	ip := cell(r, 14)
	if ip == "" {
		return errors.New("hostIp 必填")
	}
	var inst model.ClusterInstance
	found := s.db.Where("host_ip = ?", ip).First(&inst).Error == nil
	if !found {
		if !validIP(ip) {
			return errors.New("非法 IP 地址")
		}
		inst = model.ClusterInstance{ClusterID: cl.ID, HostIP: ip}
	}
	if found && inst.ClusterID != cl.ID {
		return fmt.Errorf("hostIp %s 已属于其他集群", ip)
	}
	if v := cell(r, 15); v != "" {
		inst.HostName = v
	}
	if v := cell(r, 16); v != "" {
		inst.Role = v
	}
	if inst.ID == 0 {
		inst.ClusterID = cl.ID
		return s.db.Create(&inst).Error
	}
	return s.db.Save(&inst).Error
}

// ImportClustersCSV 解析 CSV 并逐行 upsert 集群/库/实例（对应 POST /api/v1/cluster/import）。
// 首列 type 标识行类型（cluster/db/instance），clusterCode 关联；缺省 clusterCode 查找不到则整行报错。
func (s *Service) ImportClustersCSV(rd io.Reader) (ImportSummary, error) {
	var sum ImportSummary
	recs, err := csv.NewReader(rd).ReadAll()
	if err != nil {
		return sum, fmt.Errorf("解析 CSV 失败: %w", err)
	}
	for i, rec := range recs {
		if len(rec) == 0 || allEmpty(rec) {
			continue
		}
		if i == 0 && cell(rec, 0) == "type" {
			continue // 表头
		}
		var applyErr error
		switch cell(rec, 0) {
		case "cluster":
			applyErr = s.applyClusterRow(rec)
			if applyErr == nil {
				sum.Cluster++
			}
		case "db":
			applyErr = s.applyDBRow(rec)
			if applyErr == nil {
				sum.DB++
			}
		case "instance":
			applyErr = s.applyInstanceRow(rec)
			if applyErr == nil {
				sum.Instance++
			}
		default:
			applyErr = fmt.Errorf("未知类型 %q", cell(rec, 0))
		}
		if applyErr != nil {
			sum.Errors = append(sum.Errors, fmt.Sprintf("第%d行 %s: %v", i+1, cell(rec, 0), applyErr))
		}
	}
	return sum, nil
}

// allEmpty 判断整行均为空。
func allEmpty(r []string) bool {
	for _, c := range r {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

// UpdateCluster 更新集群配置（保留 CreatedAt，校验 clusterCode 唯一）。
func (s *Service) UpdateCluster(id uint, c *model.Cluster) error {
	var ex model.Cluster
	if err := s.db.First(&ex, id).Error; err != nil {
		return err
	}
	c.ID, c.CreatedAt = ex.ID, ex.CreatedAt
	if c.ClusterCode != "" && c.ClusterCode != ex.ClusterCode {
		var n int64
		if err := s.db.Model(&model.Cluster{}).
			Where("cluster_code = ? AND id <> ?", c.ClusterCode, id).Count(&n).Error; err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("clusterCode %q 已被其他集群使用", c.ClusterCode)
		}
	}
	if c.ClusterCode == "" {
		c.ClusterCode = ex.ClusterCode
	}
	return s.db.Save(c).Error
}

// DeleteCluster 删除集群，级联删除其下全部库与实例（备份元数据/归档历史保留）。
func (s *Service) DeleteCluster(id uint) error {
	if err := s.db.First(&model.Cluster{}, id).Error; err != nil {
		return err
	}
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("cluster_id = ?", id).Delete(&model.DB{}).Error; err != nil {
			return err
		}
		if err := tx.Where("cluster_id = ?", id).Delete(&model.ClusterInstance{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.Cluster{}, id).Error
	})
}

// UpdateDB 更新库配置（保留 CreatedAt）。
func (s *Service) UpdateDB(id uint, d *model.DB) error {
	var ex model.DB
	if err := s.db.First(&ex, id).Error; err != nil {
		return err
	}
	d.ID, d.ClusterID, d.CreatedAt = ex.ID, ex.ClusterID, ex.CreatedAt
	return s.db.Save(d).Error
}

// DeleteDB 删除库（备份元数据/归档历史保留）。
func (s *Service) DeleteDB(id uint) error {
	if err := s.db.First(&model.DB{}, id).Error; err != nil {
		return err
	}
	return s.db.Delete(&model.DB{}, id).Error
}

// UpdateClusterInstance 更新实例配置（保留 CreatedAt）。
func (s *Service) UpdateClusterInstance(id uint, inst *model.ClusterInstance) error {
	var ex model.ClusterInstance
	if err := s.db.First(&ex, id).Error; err != nil {
		return err
	}
	inst.ID, inst.ClusterID, inst.CreatedAt = ex.ID, ex.ClusterID, ex.CreatedAt
	if inst.HostIP == "" {
		inst.HostIP = ex.HostIP
	}
	return s.db.Save(inst).Error
}

// DeleteClusterInstance 删除实例。
func (s *Service) DeleteClusterInstance(id uint) error {
	if err := s.db.First(&model.ClusterInstance{}, id).Error; err != nil {
		return err
	}
	return s.db.Delete(&model.ClusterInstance{}, id).Error
}

// ResolveClusterByIP 备份按 IP 反查所属集群：
// 经 tb_cluster_instance 由 HostIP → cluster_id → Cluster。
func (s *Service) ResolveClusterByIP(hostIP string) (*model.Cluster, error) {
	var inst model.ClusterInstance
	if err := s.db.Where("host_ip = ?", hostIP).First(&inst).Error; err != nil {
		return nil, fmt.Errorf("IP %s 未关联任何集群: %w", hostIP, err)
	}
	var cl model.Cluster
	if err := s.db.First(&cl, inst.ClusterID).Error; err != nil {
		return nil, err
	}
	return &cl, nil
}

// --- 备份元数据上报 ---

// resolvePathTemplate 解析库的实际 GFS 路径模板：库级 → 集群级 → 全局默认。
func (s *Service) resolvePathTemplate(dbm *model.DB, cl *model.Cluster) string {
	if dbm.PathTemplate != "" {
		return dbm.PathTemplate
	}
	if cl.PathTemplate != "" {
		return cl.PathTemplate
	}
	return sim.DefaultPathTemplate
}

// BuildPathForDB 按库/集群的路径模板生成该备份的双卷路径(cluster 用 IP 标识)。
func (s *Service) BuildPathForDB(dbm *model.DB, cl *model.Cluster, hostIP, date string, edition model.BackupEdition) (city, dr string) {
	tmpl := s.resolvePathTemplate(dbm, cl)
	return sim.BuildPaths(tmpl, hostIP, dbm.DBName, date, string(edition))
}

// ReportBackupMeta 上报备份元数据（链路A判定已由 DB 完成双卷写入+自查）。
// 对应 POST /api/v1/backup/meta/report，主机以 hostIP 上报，系统按 IP 反查集群。
// 幂等：以唯一维度覆盖更新。
func (s *Service) ReportBackupMeta(hostIP, dbName string, date time.Time, edition model.BackupEdition,
	gfsPath, peerGfsPath string, size int64) (*model.BackupMeta, error) {

	cl, err := s.ResolveClusterByIP(hostIP)
	if err != nil {
		return nil, err
	}
	var dbm model.DB
	if err := s.db.Where("cluster_id = ? AND db_name = ?", cl.ID, dbName).First(&dbm).Error; err != nil {
		return nil, fmt.Errorf("库未绑定集群: %w", err)
	}

	date = date.UTC().Truncate(24 * time.Hour)

	meta := model.BackupMeta{}
	err = s.db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ?",
		cl.ID, dbm.ID, date, edition).First(&meta).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 终态/归档中锁定：重复上报（DB 重试、消息重投）不得复位 ARCHIVING/ARCHIVED，
	// 即使文件已随归档清理也幂等返回原记录，不触发补发。
	if meta.ID != 0 && (meta.ArchiveStatus == model.StatusArchiving ||
		meta.ArchiveStatus == model.StatusArchived) {
		return &meta, nil
	}

	// 由 DB 上报前应已通过「双卷存在+非空」校验；此处复核一次。
	// 多机房模式下主卷/对卷分属主/对机房的 GFS 管理端，分别复核。
	mainVol, peerVol := mainVolOf(edition), peerVolOf(edition)
	if !s.volStore(cl, mainVol).Exists(gfsPath) || !s.volStore(cl, peerVol).Exists(peerGfsPath) {
		return nil, errors.New("上传判定未通过：双卷文件不存在或未达阈值")
	}

	now := time.Now()
	if errors.Is(err, gorm.ErrRecordNotFound) {
		meta = model.BackupMeta{
			ClusterID: cl.ID, DBID: dbm.ID, DBName: dbName,
			HostIP: hostIP, BackupDate: date, BackupEdition: edition,
		}
	}
	// 覆盖更新
	meta.HostIP = hostIP
	meta.GFSPath = gfsPath
	meta.PeerGFSPath = peerGfsPath
	meta.FileSizeBytes = size
	if meta.FileSizeBytes <= 0 { // 未显式上报 size 时，从 GFS stat 实测主卷
		if ok, sz := s.volStore(cl, mainVol).Stat(gfsPath); ok {
			meta.FileSizeBytes = sz
		}
	}
	if ok, sz := s.volStore(cl, peerVol).Stat(peerGfsPath); ok {
		meta.PeerFileSizeBytes = sz
	}
	meta.UploadVerify = model.VerifyVerified
	meta.UploadStatus = "DONE"
	meta.UploadVerifiedAt = &now
	state.OnMetaReport(&meta) // -> NOT_ARCHIVED

	if meta.ID == 0 {
		if err := s.db.Create(&meta).Error; err != nil {
			return nil, err
		}
	} else {
		if err := s.db.Save(&meta).Error; err != nil {
			return nil, err
		}
	}
	// 留痕：链路A 双卷「存在 + size」判定 + 上报流水
	s.logVerify(meta.ID, "WRITE", mainVol, "STAT_SIZE", s.minFileSize, meta.FileSizeBytes, "VERIFIED")
	s.logVerify(meta.ID, "WRITE", peerVol, "STAT_SIZE", s.minFileSize, meta.PeerFileSizeBytes, "VERIFIED")
	s.audit("META_REPORT", meta.ID, fmt.Sprintf("%s %s size=%d",
		dbName, meta.BackupDate.Format("2006-01-02"), meta.FileSizeBytes))
	return &meta, nil
}

// InspectResult 巡检结果。
type InspectResult struct {
	InspectID    uint
	TotalSlots   int
	MissingList  []model.InspectMissing
	MissingCount int
	BrokenCount  int
	OKCount      int
}

// RunInspect 每日完整性巡检（对应 POST /api/v1/inspect/run）。
// 遍历近 windowDays 天所有（集群,库,版本）应备槽位，stat 判定存在+非空。
// 已归档（文件可能已随正常清理删除）或已清理的记录不再纳入，避免把正常清理误判为缺失。
func (s *Service) RunInspect(targetDate time.Time, windowDays int) (*InspectResult, error) {
	target := targetDate.UTC().Truncate(24 * time.Hour)
	// 该日应生成的全量备份（排除已归档/已清理记录）
	metaList := []model.BackupMeta{}
	if err := s.db.
		Where("backup_date >= ? AND archive_status <> ? AND deleted_at IS NULL",
			target.AddDate(0, 0, -windowDays), model.StatusArchived).
		Find(&metaList).Error; err != nil {
		return nil, err
	}

	// 应备槽位 = 已上报元数据的每条记录（其双卷应都在 GFS 上）
	cls := map[uint]*model.Cluster{} // 集群缓存（多机房路由需按集群取 region）
	clusterOf := func(id uint) *model.Cluster {
		if c, ok := cls[id]; ok {
			return c
		}
		var c model.Cluster
		s.db.First(&c, id)
		cls[id] = &c
		return &c
	}
	res := &InspectResult{TotalSlots: len(metaList)}
	for i := range metaList {
		m := &metaList[i]
		cl := clusterOf(m.ClusterID)
		ok := s.volStore(cl, mainVolOf(m.BackupEdition)).Exists(m.GFSPath) &&
			s.volStore(cl, peerVolOf(m.BackupEdition)).Exists(m.PeerGFSPath)
		if ok {
			res.OKCount++
			continue
		}
		mt := "MISSING"
		if exists, sz := s.volStore(cl, mainVolOf(m.BackupEdition)).Stat(m.GFSPath); exists && sz <= s.minFileSize {
			mt = "BROKEN" // 半截/空文件
			res.BrokenCount++
		}
		res.MissingList = append(res.MissingList, model.InspectMissing{
			ClusterID: m.ClusterID, DBID: m.DBID, DBName: m.DBName,
			BackupDate: m.BackupDate, BackupEdition: m.BackupEdition,
			MissingType: mt,
		})
	}

	res.MissingCount = len(res.MissingList)
	ins := model.Inspect{
		InspectDate: time.Now(), TargetDate: target, Scope: "ALL",
		TotalSlots: res.TotalSlots, MissingCount: res.MissingCount,
		BrokenCount: res.BrokenCount, OKCount: res.OKCount, Status: "DONE",
	}
	if err := s.db.Create(&ins).Error; err != nil {
		return nil, err
	}
	res.InspectID = ins.ID
	for i := range res.MissingList {
		res.MissingList[i].InspectID = ins.ID
	}
	if len(res.MissingList) > 0 {
		if err := s.db.Create(&res.MissingList).Error; err != nil {
			return nil, err
		}
	}
	return res, nil
}

// CreateReplenishTask 下发补发指令（对应 POST /api/v1/replenish/task）。防抖由唯一维度保证。
// 管控只记录本地任务并【经 HTTP 推送给外部组件】执行补发；外部补完再回调 CompleteReplenish 闭环。
func (s *Service) CreateReplenishTask(clusterID, dbID uint, dbName string, date time.Time, edition model.BackupEdition) (*model.ReplenishTask, error) {
	// 防抖：同一维度未完成前不重复下发
	var existing model.ReplenishTask
	err := s.db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ? AND status != ?",
		clusterID, dbID, date.UTC().Truncate(24*time.Hour), edition, "SUCCESS").First(&existing).Error
	if err == nil {
		return &existing, nil // 已存在进行中的任务，直接返回（防抖）
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 已归档/归档中的备份不再补发：文件可能已正常清理，或大数据正在拉取
	var guard model.BackupMeta
	if err := s.db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ?",
		clusterID, dbID, date.UTC().Truncate(24*time.Hour), edition).First(&guard).Error; err == nil {
		if guard.ArchiveStatus == model.StatusArchived || guard.ArchiveStatus == model.StatusArchiving {
			return nil, fmt.Errorf("备份已归档/归档中，无需补发: %s", guard.ArchiveStatus)
		}
	}

	task := model.ReplenishTask{
		TaskID:    fmt.Sprintf("REP%d", time.Now().UnixNano()),
		ClusterID: clusterID, DBID: dbID, DBName: dbName,
		BackupDate: date.UTC().Truncate(24 * time.Hour), BackupEdition: edition,
		Status: "RUNNING", MaxRetry: s.retryMax,
	}
	if err := s.db.Create(&task).Error; err != nil {
		return nil, err
	}
	// 指令下发走 HTTP：补发执行方（DB 代理）监听同在 GFS 管理端，按缺失卷机房分发
	if s.Notifier != nil || s.regionSource() != nil {
		// 定位缺失卷路径：以唯一维度查元数据，双卷中不存在的那个即为补发目标
		var meta model.BackupMeta
		_path, _size := "", int64(0)
		missingVol := mainVolOf(edition)
		if err := s.db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ?",
			clusterID, dbID, task.BackupDate, edition).First(&meta).Error; err == nil {
			var cl model.Cluster
			s.db.First(&cl, clusterID)
			if !s.volStore(&cl, mainVolOf(edition)).Exists(meta.GFSPath) {
				_path, _size = meta.GFSPath, meta.FileSizeBytes
			} else if !s.volStore(&cl, peerVolOf(edition)).Exists(meta.PeerGFSPath) {
				_path, _size = meta.PeerGFSPath, meta.PeerFileSizeBytes
				missingVol = peerVolOf(edition)
			} else { // 双卷都在（防抖返回的分支不会到此处），默认补主卷
				_path, _size = meta.GFSPath, meta.FileSizeBytes
			}
		}
		var cl model.Cluster
		s.db.First(&cl, clusterID)
		if err := s.notifyReplenish(&cl, missingVol, notify.ReplenishCandidate{
			TaskID: task.TaskID, ClusterID: clusterID, DBID: dbID, DBName: dbName,
			BackupDate: task.BackupDate, BackupEdition: edition,
			Path: _path, FileSizeBytes: _size,
		}); err != nil {
			return nil, fmt.Errorf("补发指令推送失败: %w", err)
		}
	}
	s.audit("REPLENISH", 0, fmt.Sprintf("task=%s db=%s date=%s",
		task.TaskID, dbName, task.BackupDate.Format("2006-01-02")))
	return &task, nil
}

// notifyReplenish 把补发指令下发给补发执行方（DB 代理，监听同在 GFS 管理端）。
// 多机房（库表 tb_gfs_endpoint / -gfs-map）时按缺失卷所在机房分发到对应管理端；
// 未配置机房映射时回落 s.Notifier（单地址 GFS 管理端）。
func (s *Service) notifyReplenish(cl *model.Cluster, vol string, item notify.ReplenishCandidate) error {
	if src := s.regionSource(); src != nil {
		region := ""
		if cl != nil {
			if vol == sim.VolDR {
				region = cl.DRRegion
			} else {
				region = cl.CityRegion
			}
		}
		if fs, ok := src.For(region).(*gfsclient.HTTPFileStore); ok {
			return notify.NewHTTPTimeout(fs.Base, fs.Timeout).Notify(item)
		}
	}
	if s.Notifier != nil {
		return s.Notifier.Notify(item)
	}
	return fmt.Errorf("补发执行方地址未配置")
}

// CompleteReplenish 补发状态回调（对应 POST /api/v1/replenish/task/status）。
func (s *Service) CompleteReplenish(taskID string, success bool, gfsPath string, size int64) error {
	var t model.ReplenishTask
	if err := s.db.First(&t, "task_id = ?", taskID).Error; err != nil {
		return err
	}
	if success {
		t.Status = "SUCCESS"
		t.GFSPath = gfsPath
		t.FileSizeBytes = size
		// 更新元数据并纳入归档（回到 NOT_ARCHIVED）
		var meta model.BackupMeta
		if err := s.db.Where("cluster_id = ? AND db_id = ? AND backup_date = ? AND backup_edition = ?",
			t.ClusterID, t.DBID, t.BackupDate, t.BackupEdition).First(&meta).Error; err == nil {
			if gfsPath != "" {
				// 回调路径等于对卷路径即补的是对卷，更新对卷字段，不覆盖主卷路径
				if gfsPath == meta.PeerGFSPath {
					meta.PeerGFSPath = gfsPath
					meta.PeerFileSizeBytes = size
				} else {
					meta.GFSPath = gfsPath
					meta.FileSizeBytes = size
				}
			}
			meta.IsReplenished = true
			meta.ReplenishTaskID = taskID
			meta.UploadVerify = model.VerifyVerified
			state.OnMetaReport(&meta)
			s.db.Save(&meta)
		}
	} else {
		t.RetryCount++
		if t.RetryCount >= t.MaxRetry {
			t.Status = "FAILED"
		}
	}
	if err := s.db.Save(&t).Error; err != nil {
		return err
	}
	s.audit("REPLENISH", 0, fmt.Sprintf("task=%s status=%s retry=%d", t.TaskID, t.Status, t.RetryCount))
	return nil
}

// effectiveWindow 归档窗口：库级 → 集群级 → 全局默认 15。
func (s *Service) effectiveWindow(dbm *model.DB, cl *model.Cluster) int {
	if dbm != nil && dbm.ArchiveWindowDays > 0 {
		return dbm.ArchiveWindowDays
	}
	if cl != nil && cl.ArchiveWindowDays > 0 {
		return cl.ArchiveWindowDays
	}
	return 15
}

// effectiveRetention GFS 留存天数：集群级（DB 无留存字段，以其所属集群为准）。
// nil = 未配置 → 默认 10；显式 0 = 归档后即可清理；>0 = 归档后再留 N 天。
func (s *Service) effectiveRetention(cl *model.Cluster) int {
	if cl != nil && cl.RetentionDays != nil {
		return *cl.RetentionDays
	}
	return 10
}

// ArchiveItemFlat 滚动清单条目。
type ArchiveItemFlat struct {
	ArchiveItemID uint   `json:"archiveItemId"`
	BackupID      uint   `json:"backupId"`
	ClusterCode   string `json:"clusterCode"`
	DBName        string `json:"dbName"`
	BackupDate    time.Time    `json:"backupDate"`
	BackupEdition model.BackupEdition `json:"backupEdition"`
	GFSPath       string       `json:"gfsPath"`
	FileSizeBytes int64        `json:"fileSizeBytes"`
	// Region 主卷所在机房（同城卷→cityRegion / 异地卷→drRegion），多机房按此分发归档清单
	Region string `json:"region"`
}

// GenerateRollingList 生成滚动归档清单（对应 POST /api/v1/archive/rolling-list/generate）。
// 按「每个库自身的归档窗口」归档：仅纳入 NOT_ARCHIVED、上传判定通过、且备份日期已过该库
// 有效窗口（库级 ArchiveWindowDays → 集群 → 默认15）的备份。元数据去重，每份只归档一次。
func (s *Service) GenerateRollingList(cutoff time.Time) (string, []ArchiveItemFlat, error) {
	cutoff = cutoff.UTC().Truncate(24 * time.Hour)

	var (
		batchID string
		items   []ArchiveItemFlat
	)
	// 事务内完成「候选筛选 → 条件抢占 → 批次/条目落库」，保证并发触发不会重复下发。
	txErr := s.db.Transaction(func(tx *gorm.DB) error {
		batch := model.ArchiveBatch{
			ID:           fmt.Sprintf("ARC%d", time.Now().UnixNano()),
			GenerateTime: time.Now(), CutoffDate: cutoff,
			Status: "GENERATED",
		}

		// 候选：未归档或上次失败（失败直接由下轮清单重试，不另设复位工序），且上传判定通过
		pendingStates := []model.ArchiveStatus{model.StatusNotArchived, model.StatusArchiveFailed}
		var metas []model.BackupMeta
		if err := tx.Where("archive_status IN ? AND upload_verify = ?",
			pendingStates, model.VerifyVerified).Find(&metas).Error; err != nil {
			return err
		}

		// 按 DBID 懒加载缓存各自的有效窗口与机房（DB/集群只查一次）
		type winRec struct {
			window     int
			code       string
			cityRegion string
			drRegion   string
		}
		winByDB := map[uint]winRec{}
		var winFor = func(m *model.BackupMeta) winRec {
			if r, ok := winByDB[m.DBID]; ok {
				return r
			}
			var dbm model.DB
			if err := tx.First(&dbm, m.DBID).Error; err == nil {
				var cl model.Cluster
				tx.First(&cl, dbm.ClusterID)
				winByDB[m.DBID] = winRec{s.effectiveWindow(&dbm, &cl), cl.ClusterCode, cl.CityRegion, cl.DRRegion}
			}
			return winByDB[m.DBID]
		}

		items = make([]ArchiveItemFlat, 0, len(metas))
		maxWindow := 0
		for i := range metas {
			m := &metas[i]
			rec := winFor(m)
			// 已过该库的归档窗口才纳入
			if m.BackupDate.After(cutoff.AddDate(0, 0, -rec.window)) {
				continue
			}
			// 条件抢占：仍为 未归档/失败 才能置 ARCHIVING 并记下批次；RowsAffected=0 即被并发拿走
			res := tx.Model(&model.BackupMeta{}).
				Where("id = ? AND archive_status IN ?", m.ID, pendingStates).
				Updates(map[string]any{
					"archive_status":  model.StatusArchiving,
					"archive_batch_id": batch.ID,
				})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				continue
			}
			m.ArchiveStatus = model.StatusArchiving
			m.ArchiveBatchID = batch.ID
			// 主卷所在机房：同城卷→cityRegion，异地卷→drRegion（多机房按此分发清单）
			region := rec.cityRegion
			if mainVolOf(m.BackupEdition) == sim.VolDR {
				region = rec.drRegion
			}
			items = append(items, ArchiveItemFlat{
				BackupID: m.ID, ClusterCode: rec.code, DBName: m.DBName,
				BackupDate: m.BackupDate, BackupEdition: m.BackupEdition,
				GFSPath: m.GFSPath, FileSizeBytes: m.FileSizeBytes,
				Region: region,
			})
			if rec.window > maxWindow {
				maxWindow = rec.window
			}
		}

		// 零项不建批次（避免每天累积空批次）
		if len(items) == 0 {
			return nil
		}

		batch.WindowDays = maxWindow
		batch.ItemCount = len(items)
		if err := tx.Create(&batch).Error; err != nil {
			return err
		}
		batchID = batch.ID
		for i := range items {
			it := model.ArchiveItem{
				ArchiveBatchID: batch.ID, BackupID: items[i].BackupID,
				TransferStatus: model.TransferPending,
			}
			if err := tx.Create(&it).Error; err != nil {
				return err
			}
			items[i].ArchiveItemID = it.ID
		}
		return nil
	})
	if txErr != nil {
		return "", nil, txErr
	}
	// 留痕：批次纳入流水（按备份逐条）
	for _, it := range items {
		s.audit("ROLLING", it.BackupID, fmt.Sprintf("batch=%s path=%s", batchID, it.GFSPath))
	}

	// 清单下发走 HTTP（事务外，避免外部调用持有事务）：推给大数据组件自治拉取→写HDFS→回调。
	// 多机房（tb_gfs_endpoint）按主卷机房分组逐机房推送，失败只回滚该机房条目；单地址整批推送。
	if src := s.regionSource(); src != nil {
		byRegion := map[string][]ArchiveItemFlat{}
		var order []string
		for _, it := range items {
			if _, ok := byRegion[it.Region]; !ok {
				order = append(order, it.Region)
			}
			byRegion[it.Region] = append(byRegion[it.Region], it)
		}
		for _, region := range order {
			group := byRegion[region]
			var cands []notify.ArchiveCandidate
			for _, it := range group {
				cands = append(cands, notify.ArchiveCandidate{
					ArchiveItemID: it.ArchiveItemID, BackupID: it.BackupID,
					GFSPath: it.GFSPath, FileSizeBytes: it.FileSizeBytes,
				})
			}
			fs, ok := src.For(region).(*gfsclient.HTTPFileStore)
			if ok {
				if err := notify.NewHTTPArchiveTimeout(fs.Base, fs.Timeout).NotifyArchive(batchID, cands); err != nil {
					s.failBatch(batchID, group)
					return "", nil, fmt.Errorf("归档清单推送失败(机房 %s): %w", region, err)
				}
				continue
			}
			// 机房未配置端点：回落单地址出口；连单地址都没有则该机房条目回滚重试
			if s.ArchiveNotifier == nil {
				s.failBatch(batchID, group)
				return "", nil, fmt.Errorf("归档清单推送失败: 机房 %s 无可用管理端", region)
			}
			if err := s.ArchiveNotifier.NotifyArchive(batchID, cands); err != nil {
				s.failBatch(batchID, group)
				return "", nil, fmt.Errorf("归档清单推送失败(机房 %s): %w", region, err)
			}
		}
	} else if s.ArchiveNotifier != nil {
		cands := make([]notify.ArchiveCandidate, 0, len(items))
		for _, it := range items {
			cands = append(cands, notify.ArchiveCandidate{
				ArchiveItemID: it.ArchiveItemID, BackupID: it.BackupID,
				GFSPath: it.GFSPath, FileSizeBytes: it.FileSizeBytes,
			})
		}
		if err := s.ArchiveNotifier.NotifyArchive(batchID, cands); err != nil {
			s.failBatch(batchID, items)
			return "", nil, fmt.Errorf("归档清单推送失败: %w", err)
		}
	}
	s.db.Model(&model.ArchiveBatch{}).Where("id = ?", batchID).Update("status", "PUSHED")
	return batchID, items, nil
}

// failBatch 归档清单推送失败后的状态恢复：批次置 FAILED，批次内仍为 ARCHIVING 的
// 备份经状态机置 ARCHIVE_FAILED（ARCHIVING→ARCHIVE_FAILED 合法），下轮扫描复位后重试。
func (s *Service) failBatch(batchID string, items []ArchiveItemFlat) {
	s.db.Model(&model.ArchiveBatch{}).Where("id = ?", batchID).Update("status", "FAILED")
	for _, it := range items {
		var m model.BackupMeta
		if s.db.First(&m, it.BackupID).Error == nil && m.ArchiveStatus == model.StatusArchiving {
			if e := state.OnArchiveResult(&m, false); e == nil {
				s.db.Save(&m)
			}
		}
	}
}

// ArchiveResult 归档结果上报（对应 POST /api/v1/archive/result）。
// 由大数据组件自治拉取写 HDFS 后回调：管控本身【不读 GFS 卷、不写 HDFS】，
// 仅核对回调的 hdfsSize 与清单一致后置 ARCHIVED，放行 GFS 清理。
func (s *Service) ArchiveResult(archiveItemID uint, hdfsPath string, hdfsSizeBytes int64) error {
	var item model.ArchiveItem
	if err := s.db.First(&item, archiveItemID).Error; err != nil {
		return err
	}
	var meta model.BackupMeta
	if err := s.db.First(&meta, item.BackupID).Error; err != nil {
		return err
	}

	// 判定：大数据已在 HDFS 侧完成存在+size 校验；回调 size>0 且与清单(来源 GFS size)一致。
	sizeValid := hdfsSizeBytes > 0 && hdfsSizeBytes == meta.FileSizeBytes

	item.TransferStatus = model.TransferDone
	meta.HDFSPath = hdfsPath
	meta.HDFSSizeBytes = hdfsSizeBytes
	if sizeValid {
		if err := state.OnArchiveResult(&meta, true); err != nil {
			return err
		}
		now := time.Now()
		meta.ArchivedAt = &now
		meta.CanDelete = true
	} else {
		state.OnArchiveResult(&meta, false)
		item.TransferStatus = model.TransferFailed
	}

	s.db.Save(&item)
	if err := s.db.Save(&meta).Error; err != nil {
		return err
	}
	// 留痕：链路B HDFS 归档 size 校验 + 归档结果流水
	verifyResult := "FAILED"
	if sizeValid {
		verifyResult = "VERIFIED"
	}
	s.logVerify(meta.ID, "ARCHIVE", mainVolOf(meta.BackupEdition), "ARCHIVE_SIZE",
		meta.FileSizeBytes, hdfsSizeBytes, verifyResult)
	s.audit("ARCHIVE", meta.ID, fmt.Sprintf("%s hdfsSize=%d %s",
		meta.ArchiveStatus, hdfsSizeBytes, verifyResult))
	return nil
}

// QueryArchiveStatus 归档状态查询（对应 GET /api/v1/backup/archive-status）。用于 GFS 清理判定。
// 返回 (归档状态, 是否可删, 归档时间, 计划删除时间)。canDelete：已归档 且 距归档已满该集群留存天数。
func (s *Service) QueryArchiveStatus(clusterCode, dbName string, date time.Time, edition model.BackupEdition) (string, bool, *time.Time, *time.Time, error) {
	var cl model.Cluster
	if err := s.db.Where("cluster_code = ?", clusterCode).First(&cl).Error; err != nil {
		return "", false, nil, nil, err
	}
	var meta model.BackupMeta
	if err := s.db.Where("cluster_id = ? AND db_name = ? AND backup_date = ? AND backup_edition = ?",
		cl.ID, dbName, date.UTC().Truncate(24*time.Hour), edition).First(&meta).Error; err != nil {
		return "", false, nil, nil, err
	}
	retention := s.effectiveRetention(&cl)
	var pending *time.Time
	canDelete := false
	if meta.ArchiveStatus == model.StatusArchived && meta.ArchivedAt != nil {
		d := meta.ArchivedAt.AddDate(0, 0, retention)
		pending = &d
		canDelete = !d.After(time.Now())
	}
	return string(meta.ArchiveStatus), canDelete, meta.ArchivedAt, pending, nil
}

// GfsStatSize 返回 GFS 上某路径的存在性与实测 size（供接口回显，不做判定）。
// 多机房模式下按集群+版本推主卷所属机房分发；集群查不到时回落默认访问面。
func (s *Service) GfsStatSize(clusterID uint, edition model.BackupEdition, path string) (exists bool, size int64) {
	var cl model.Cluster
	if err := s.db.First(&cl, clusterID).Error; err == nil {
		return s.volStore(&cl, mainVolOf(edition)).Stat(path)
	}
	return s.fs.Stat(path)
}

// Cleanup 执行 GFS 清理。触发方由 CleanupMode 决定：
//   - mgr（默认）：管控遍历已归档元数据，经 fs.Delete 让 GFS 删文件（当前实现）。
//   - gfs（原始设想）：管控只做"放行判定"（哪些已满留存、可删），不实际删除；
//     真正的删除由外部 GFS 组件自治执行（GFS 删前经 /archive-status 或本放行判定自查）。
func (s *Service) Cleanup() (int, error) {
	var metas []model.BackupMeta
	if err := s.db.Where("archive_status = ?", model.StatusArchived).Find(&metas).Error; err != nil {
		return 0, err
	}
	now := time.Now()
	mgrMode := s.CleanupMode != "gfs" // 默认 mgr
	deleted := 0
	for _, m := range metas {
		if m.ArchivedAt == nil {
			continue
		}
		var cl model.Cluster
		s.db.First(&cl, m.ClusterID)
		deadline := m.ArchivedAt.AddDate(0, 0, s.effectiveRetention(&cl))
		if deadline.After(now) {
			continue // 未满留存天数，暂不清理
		}
		if !mgrMode {
			// GFS 自治模式：管控仅放行判定（满足删除条件的备份保持原状态），
			// 真正的删除由外部 GFS 组件按归档状态(/archive-status canDelete)自查执行。
			continue
		}
		// 双卷删除都成功才记账；任一失败不写 DeletedAt，留待下次清理重试
		if err := s.volStore(&cl, mainVolOf(m.BackupEdition)).Delete(m.GFSPath); err != nil {
			continue
		}
		if err := s.volStore(&cl, peerVolOf(m.BackupEdition)).Delete(m.PeerGFSPath); err != nil {
			continue
		}
		m.DeletedAt = &now
		m.CanDelete = false
		s.db.Save(&m)
		s.audit("CLEAN", m.ID, fmt.Sprintf("gfs=%s peer=%s", m.GFSPath, m.PeerGFSPath))
		deleted++
	}
	return deleted, nil
}

// archiveStuckAfter 归档卡死判定阈值：超过此时长未收到大数据结果视为回调丢失。
const archiveStuckAfter = 6 * time.Hour

// SweepArchive 归档超时兜底：ARCHIVING 超过 archiveStuckAfter 仍未收到大数据结果
// （大数据组件挂了、回调丢失）才置 ARCHIVE_FAILED；之后由下轮滚动清单直接重新纳入。
// 失败重试不经此处，避免与仍在正常进行的慢拉取赛跑。
func (s *Service) SweepArchive() (timedOut int, err error) {
	cutoff := time.Now().Add(-archiveStuckAfter)
	var hanging []model.BackupMeta
	if err = s.db.Where("archive_status = ? AND updated_at < ?", model.StatusArchiving, cutoff).
		Find(&hanging).Error; err != nil {
		return 0, err
	}
	for i := range hanging {
		m := &hanging[i]
		if e := state.OnArchiveResult(m, false); e != nil { // ARCHIVING -> ARCHIVE_FAILED
			continue
		}
		if e := s.db.Save(m).Error; e == nil {
			s.audit("SWEEP", m.ID, "ARCHIVING -> ARCHIVE_FAILED")
			timedOut++
		}
	}
	return timedOut, nil
}

// validIP 校验 IPv4/IPv6 地址格式。
func validIP(s string) bool {
	return net.ParseIP(s) != nil
}

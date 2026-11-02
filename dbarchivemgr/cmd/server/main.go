// Command server 启动 DBArchiveManager HTTP 服务。
// 全链路以纯内存模拟桩运行：无需真实 MySQL/GlusterFS/HDFS 环境。
// 管控（本服务）为调度枢纽：不直连文件系统，全部经 HTTP 与 GFS / 大数据组件交互。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"github.com/dbarchivemgr/dbarchivemgr/internal/api"
	"github.com/dbarchivemgr/dbarchivemgr/internal/auth"
	"github.com/dbarchivemgr/dbarchivemgr/internal/bigdata"
	"github.com/dbarchivemgr/dbarchivemgr/internal/demo"
	"github.com/dbarchivemgr/dbarchivemgr/internal/gfsclient"
	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/notify"
	"github.com/dbarchivemgr/dbarchivemgr/internal/scheduler"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
	"github.com/dbarchivemgr/dbarchivemgr/internal/store"
	"github.com/dbarchivemgr/dbarchivemgr/internal/web"
)

// components 管控对外的组件接线结果。
type components struct {
	svc             *service.Service
	controlBase     string           // 大数据回调管控的地址（生产经 -public-url 给定）
	closeStubs      func()           // 关闭内置桩（外部模式为 no-op）
	usesExternalGFS bool             // 是否直连外部 GFS 组件
	bd              *bigdata.BigData // 内置大数据桩（演示模式非 nil，需回填回调地址）
}

func main() {
	addr := flag.String("addr", ":8090", "HTTP 监听地址")
	dbDSN := flag.String("dsn", "file:dbarchivemgr.db?cache=shared", "元数据库 DSN")

	// 大数据组件回调管控用的对外地址（生产需为可达的公网/内网稳定地址）；
	// 缺省回落到 -addr 的监听地址（演示/单机适用）。
	// GFS 管理端地址不走启动参数：在管理后台「机房管理」配置（"*" 兜底行=单地址模式），
	// 或用环境变量 DBAM_GFS_MAP；均未配置则起内置桩（纯演示）。
	publicURL := flag.String("public-url", envOr("DBAM_PUBLIC_URL", ""), "管控对外地址，供大数据回调(如 http://dbam:8090；空=取监听地址)")

	// 定时调度：默认演示秒级，便于肉眼观察；生产用 -schedule-demo=false 切每日(凌晨)。
	// 巡检/滚窗/扫描的 cron 与巡检天数均为包内常量，无需配置。
	scheduleDemo := flag.Bool("schedule-demo", true, "演示模式：秒级触发巡检/滚动；false 用每日凌晨 cron")

	// CAS 单点登录（CAS 2/3 标准协议）：未配置 -cas-url 时为演示模式，不鉴权。
	casURL := flag.String("cas-url", envOr("DBAM_CAS_URL", ""),
		"CAS 服务根地址(如 https://cas.example.com/cas；空=演示模式不鉴权)")
	flag.Parse()

	// 部署低频配置只走环境变量（不上命令行）：
	//   DBAM_GFS_MAP           机房→GFS管理端映射(JSON)，多机房也可在界面「机房管理」配置
	//   DBAM_CLEANUP_MODE      清理触发方：gfs(默认)=GFS自治清理 / mgr=管控触发删除(应急)
	//   DBAM_CAS_VALIDATE_PATH CAS ticket 校验路径(默认 /p3/proxyValidate，CAS 2.x 用 /serviceValidate)
	//   DBAM_MACHINE_TOKEN     机器回调共享密钥(密钥不上命令行，ps 不可见)
	gfsMap, err := parseGFSMap(envOr("DBAM_GFS_MAP", ""))
	if err != nil {
		log.Fatalf("解析机房映射 DBAM_GFS_MAP 失败: %v", err)
	}
	cleanupMode := envOr("DBAM_CLEANUP_MODE", "gfs")
	casValidate := envOr("DBAM_CAS_VALIDATE_PATH", "/p3/proxyValidate")

	// 元数据库（SQLite，纯本地）
	db, err := store.New(*dbDSN)
	if err != nil {
		log.Fatalf("打开元数据库失败: %v", err)
	}

	// =====================================================================
	// 装配组件：管控出站只对接 GFS 管理端（stat/delete + 归档清单下发）与
	// 补发执行方 DB 代理（补发指令）；不直连大数据——归档结果由大数据回调进来。
	// GFS 地址来自机房管理库表或 DBAM_GFS_MAP；缺省全用内置内存桩（演示）。
	// =====================================================================
	cps := bootstrap(*publicURL, cleanupMode, gfsMap, db)
	defer cps.closeStubs()
	svc := cps.svc

	// 定时调度（cron 规格与巡检天数为 scheduler 包内常量，demo=false 切生产节奏）
	sched := scheduler.New(svc, db, *scheduleDemo)
	cronS := cron.New(cron.WithSeconds())
	if err := sched.Start(cronS); err != nil {
		log.Fatalf("启动定时调度失败: %v", err)
	}
	defer cronS.Stop()

	r := gin.Default()
	// CAS 登录：/auth/* 先注册（自身不走鉴权），其余路由全部纳入 Require
	// 机器回调密钥只从环境变量读取（避免密钥出现在命令行 ps 输出里）。
	machineToken := envOr("DBAM_MACHINE_TOKEN", "")
	authMw := auth.New(*casURL, casValidate, *publicURL)
	authMw.MachineToken = machineToken
	authMw.Register(r)
	r.Use(authMw.Require())
	if *casURL == "" {
		log.Printf("[装配] 登录: 演示模式（未配置 -cas-url，不鉴权；生产对接 CAS 单点登录）")
	} else {
		log.Printf("[装配] 登录: CAS 单点登录 → %s（校验 %s；机器回调 /api/v1/archive/result、/api/v1/replenish/task/status 已放行）",
			*casURL, casValidate)
	}
	(&api.Handler{Svc: svc, DB: db, Sched: sched}).Register(r)
	web.Register(r) // 管理后台静态页（/ui，go:embed 打进二进制）

	// 监听接管后，把管控对外地址回填给大数据组件，供其回调 /api/v1/archive/result 等
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	ctrlBase := "http://" + ln.Addr().String()
	if cps.controlBase != "" {
		ctrlBase = cps.controlBase
	}
	// 演示模式下把回调地址回填给内置大数据桩，使其归档结果/补发状态能回调管控。
	if cps.bd != nil {
		cps.bd.SetControl(ctrlBase)
		cps.bd.SetMachineToken(machineToken)
	}
	log.Printf("[装配] 大数据回调管控: %s  (外部GFS=%v)", ctrlBase, cps.usesExternalGFS)

	log.Printf("DBArchiveManager listening on %s", *addr)
	log.Printf("  管理后台: %s/ui/", "http://"+(*addr))
	log.Printf("  GlusterFS 双卷路径: /gfs/<cluster>/<db>/<date>/{city|dr}/full.sql.gz")

	// 平滑退出：收到中断/终止信号时停止 cron 并退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("收到退出信号，停止调度并关闭服务")
		cronS.Stop()
		os.Exit(0)
	}()

	if err := r.RunListener(ln); err != nil {
		log.Fatalf("服务退出: %v", err)
	}
}

// envOr 读取环境变量，为空时返回缺省值。
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseGFSMap 解析机房→GFS 管理端映射：内联 JSON（以 "{" 开头）或 @file 指向 JSON 文件。
// 形如 {"杭州同城":"http://gfs-hz:8200","乌兰察布":"http://gfs-wlc:8210","*":"http://gfs:8200"}，
// 键 "*" 为未匹配机房的兜底管理端。
func parseGFSMap(spec string) (map[string]string, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	data := []byte(spec)
	if strings.HasPrefix(spec, "@") {
		b, err := os.ReadFile(strings.TrimPrefix(spec, "@"))
		if err != nil {
			return nil, err
		}
		data = b
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("应为 {\"机房\":\"url\"} JSON: %w", err)
	}
	return m, nil
}

// bootstrap 装配管控对外组件的访问面。
//
// 出站只有一方（管控不直连大数据，大数据只回调管控）：GFS 管理端。三类出站都打管理端：
//   - stat/delete：巡检与上报双卷复核（多机房时按集群 cityRegion/drRegion 分发）；
//   - 归档清单 POST /v1/archive/notify：由 GFS 转发大数据；
//   - 补发指令 POST /v1/replenish/notify：DB 代理监听同在 GFS 管理端，由其转交 DB 侧重跑备份。
//
// GFS 地址只有一个来源：管理后台「机房管理」库表（"*" 兜底行=单地址模式）或 DBAM_GFS_MAP。
// 配置了任一 → 外部模式（不起内置桩、不注入种子）；均未配置 → 内置桩演示模式。
func bootstrap(publicURL, cleanupMode string, gfsMap map[string]string, db *gorm.DB) *components {
	c := &components{closeStubs: func() {}}

	// ---- GFS 访问面 ----
	var epCount int64
	db.Model(&model.GfsEndpoint{}).Count(&epCount)
	external := len(gfsMap) > 0 || epCount > 0

	var stubGFS *sim.GFS
	var bdSrv *httptest.Server
	if external {
		c.usesExternalGFS = true
		log.Printf("[装配] 外部 GFS 模式（机房端点：库表 %d 条 / DBAM_GFS_MAP %d 条）", epCount, len(gfsMap))
	} else {
		stubGFS = sim.NewGFS("/gfs")
		// 内置大数据桩：演示模式既扮大数据归档组件（写 HDFS 后回调管控），
		// 又在补发链路中扮 DB 代理替身（按指令在 GFS 重写缺失卷）。
		bd := bigdata.New(stubGFS, sim.NewHDFS())
		bdSrv = httptest.NewServer(bd.Handler())
		c.bd = bd
	}

	// 演示模式：GFS 桩包一层转发——/v1/archive/notify、/v1/replenish/notify 转给大数据桩，
	// 复刻设计链路「管控下发清单/指令给 GFS → GFS 转发大数据/DB 代理执行」。
	// 外部模式：机房未匹配且无兜底时按"文件不存在"处理（照常进补发流程，不 panic）。
	fs := gfsclient.NewMissing()
	var stubBase string
	if stubGFS != nil {
		var gfsSrv *httptest.Server
		if bdSrv != nil {
			gfsSrv = httptest.NewServer(forwardControlNotify(stubGFS.Handler(), bdSrv.URL))
		} else {
			gfsSrv = httptest.NewServer(stubGFS.Handler())
		}
		stubBase = gfsSrv.URL
		fs = gfsclient.NewHTTP(stubBase)
		log.Printf("[装配] GFS 组件 HTTP(桩): %s", stubBase)
		c.closeStubs = gfsSrv.Close
	}
	svc := service.New(db, fs)
	svc.CleanupMode = cleanupMode
	if cleanupMode == "gfs" {
		log.Printf("[装配] 清理触发方=gfs（管控仅放行判定，GFS 自治清理）")
	} else {
		log.Printf("[装配] 清理触发方=mgr（管控经 HTTP 触发删除）")
	}

	// ---- 多机房 GFS 路由（可选）：DBAM_GFS_MAP 静态映射；库表（界面配置）在 service 内优先 ----
	// 管控按集群 cityRegion(同城卷)/drRegion(异地卷) 分发 stat/delete；未匹配机房回落兜底。
	if len(gfsMap) > 0 {
		svc.Regions = gfsclient.NewRegionRouter("", gfsMap)
		log.Printf("[装配] 多机房 GFS 路由(DBAM_GFS_MAP): %d 个机房映射 %+v，兜底=映射首个/\"*\"键", len(gfsMap), gfsMap)
	}

	// ---- 出站：归档清单（GFS 转发大数据）、补发指令（DB 代理监听同在管理端） ----
	// 外部模式出站地址全部来自机房端点路由（service 按机房分发），不设单地址兜底；
	// 演示模式单地址指向内置桩。
	if stubGFS != nil {
		svc.ArchiveNotifier = notify.NewHTTPArchive(stubBase)
		svc.Notifier = notify.NewHTTP(stubBase)
		log.Printf("[装配] 出站 → GFS 管理端(桩) %s：归档清单（GFS 转发大数据）+ 补发指令（DB 代理监听同在管理端）", stubBase)
	} else {
		log.Printf("[装配] 出站 → 机房端点路由（机房管理库表 / DBAM_GFS_MAP）：归档清单+补发指令按机房分发")
	}

	// 纯演示（GFS 为内置桩）时注入种子数据，便于一键跑通全链路。
	if stubGFS != nil {
		demo.Seed(svc, stubGFS, time.Now())
	}

	c.svc = svc
	c.controlBase = publicURL // 生产由运维显式给定管控对外地址（用于大数据回调）
	return c
}

// forwardControlNotify 包装 GFS 桩：把 /v1/archive/notify、/v1/replenish/notify 转发给大数据桩，
// 其余请求原样交给 GFS——复刻「管控下发清单/指令给 GFS → GFS 转发大数据/DB 代理执行」的设计链路。
func forwardControlNotify(gfs http.Handler, bdURL string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/archive/notify" || r.URL.Path == "/v1/replenish/notify" {
			req, err := http.NewRequestWithContext(r.Context(), r.Method, bdURL+r.URL.Path, r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			for k, vv := range resp.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		gfs.ServeHTTP(w, r)
	})
}

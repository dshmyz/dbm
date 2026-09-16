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
	windowDays := flag.Int("window-days", 15, "巡检/滚窗共用的归档窗口天数")

	// 外部组件地址（线上可配，flag 优先，其次环境变量；均未配置则用内置内存桩）。
	// 出站全部指向 GFS 管理端：stat/delete、归档清单转发、补发指令（DB 代理监听同在管理端）。
	// 不直连大数据——归档结果由大数据回调进来。
	gfsURL := flag.String("gfs-url", envOr("DBAM_GFS_URL", ""), "外部 GFS 管理端地址(stat/delete + 归档清单转发 + 补发指令，如 http://gfs:8200；空=内置桩)")
	// 多机房部署：各机房独立 GlusterFS 集群时，按机房映射多个管理端；
	// 管控按集群 cityRegion(同城卷)/drRegion(异地卷) 分发 stat/delete，未匹配机房回落 -gfs-url。
	gfsMapSpec := flag.String("gfs-map", envOr("DBAM_GFS_MAP", ""),
		"机房→GFS管理端 映射(JSON 内联或 @file，如 '{\"杭州同城\":\"http://gfs-hz:8200\",\"乌兰察布\":\"http://gfs-wlc:8210\",\"*\":\"http://gfs:8200\"}'；\"*\" 为兜底)")
	// 大数据组件回调管控用的对外地址（生产需为可达的公网/内网稳定地址）；
	// 缺省回落到 -addr 的监听地址（演示/单机适用）。
	publicURL := flag.String("public-url", envOr("DBAM_PUBLIC_URL", ""), "管控对外地址，供大数据回调(如 http://dbam:8090；空=取监听地址)")
	// 清理触发方：mgr(默认/现状)=管控经HTTP让GFS删；gfs(原始设想)=管控仅放行判定，GFS自治清理。
	cleanupMode := flag.String("cleanup-mode", envOr("DBAM_CLEANUP_MODE", "mgr"), "清理触发方: mgr=管控触发删除(默认) / gfs=GFS自治清理")

	// 定时调度：默认演示秒级，便于肉眼观察；生产用 -schedule-demo=false 切每日(凌晨)。
	scheduleDemo := flag.Bool("schedule-demo", true, "演示模式：秒级触发巡检/滚动；false 用每日凌晨 cron")
	inspectSpec := flag.String("schedule-spec", "", "巡检+补发 cron 规格(可空→按 demo/prod 取默认)")
	rollingSpec := flag.String("rolling-spec", "", "滚动清单 cron 规格(可空→按 demo/prod 取默认)")

	// CAS 单点登录（CAS 2/3 标准协议）：未配置 -cas-url 时为演示模式，不鉴权。
	casURL := flag.String("cas-url", envOr("DBAM_CAS_URL", ""),
		"CAS 服务根地址(如 https://cas.example.com/cas；空=演示模式不鉴权)")
	casValidate := flag.String("cas-validate-path", envOr("DBAM_CAS_VALIDATE_PATH", "/p3/proxyValidate"),
		"CAS ticket 校验路径(CAS 3.x: /p3/proxyValidate；CAS 2.x 服务端可用 /serviceValidate)")
	flag.Parse()

	// 机房→GFS 管理端映射：支持内联 JSON 或 @file 指向 JSON 文件
	gfsMap, err := parseGFSMap(*gfsMapSpec)
	if err != nil {
		log.Fatalf("解析机房映射 -gfs-map 失败: %v", err)
	}

	// 元数据库（SQLite，纯本地）
	db, err := store.New(*dbDSN)
	if err != nil {
		log.Fatalf("打开元数据库失败: %v", err)
	}

	// =====================================================================
	// 装配组件：管控出站只对接 GFS 管理端（stat/delete + 归档清单下发）与
	// 补发执行方 DB 代理（补发指令）；不直连大数据——归档结果由大数据回调进来。
	// 线上默认归一到 -gfs-url（或 -component-url 统一网关）；缺省全用内置内存桩（演示）。
	// =====================================================================
	cps := bootstrap(*publicURL, *gfsURL, *cleanupMode, gfsMap, db)
	defer cps.closeStubs()
	svc := cps.svc

	// 定时调度
	is := resolveSpec(*inspectSpec, *scheduleDemo, "0 0 3 * * *", "@every 15s") // 巡检: 生产每日3点 / 演示15s
	rs := resolveSpec(*rollingSpec, *scheduleDemo, "0 0 2 * * *", "@every 30s") // 滚窗: 生产每日2点 / 演示30s
	sched := scheduler.New(svc, db, *windowDays, is, rs)
	cronS := cron.New(cron.WithSeconds())
	if err := sched.Start(cronS); err != nil {
		log.Fatalf("启动定时调度失败: %v", err)
	}
	defer cronS.Stop()
	log.Printf("定时调度已启动: 巡检=%s 滚动清单=%s", is, rs)

	r := gin.Default()
	// CAS 登录：/auth/* 先注册（自身不走鉴权），其余路由全部纳入 Require
	authMw := auth.New(*casURL, *casValidate, *publicURL)
	authMw.Register(r)
	r.Use(authMw.Require())
	if *casURL == "" {
		log.Printf("[装配] 登录: 演示模式（未配置 -cas-url，不鉴权；生产对接 CAS 单点登录）")
	} else {
		log.Printf("[装配] 登录: CAS 单点登录 → %s（校验 %s；机器回调 /api/v1/archive/result、/api/v1/replenish/task/status 已放行）",
			*casURL, *casValidate)
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

// resolveSpec 解析 cron 规格：显式指定优先；否则按 demo/prod 取默认。
func resolveSpec(explicit string, demo bool, prodSpec, demoSpec string) string {
	if explicit != "" {
		return explicit
	}
	if demo {
		return demoSpec
	}
	return prodSpec
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
// 出站只有一方（管控不直连大数据，大数据只回调管控）：GFS 管理端
// （-gfs-url / -gfs-map / 机房管理界面）。三类出站都打管理端：
//   - stat/delete：巡检与上报双卷复核（多机房时按集群 cityRegion/drRegion 分发）；
//   - 归档清单 POST /v1/archive/notify：由 GFS 转发大数据；
//   - 补发指令 POST /v1/replenish/notify：DB 代理监听同在 GFS 管理端，由其转交 DB 侧重跑备份。
//
// 线上模式：给了 -gfs-url 时管控直连外部 GFS（不起内置桩、不注入种子）。
// 演示/本地模式：没配时起内置桩（sim.GFS / bigdata），管控经 httptest 地址经 HTTP 与桩交互，
// 并注入种子数据、回填回调地址，保证 demo.sh 无需改参即可跑通。
func bootstrap(publicURL, gfsURL, cleanupMode string, gfsMap map[string]string, db *gorm.DB) *components {
	c := &components{closeStubs: func() {}}

	// ---- GFS 访问面 ----
	// 给了外部 GFS 地址 → 直连外部；否则启动内置 sim.GFS 桩。
	gfsExternal := gfsURL != ""
	var stubGFS *sim.GFS
	var bdSrv *httptest.Server
	if gfsURL != "" {
		c.usesExternalGFS = true
		log.Printf("[装配] 外部 GFS 管理端: %s", gfsURL)
	} else {
		stubGFS = sim.NewGFS("/gfs")
		// 内置大数据桩：演示模式既扮大数据归档组件（写 HDFS 后回调管控），
		// 又在补发链路中扮 DB 代理替身（按指令在 GFS 重写缺失卷）。
		bd := bigdata.New(stubGFS, sim.NewHDFS())
		bdSrv = httptest.NewServer(bd.Handler())
		c.bd = bd
	}

	// GFS server：演示模式下包一层转发——/v1/archive/notify、/v1/replenish/notify 转给大数据桩，
	// 复刻设计链路「管控下发清单/指令给 GFS → GFS 转发大数据/DB 代理执行」。
	var gfsSrv *httptest.Server
	if stubGFS != nil {
		if bdSrv != nil {
			gfsSrv = httptest.NewServer(forwardControlNotify(stubGFS.Handler(), bdSrv.URL))
		} else {
			gfsSrv = httptest.NewServer(stubGFS.Handler())
		}
		gfsURL = gfsSrv.URL
		log.Printf("[装配] GFS 组件 HTTP(桩): %s", gfsSrv.URL)
		c.closeStubs = gfsSrv.Close
	}
	fs := gfsclient.NewHTTP(gfsURL)
	svc := service.New(db, fs)
	svc.CleanupMode = cleanupMode
	if cleanupMode == "gfs" {
		log.Printf("[装配] 清理触发方=gfs（管控仅放行判定，GFS 自治清理）")
	} else {
		log.Printf("[装配] 清理触发方=mgr（管控经 HTTP 触发删除）")
	}

	// ---- 多机房 GFS 路由（可选）：各机房独立 GlusterFS 集群时按机房映射多个管理端 ----
	// 管控按集群 cityRegion(同城卷)/drRegion(异地卷) 分发 stat/delete；未匹配机房回落兜底。
	if len(gfsMap) > 0 {
		def := ""
		if gfsExternal {
			def = gfsURL
		}
		svc.Regions = gfsclient.NewRegionRouter(def, gfsMap)
		c.usesExternalGFS = true // 映射指向外部管理端，不再按纯演示注入种子数据
		log.Printf("[装配] 多机房 GFS 路由: %d 个机房映射 %+v，兜底=%s（未配则取映射首个/\"*\"键）",
			len(gfsMap), gfsMap, def)
	}

	// ---- 出站统一指向 GFS 管理端：归档清单（GFS 转发大数据）、补发指令（DB 代理监听同在管理端） ----
	// 多机房时补发指令由 service 按缺失卷机房分发（notifyReplenish），这里单地址为兜底。
	svc.ArchiveNotifier = notify.NewHTTPArchive(gfsURL)
	svc.Notifier = notify.NewHTTP(gfsURL)
	log.Printf("[装配] 出站 → GFS 管理端 %s：归档清单（GFS 转发大数据）+ 补发指令（DB 代理监听同在管理端）", gfsURL)

	// 纯演示（GFS 为内置桩）时注入种子数据，便于一键跑通全链路。
	if stubGFS != nil && !c.usesExternalGFS {
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

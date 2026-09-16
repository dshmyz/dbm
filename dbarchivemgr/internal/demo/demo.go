// Package demo 提供无真实环境时的种子数据与全链路演示流程。
// 通过纯内存模拟桩跑通：注册集群 → 绑定库 → 上报元数据(双卷写入) → 巡检
// → 补发 → 滚动清单 → 归档结果 → 状态查询 → 清理。
package demo

import (
	"log"
	"time"

	"github.com/dbarchivemgr/dbarchivemgr/internal/model"
	"github.com/dbarchivemgr/dbarchivemgr/internal/service"
	"github.com/dbarchivemgr/dbarchivemgr/internal/sim"
)

// Seed 写入一个演示集群/库/实例IP，并在 GFS 双卷生成若干历史备份文件 + 对应元数据。
// 路径首段为实例 IP（真实环境备份文件即以 IP 标识），按 IP 反查集群上报告警。
func Seed(svc *service.Service, gfs *sim.GFS, now time.Time) {
	cl, err := svc.RegisterCluster(&model.Cluster{
		ClusterCode: "CL-MYSQL-01", ClusterName: "核心交易库集群",
		PrimaryRegion: "杭州A", DRRegion: "杭州B", CityRegion: "杭州同城",
		// 留存 0 天：归档后立即满足保留条件，让演示的清理步骤能删到已归档备份
		//（真实生产按需配 retentionDays，归档后留存 N 天才清理）。
		RetentionDays: model.IntPtr(0), ArchiveWindowDays: 15,
	})
	if err != nil {
		log.Fatalf("注册集群失败: %v", err)
	}
	// 演示「库级自定义路径模板」：orders 库多用一层 prod/ 分隔、文件名带日分。
	// 其余库未配模板时回退到集群/全局默认模板。
	dbm, err := svc.BindDB(&model.DB{
		ClusterID: cl.ID, DBName: "orders", ArchiveWindowDays: 15,
		PathTemplate: "{root}/prod/{cluster}/{db}/{date}/{vol}/orders.sql.gz",
	})
	if err != nil {
		log.Fatalf("绑定库失败: %v", err)
	}

	// 给集群挂实例 IP：备份文件/路径用 IP 标识，据此反查集群。
	hostIP := "10.1.2.3"
	if _, err := svc.AddClusterInstance(cl.ID, hostIP, "db-orders-01", "MASTER"); err != nil {
		log.Fatalf("挂载实例IP失败: %v", err)
	}

	// 生成近 15 天内逐日「双卷」备份文件（按库级模板生成路径），并上报元数据。
	// 唯一维度 (cluster_id, db_id, backup_date, edition) 幂等覆盖。
	for i := 1; i <= 15; i++ {
		date := now.AddDate(0, 0, -i)
		day := date.Format("2006-01-02")
		cityPath, drPath := svc.BuildPathForDB(dbm, cl, hostIP, day, model.EditionSameCity)
		_ = gfs.WriteFull(cityPath, 64*1024*1024) // 同城卷
		_ = gfs.WriteFull(drPath, 64*1024*1024)   // 异地卷
		if _, err := svc.ReportBackupMeta(hostIP, dbm.DBName, date,
			model.EditionSameCity, cityPath, drPath, 0); err != nil {
			log.Fatalf("上报元数据失败(%s): %v", day, err)
		}
	}

	// 故意制造一个「从卷缺失」：2 天前只保留主卷，删除异地卷。
	// 同步只上报主卷 → 双卷存在校验不过，引导通过补发起量。
	missDate := now.AddDate(0, 0, -2)
	_, drPath := svc.BuildPathForDB(dbm, cl, hostIP, missDate.Format("2006-01-02"), model.EditionSameCity)
	gfs.Delete(drPath)

	log.Printf("演示种子就绪：1 集群 / 1 库(自定义路径模板) / 实例IP %s / 15 天双卷备份元数据，其中 2 天前异地卷缺失（可巡检→补发）。", hostIP)
}

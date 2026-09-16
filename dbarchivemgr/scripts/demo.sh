#!/usr/bin/env bash
# DBArchiveManager 全链路演示脚本（纯内存模拟桩，无真实环境依赖）。
# 架构要点：管控为调度枢纽，【不直连文件系统】——全部经 HTTP 与 GFS / 大数据组件交互；
# 大数据自治拉取 GFS→写 HDFS→回调管控；补发指令亦经 HTTP 下发。
# 前提：已在 dbarchivemgr/ 目录执行 go build -o bin/dbam ./cmd/server
set -euo pipefail

BASE="http://localhost:${PORT:-8090}"
API="$BASE/api/v1"
LOG=/tmp/dbam_demo.log

# 日期口径与种子对齐（种子按「今天」动态生成，脚本不能写死日期）：
#   MISSING=2 天前（种子故意删了当天异地卷）
#   TODAY=今天：滚动清单 cutoff 为参照日，服务内部再减 15 天窗口 → 恰纳入最老一份(now-15)
days_ago() { if date -v-1d >/dev/null 2>&1; then date -u -v-${1}d +%F; else date -u -d "${1} days ago" +%F; fi; }
MISSING=$(days_ago 2)
TODAY=$(days_ago 0)
OLDEST=$(days_ago 15)

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

say "启动 server（后台）；组件装配：GFS / 大数据 均为独立 HTTP server"
bin/dbam -addr ":8090" -dsn "file:dbam_demo.db?cache=shared" >$LOG 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true; rm -f dbam_demo.db' EXIT

for i in $(seq 1 30); do
  if curl -sf "$BASE/healthz" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SRV" 2>/dev/null; then
    echo "server 启动失败，日志如下：" >&2
    tail -20 "$LOG" >&2 || true
    exit 1
  fi
  sleep 1
done

echo "--- 装配日志（管控何时经 HTTP 访问组件）---"
grep -E "\[装配\]" "$LOG" || true

say "1) 健康检查"
curl -s "$BASE/healthz"; echo

say "2) 集群与实例 IP（备份按 IP 反查集群）"
curl -s "$API/cluster/1/instances"; echo

say "3) 归档状态对账（种子：15 份 NOT_ARCHIVED，其中 2 天前异地卷缺失）"
curl -s "$API/meta/reconciliation"; echo

say "4) 每日完整性巡检（应发现 1 条异地卷缺失）"
curl -s -X POST "$API/inspect/run" -d "{\"targetDate\":\"$MISSING\",\"windowDays\":15}"; echo

say "5) 下发补发指令（经 HTTP 推大数据 → 大数据重写缺失卷 → 回调 SUCCESS）"
curl -s -X POST "$API/replenish/task" \
  -d "{\"clusterId\":1,\"dbId\":1,\"dbName\":\"orders\",\"backupDate\":\"$MISSING\",\"backupEdition\":\"SAME_CITY\"}"; echo
sleep 1

say "6) 生成滚动归档清单（仅纳入已过窗口备份；经 HTTP 通知大数据自治拉取）"
out=$(curl -s -X POST "$API/archive/rolling-list/generate" -d "{\"cutoffDate\":\"$TODAY\"}")
printf '%s' "$out" | head -c 420; echo
sleep 1

say "7) 大数据已自治拉取 GFS→写 HDFS→回调管控 /archive/result；抽查一条已 ARCHIVED"
curl -s "$API/backup/archive-status?clusterCode=CL-MYSQL-01&dbName=orders&backupDate=$OLDEST&backupEdition=SAME_CITY"; echo

say "8) 每日 GFS 清理（管控经 HTTP 调 GFS 删除已归档且满留存的备份）"
curl -s -X POST "$API/cleanup"; echo

say "9) 清理后对账"
curl -s "$API/meta/reconciliation"; echo

say "10) 演示「管控 ↔ GFS / 大数据」HTTP 交互日志佐证"
echo "　(来自 server 日志，前缀标明调用方向)"
grep -E "\[HTTP 管控|\[bigdata\]" "$LOG" | tail -25 || true

echo
echo "演示完成 ✅"

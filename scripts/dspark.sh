#!/usr/bin/env bash
# dspark.sh — 双 DGX Spark 集群常用运维脚本（多模型一键切换）
# 在 head 节点上以普通用户运行，无需 sudo（IP/用户名等经环境变量或 defaults 配置）
#
# 用法: ./dspark.sh <命令> [参数]
#   models     列出已注册模型（标注当前运行的）
#   current    显示当前模型（短名/配方/模型ID）
#   use <名>   一键切换：停旧服务 → 等内存释放 → 起新模型 → 等健康检查（sparkrun ~6 分钟 / glm53 冷启动 7-10 分钟）
#   status     集群总览（容器 + API 健康 + 两机资源）
#   check      一键巡检（SSH/链路/巨帧/容器/API/磁盘/代理/自启单元+Dify shim）
#   start      启动当前模型的双机服务并等待健康检查
#   stop       停止服务
#   restart    重启服务
#   logs       跟随 sparkrun 编排日志
#   elog [N]   跟随 head 容器内引擎日志 /tmp/sparkrun_serve.log（默认最后 200 行）
#   monitor    单次采集两机 GPU/内存/负载（持续观察加 watch: watch -n5 ./dspark.sh monitor）
#   net        CX7 双链路连通性 + 巨帧(MTU9000)测试
#   proxy st   mihomo 代理: st=状态 / up=启动 / down=停止 / test=测速
#   ask "问题" 快速提问（默认关闭思考，流式输出）
#   bench      生成性能测试（TTFT / tok/s）
#   watchdog   看门狗: on启用 / off关闭 / status状态 / recover手动双机重建 / log判定日志 / pm崩溃现场（glm53）
#   switch <名> 一键形态切换: glm53(vLLM双机) ↔ comfy(H3视频)；自动停对端/等两机内存/管看门狗（幂等）
#   comfy      MiniMax H3视频: up(=switch comfy) / down停止 / st状态 / logs[1|2|d]日志（统一入口:8188）
#   web        运维Web面板（:8200，免密仅管理网）: up启动(装systemd单元+开机自启) / down停止 / st状态 / url地址；崩溃自动重启
#   tunnel|tu 公网暴露(glm53): st状态(隧道+HTTPS鉴权探测) / url地址与key / key只看key / rotate轮换key并重启 / log frpc日志
#   cache      双机HF权重缓存清单(JSON)：大小/双机一致性/已注册关联
#   recipes [kw] 搜索 sparkrun 配方市场；recipe-show <@reg/name> 看配方详情(模型ID/TP/默认参数)
#   download <org/name> [--host spark1|spark2|both] [--workers N] [--no-mirror] [--proxy] [--include GLOB ...]
#              下载HF权重（默认hf-mirror+禁XET；both=双机safetensors分片并行后自动内网合并）
#   model-sync <org/name> 200G fabric 双向rsync合并权重（下载后同步双机一致）
#   register <短名> <@reg/recipe> [关思考JSON] 注册配方到可切换列表；unregister <短名> 注销(不删权重)
#   models --json 结构化模型列表（Web面板用）

set -uo pipefail

# ---------- 集群常量 ----------
CLUSTER="${DSPARK_CLUSTER:-dualspark}"
API="http://${DSPARK_HEAD:-192.168.31.51}:8000"
SHIM="http://${DSPARK_HEAD:-192.168.31.51}:8001"   # 全模型必经：Dify legacy functions 协议转换 shim（上游即当前 active 引擎）
HEAD="127.0.0.1"
WORKER="${DSPARK_WORKER:-192.168.31.52}"
WORKER_FAB="${DSPARK_WORKER_FAB:-192.168.0.52}"   # CX7 fabric 直连（切换关键路径优先走它，管理网掉线不影响）
CX7_IPS=("${DSPARK_CX7_WORKER1:-192.168.0.52}" "${DSPARK_CX7_WORKER2:-192.168.1.52}")   # 两条 CX7 fabric 链路的 worker 侧 IP
HOSTS_LABEL=("$HEAD:spark1" "$WORKER:spark2")
HEAD_IP="${DSPARK_HEAD:-192.168.31.51}"   # 文案展示用（与 API/SHIM 同主机）
MIHOMO_DIR="$HOME/mihomo"

export PATH="$HOME/.local/bin:$PATH"

# ---------- 模型注册表 ----------
# 每行: 短名|配方/启动器|HF模型ID|关思考参数(JSON)|说明|镜像覆盖(空=默认)|编排类型(空=sparkrun, docker-direct=裸docker)
# 内置模型（勿手改）；用户经 Web/register 注册的模型在 $MODELS_LOCAL（同样的行格式，无外层引号）
MODELS=(
  "deepseek|@official/deepseek-v4-flash-0731-b12x-dspark-vllm|deepseek-ai/DeepSeek-V4-Flash-0731|{\"thinking\":false}|DeepSeek-V4-Flash 0731 · 156GB bf16 · 1M上下文 · 通用旗舰 ~33tok/s||"
  "qwen38|@eugr/qwen3.8-27b-nvfp4-dflash2|nvidia/Qwen3.8-27B-NVFP4|{\"enable_thinking\":false}|Qwen3.8-27B NVFP4+DFlash2投机8 · 262k · 低延迟高并发|dgx-vllm-qwen38-patched:local|"
  "glm53|docker-direct|Intel/GLM-5.3-Flash-W4A16-AutoRound|{\"reasoning_effort\":\"low\"}|GLM-5.3-Flash W4A16 MTP3+PMU128 · 1M上下文 · Agent工具调用/多会话并发(91/100) · 强制思考仅可low/high/max|spark-recipes/glm53-autoround-mtp3-pmu128:20260903-difyrc|docker-direct"
)
BUILTIN_MODELS="deepseek qwen38 glm53"
MODELS_LOCAL="$HOME/.config/dspark/models.local.conf"
if [[ -f "$MODELS_LOCAL" ]]; then
  while IFS= read -r __line || [[ -n "$__line" ]]; do
    [[ -z "$__line" || "$__line" == \#* ]] && continue
    MODELS+=("$__line")
  done < "$MODELS_LOCAL"
  unset __line
fi
ACTIVE_FILE="$HOME/.config/dspark/active"

# docker-direct 模型专用：启动脚本与容器名（两机同名，按主机区分）
GLM_DIR="$HOME/spark-recipes/tp2_glm53flash_autoround_mtp3_pmu128"
GLM_LAUNCH="$GLM_DIR/launch-dspark.sh"
GLM_NAME="spark_glm53_autoround_mtp3_pmu128"
GLM_WATCHDOG="$GLM_DIR/watchdog.sh"

# ComfyUI + MiniMax H3（keys-heretic，两机双实例 + 队列分发；脚本经 NFS 共享）
COMFY_OPS="$HOME/comfydata/ops"
COMFY_CTL="$COMFY_OPS/comfyctl.sh"
COMFY_DISPATCHER="$COMFY_OPS/dispatcher.py"
COMFY_VENV_PY="/opt/minnimax-h3-venv/bin/python"
COMFY_URL="http://${DSPARK_HEAD:-192.168.31.51}:8188"

# 运维 Web 面板（dspark.sh 的 HTTP 薄外壳，系统 python3 标准库，零三方依赖）
# 免密 + 仅管理网来源；systemd 托管（dspark-opsweb.service，开机自启/崩溃自动重启）
OPS_WEB_DIR="${DSPARK_OPSWEB_DIR:-$HOME/ops-web}"
OPS_WEB_PY="$OPS_WEB_DIR/ops_web.py"
OPS_WEB_PORT=8200
OPS_WEB_UNIT="dspark-opsweb"
OPS_WEB_UNIT_FILE="/etc/systemd/system/$OPS_WEB_UNIT.service"
OPS_WEB_LOG="$HOME/.local/state/dspark-opsweb/opsweb.log"

# 公网暴露（frp 内网穿透 → 公网 Caddy HTTPS → 网关:8002；vLLM 运行时常开）
PUB_DOMAIN="${DSPARK_PUB_DOMAIN:-}"  # optional, empty = public access disabled
PUB_BASE="https://$PUB_DOMAIN"
FRPC_UNIT="frpc-glm"

m_row() {  # $1=短名，输出整行
    local r
    for r in "${MODELS[@]}"; do [[ "${r%%|*}" == "$1" ]] && { echo "$r"; return 0; }; done
    return 1
}
m_field() {  # $1=短名 $2=字段序号(1短名 2配方 3模型ID 4关思考JSON 5说明 6镜像覆盖 7编排类型 8额外启动参数)
    local r name recipe model nothink desc image kind extra
    r=$(m_row "$1") || return 1
    IFS='|' read -r name recipe model nothink desc image kind extra <<< "$r"
    case "$2" in
      1) echo "$name";; 2) echo "$recipe";; 3) echo "$model";;
      4) echo "$nothink";; 5) echo "$desc";; 6) echo "$image";; 7) echo "$kind";;
      8) echo "$extra";;
    esac
}
active_name() {  # 当前选中的模型短名（默认 deepseek）
    local n; n=$(cat "$ACTIVE_FILE" 2>/dev/null || echo deepseek)
    m_row "$n" >/dev/null || n=deepseek
    echo "$n"
}

# ---------- 输出工具 ----------
if [[ -t 1 ]]; then
    C_R=$'\033[31m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_B=$'\033[36m'; C_0=$'\033[0m'
else
    C_R=""; C_G=""; C_Y=""; C_B=""; C_0=""
fi
ok()   { echo "${C_G}[✓]${C_0} $*"; }
warn() { echo "${C_Y}[!]${C_0} $*"; }
err()  { echo "${C_R}[✗]${C_0} $*"; }
info() { echo "${C_B}:: $*${C_0}"; }
hr()   { echo "------------------------------------------------------------"; }

# 兼容旧终端无 docker 组身份：自动用 sg docker 重入（用户已是 docker 成员，不弹密码）
need_docker=0
case "${1:-}" in
  status|check|start|stop|restart|logs|elog|monitor|use|comfy|switch|to|web) need_docker=1 ;;
esac
if [[ $need_docker -eq 1 ]] && ! docker info >/dev/null 2>&1; then
    warn "当前会话无 docker 组身份，自动切换（如频繁出现请注销重登/Reload Window）"
    exec sg docker -c "$(printf '%q ' "$0" "$@")"
fi

# 与 glm53 看门狗（glm53-watchdog.timer）互斥：人工 use/start/stop/restart 期间，
# 看门狗 tick 拿不到锁会跳过；反过来人工命令等看门狗恢复动作完成（最多 5 分钟）。
WD_LOCK="$HOME/.local/state/glm53-watchdog/lock"
case "${1:-}" in
  use|start|restart|stop|switch|to)
    mkdir -p "$(dirname "$WD_LOCK")"
    exec 8>"$WD_LOCK"
    flock -w 300 8 || { err "等待看门狗锁（可能正在自动恢复），请稍后重试"; exit 1; }
    ;;
esac

# ---------- 动态发现 ----------
# 统一作业检测：返回 sparkrun:<id> 或 docker-direct；空=无服务
job_id() {
    local j
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${GLM_NAME}\$"; then
        echo "docker-direct"; return 0
    fi
    j=$(sparkrun status --cluster "$CLUSTER" 2>/dev/null \
        | grep -oE '\[[0-9a-f]{16}_[0-9a-f]{12}\]' | head -1 | tr -d '[]')
    [[ -n "$j" ]] && echo "sparkrun:$j"
}
# 停掉任意编排类型的在跑服务（两机）
svc_stop() {  # $1=job_id 输出值
    local j="$1"
    case "$j" in
      docker-direct)
        info "停止 docker-direct 容器（两机）"
        "$GLM_LAUNCH" stop
        ;;
      sparkrun:*)
        info "停止当前 job ${j#sparkrun:}"
        sparkrun stop "${j#sparkrun:}" >/dev/null
        ;;
    esac
}
node_container() {  # $1=0|1 （node_0 在本机 head；node_1 在 spark2）
    if [[ "$1" == 0 ]]; then
        docker ps --format '{{.Names}}' 2>/dev/null | grep -E "sparkrun_.*_node_0\$|^${GLM_NAME}\$" | head -1
    else
        wssh "docker ps --format '{{.Names}}' 2>/dev/null | grep -E 'sparkrun_.*_node_1\$|^${GLM_NAME}\$' | head -1" 2>/dev/null
    fi
}
api_code() { curl -s -o /dev/null -w '%{http_code}' -m 5 "$API/health" 2>/dev/null || echo 000; }
# vLLM --api-key 凭据（/health 与 /metrics 不校验，仅 /v1/* 需要）
VLLM_KEY_FILE="$HOME/.config/dspark/vllm_api_key"
vllm_auth=()
[[ -f "$VLLM_KEY_FILE" ]] && vllm_auth=(-H "Authorization: Bearer $(cat "$VLLM_KEY_FILE")")
vllm_key() { [[ -f "$VLLM_KEY_FILE" ]] && cat "$VLLM_KEY_FILE"; }

# 在 worker 上执行命令：fabric 直连优先，管理网兜底（r8127 watchdog 曾致管理网掉线）
wssh() {
    ssh -o BatchMode=yes -o ConnectTimeout=6 "$WORKER_FAB" "$@" 2>/dev/null \
        || ssh -o BatchMode=yes -o ConnectTimeout=6 "$WORKER" "$@"
}
# 按 IP 执行远端命令串：head 本机直接 bash -c，worker 走 wssh（fabric 优先）
rssh() {
    local ip="$1"; shift
    if [[ "$ip" == "$HEAD" ]]; then bash -c "$1"; else wssh "$1"; fi
}
mem_avail_gb() { free -m | awk 'NR==2{print int($7/1024)}'; }  # available（GiB），含可回收页缓存
wait_mem_release() {  # 等两机 available 回到 25GiB 以上（大模型容器退出后释放可能远超 30s）
    local a1 a2 i
    for i in $(seq 1 18); do
        a1=$(mem_avail_gb)
        a2=$(wssh 'free -m | awk "NR==2{print int(\$7/1024)}"' 2>/dev/null || echo 0)
        if [[ "$a1" -ge 25 && "$a2" -ge 25 ]]; then
            ok "两机可用内存已恢复 (spark1 ${a1}GiB / spark2 ${a2}GiB available)"
            return 0
        fi
        info "等待内存释放: available spark1 ${a1}GiB / spark2 ${a2}GiB ..."
        sleep 10
    done
    warn "内存未完全释放即继续（若启动报 ibv_reg_mr ENOMEM，确认空闲后重试一次 $0 use）"
}

# glm53 冷盘加载时 GPTQ→Marlin 重打包会在内存紧张下硬挂（见 models README 踩坑#4）。
# 启动前 available 不足 40GiB 时主动 drop_caches（仅释放干净页缓存/dentries，无损），两机分别尝试。
ensure_mem_glm() {
    local a1 a2
    # 权重下载抢占 IO/页缓存时，冷盘 GPTQ→Marlin repack 实测会异常耗时（25 分钟无产出）；
    # 下载脚本支持断点续传，强烈建议先停掉再切 glm53
    if comfy_download_busy; then
        warn "检测到 H3 权重下载正在运行：它与 glm53 冷盘 Marlin repack 抢 IO/页缓存，"
        warn "可能导致启动长时间卡在 'Using MarlinExperts'。建议先终止下载（可断点续传）再切换"
    fi
    a1=$(mem_avail_gb); a2=$(wssh 'free -m | awk "NR==2{print int(\$7/1024)}"' 2>/dev/null || echo 0)
    info "启动前可用内存: spark1 ${a1}GiB / spark2 ${a2}GiB available"
    if (( a1 >= 40 && a2 >= 40 )); then return 0; fi
    warn "可用内存低于 40GiB，glm53 冷盘 Marlin 重打包有硬挂风险，尝试释放两机页缓存 ..."
    if sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then ok "spark1 页缓存已释放"
    else warn "spark1 自动 drop_caches 失败（sudo 不可用），继续启动但有风险"; fi
    if wssh "sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'" 2>/dev/null; then ok "spark2 页缓存已释放"
    else warn "spark2 自动 drop_caches 失败（sudo 不可用），继续启动但有风险"; fi
    sleep 3
    a1=$(mem_avail_gb); a2=$(wssh 'free -m | awk "NR==2{print int(\$7/1024)}"' 2>/dev/null || echo 0)
    info "释放后可用内存: spark1 ${a1}GiB / spark2 ${a2}GiB available"
    if (( a1 < 40 || a2 < 40 )); then
        warn "可用内存仍偏低；若启动卡在 Using MarlinExperts 不动，按 models README 踩坑#4 双机重启"
    fi
}

wait_health() {  # $1=最长等待分钟（默认12；docker-direct 大权重加载给 25）  $2=编排类型（docker-direct 时输出引擎进度）
    local mins="${1:-12}" kind="${2:-}" i code line last_line="" stall=0
    info "等待 API 健康检查（glm53 冷启动正常需 7-10 分钟；下面的引擎日志可区分“慢”与“挂死”）..."
    for i in $(seq 1 $((mins*12))); do
        code=$(api_code)
        if [[ "$code" == "200" ]]; then echo; ok "API 已就绪 $API"; return 0; fi
        printf '.'
        if (( i % 12 == 0 )); then
            echo "  $((i*5))s (HTTP $code)"
            if [[ "$kind" == "docker-direct" ]]; then
                line=$(docker logs --tail 1 "$GLM_NAME" 2>&1 | tail -1 | cut -c1-110)
                [[ -n "$line" ]] && echo "      └ $line"
                # 冷盘 repack 硬挂判据：Marlin 阶段最后一行日志连续 5 分钟不变（正常热缓存 <2 分钟通过）
                if [[ "$line" == *Marlin* ]]; then
                    if [[ "$line" == "$last_line" ]]; then stall=$((stall+1)); else stall=0; fi
                    if (( stall >= 5 )); then
                        err "引擎连续 5 分钟停在 Marlin 阶段，疑似冷盘 repack 硬挂（不是慢！）"
                        warn "实锤判据: sudo /tmp/pyspy-venv/bin/py-spy dump --pid \$(pgrep -f VLLM::Worker_TP0 | head -1)"
                        warn "处理: $0 stop（确认两机内存充足）后重新 $0 use（详见 models README 踩坑#4）"
                        stall=0
                    fi
                fi
                last_line="$line"
            fi
        fi
        sleep 5
    done
    echo; err "${mins} 分钟内 API 未就绪：$0 elog 看引擎进度；确认挂死可 $0 stop 后重新 $0 use"
    return 1
}

# ---------- 命令实现 ----------
run_recipe() {  # $1=短名；按注册表第7字段选择编排（sparkrun / docker-direct）
    local n="$1" kind img args=()
    kind=$(m_field "$n" 7)
    if [[ "$kind" == "docker-direct" ]]; then
        [[ -x "$GLM_LAUNCH" ]] || { err "缺少启动脚本 $GLM_LAUNCH"; return 1; }
        "$GLM_LAUNCH" both
        return $?
    fi
    img=$(m_field "$n" 6)
    [[ -n "$img" ]] && args+=(--image "$img")
    # 第 8 字段：额外 sparkrun 参数（空格分隔，仅本地管理员配置）。
    # 用途：规避配方默认参数在双机上的兼容问题，如 deepseek-v4 sparse MLA
    # 在 KV 不足触发 auto-fit max_model_len 时与 CUDA graph 捕获冲突。
    local extra; extra=$(m_field "$n" 8)
    if [[ -n "$extra" ]]; then
        local ex; read -r -a ex <<< "$extra"
        args+=("${ex[@]}")
    fi
    sparkrun run "$(m_field "$n" 2)" --cluster "$CLUSTER" --tp 2 --no-follow "${args[@]}"
}

cmd_status() {
    local _n; _n=$(active_name)
    info "当前模型: ${C_G}$_n${C_0} ($(m_field "$_n" 3))"
    echo
    info "容器状态"
    if [[ "$(m_field "$_n" 7)" == "docker-direct" ]]; then
        echo "  [spark1/head]"; docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null | grep -E "NAMES|${GLM_NAME}"
        echo "  [spark2/worker]"; wssh "docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null | grep -E 'NAMES|${GLM_NAME}'"
    else
        sparkrun status --cluster "$CLUSTER" 2>&1 || true
    fi
    echo
    info "API ($API)"
    local code; code=$(api_code)
    if [[ "$code" == "200" ]]; then
        ok "health 200"
        local live; live=$(curl -s -m 5 "${vllm_auth[@]}" "$API/v1/models" 2>/dev/null)
        echo "$live" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"][0]
print("    模型: %s  上下文: %s" % (d["id"], format(d.get("max_model_len", 0), ",")))' 2>/dev/null
        local live_id expect_id
        live_id=$(echo "$live" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"][0]["id"])' 2>/dev/null)
        expect_id=$(m_field "$_n" 3)
        [[ -n "$live_id" && "$live_id" != "$expect_id" ]] && \
            warn "实际服务为 $live_id，与 active 记录 ($_n → $expect_id) 不一致（手动 launch/开机自启所致）；echo -n <短名> > $ACTIVE_FILE 可修正"
    else
        err "health = HTTP $code"
    fi
    echo
    info "Dify 协议 shim（全模型必经 :8001 → :8000）"
    if [[ "$code" == "200" ]]; then
        local sc; sc=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$SHIM/health" 2>/dev/null || echo 000)
        [[ "$sc" == "200" ]] && ok "shim :8001 health 200（模型名 dspark 与真实名均接受；Dify 走网关 :8002 即自动经此，直连用 $SHIM/v1）" \
            || err "shim :8001 不可达 (HTTP $sc)：systemctl status dify-functions-shim"
    else
        warn "引擎未健康，跳过 shim 透传探活（shim 服务状态以 check 第 8 项为准）"
    fi
    if [[ "$(m_field "$_n" 7)" == "docker-direct" ]]; then
        echo
        info "glm53 开机自启/看门狗（专属组件）"
        local ua ue
        ua=$(systemctl is-active glm53-head); ue=$(systemctl is-enabled glm53-head 2>/dev/null)
        [[ "$ua" == "active" && "$ue" == "enabled" ]] \
            && ok "glm53-head 单元 $ua/$ue" || warn "glm53-head 单元 $ua/$ue"
        local wa we
        wa=$(wssh 'systemctl is-active glm53-worker' 2>/dev/null)
        we=$(wssh 'systemctl is-enabled glm53-worker' 2>/dev/null)
        [[ "$wa" == "active" && "$we" == "enabled" ]] \
            && ok "glm53-worker 单元 $wa/$we（spark2）" || warn "glm53-worker 单元 ${wa:-?}/${we:-?}（spark2）"
        local wt wte
        wt=$(systemctl is-active glm53-watchdog.timer 2>/dev/null)
        wte=$(systemctl is-enabled glm53-watchdog.timer 2>/dev/null)
        if [[ "$wt" == "active" && "$wte" == "enabled" ]]; then
            local fails; fails=$(cat "$HOME/.local/state/glm53-watchdog/fails" 2>/dev/null || echo 0)
            ok "看门狗 timer $wt/$wte（health 异常连续计数 $fails；详情 journalctl -u glm53-watchdog）"
        else
            err "看门狗 timer $wt/$wte（应为 active/enabled）：sudo systemctl enable --now glm53-watchdog.timer"
        fi
    fi
    return 0
}

cmd_check() {
    local fails=0

    info "1/10 两机连通（spark2 走 fabric 192.168.0.52 优先，管理网兜底）"
    for hl in "${HOSTS_LABEL[@]}"; do
        local ip=${hl%%:*}; name=${hl##*:}
        if [[ "$ip" == "$HEAD" ]]; then ok "$name 本机";
        elif rssh "$ip" true 2>/dev/null; then
            ok "$name 可达（fabric $WORKER_FAB，管理网 $WORKER 兜底）"
        else err "$name 两网均不可达"; ((fails++)); fi
    done

    info "2/10 CX7 双链路 + 巨帧 (8972 字节载荷)"
    for ip in "${CX7_IPS[@]}"; do
        if out=$(ping -c2 -W2 -M do -s 8972 "$ip" 2>&1); then
            ok "$ip 巨帧通 ($(echo "$out"|grep -oE '[0-9.]+/[0-9.]+/[0-9.]+'|tail -1) ms)"
        else err "$ip 巨帧不通（检查 netplan 40-cx7.yaml / MTU9000）"; ((fails++)); fi
    done

    info "3/10 推理容器"
    local n0 n1
    n0=$(node_container 0); n1=$(node_container 1)
    [[ -n "$n0" ]] && ok "head 容器: $n0" || { err "head 容器未运行"; ((fails++)); }
    if [[ -n "$n1" ]]; then ok "worker 容器运行中（spark2）: $n1"
    else err "worker 容器未运行（spark2）"; ((fails++)); fi

    info "4/10 API"
    [[ "$(api_code)" == "200" ]] && ok "$API/health 200" || { err "API 不健康"; ((fails++)); }

    info "5/10 两机磁盘（模型 156GB/机 + 镜像 36GB，注意余量）"
    for hl in "${HOSTS_LABEL[@]}"; do
        local ip=${hl%%:*}; name=${hl##*:}
        local line; line=$(rssh "$ip" 'df -h / | awk "NR==2{print \$2,\$3,\$4,\$5}"' 2>/dev/null)
        set -- $line
        echo "    $name 总量 $1 已用 $2 可用 $3 ($4)"
        [[ "${4%\%}" -ge 90 ]] && warn "$name 根分区使用率 >=90%"
    done

    info "6/10 统一内存/GPU（两机；GB10 显存与内存统一，nvidia-smi 显存列显示 N/A 属正常）"
    for hl in "${HOSTS_LABEL[@]}"; do
        local ip=${hl%%:*}; name=${hl##*:}
        local gpu ram
        gpu=$(rssh "$ip" "nvidia-smi --query-gpu=utilization.gpu,temperature.gpu,power.draw --format=csv,noheader" 2>/dev/null)
        ram=$(rssh "$ip" "free -m | awk 'NR==2{printf \"%.0f / %.0f GiB\", \$3/1024, \$2/1024}'" 2>/dev/null)
        echo "    $name 统一内存(已用/总量) $ram | GPU 利用率/温度/功耗 $gpu"
    done

    info "7/10 mihomo 代理（仅下载需要）"
    if pgrep -x mihomo >/dev/null; then ok "mihomo 运行中 (127.0.0.1:7890)"; else warn "mihomo 未运行（$0 proxy up 启动；推理不依赖它）"; fi

    info "8/10 Dify 协议 shim（所有模型必经）+ glm53 专属自启/看门狗（仅 glm53 时检）"
    local sst sen
    sst=$(systemctl is-active dify-functions-shim); sen=$(systemctl is-enabled dify-functions-shim 2>/dev/null)
    if [[ "$sst" == "active" && "$sen" == "enabled" ]]; then ok "spark1 dify-functions-shim: $sst/$sen（全模型网关→:8001→:8000）"
    else err "spark1 dify-functions-shim: $sst/$sen（应为 active/enabled；sudo systemctl enable --now dify-functions-shim）"; ((fails++)); fi
    if [[ "$(api_code)" == "200" ]]; then
        local sc; sc=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$SHIM/health" 2>/dev/null || echo 000)
        [[ "$sc" == "200" ]] && ok "shim $SHIM/health 200（透传当前 active 引擎；:8001 接受固定模型名 dspark）" \
            || { err "shim $SHIM 不可达 (HTTP $sc)：sudo systemctl restart dify-functions-shim"; ((fails++)); }
    fi
    if [[ "$(active_name)" != "glm53" ]]; then
        echo "    非 glm53 模型：glm53-head/worker/看门狗跳过（专属组件）"
    else
        local ua ue
        for ua in glm53-head glm53-watchdog.timer; do
            local st en
            st=$(systemctl is-active "$ua"); en=$(systemctl is-enabled "$ua" 2>/dev/null)
            if [[ "$st" == "active" && "$en" == "enabled" ]]; then ok "spark1 $ua: $st/$en"
            else err "spark1 $ua: $st/$en（应为 active/enabled）"; ((fails++)); fi
        done
        local st en
        st=$(wssh 'systemctl is-active glm53-worker' 2>/dev/null)
        en=$(wssh 'systemctl is-enabled glm53-worker' 2>/dev/null)
        if [[ "$st" == "active" && "$en" == "enabled" ]]; then ok "spark2 glm53-worker: $st/$en"
        else err "spark2 glm53-worker: ${st:-未知}/${en:-未知}"; ((fails++)); fi
    fi

    info "9/10 统一模型网关（:8002 固定模型名 dspark + Bearer key 鉴权；面板切换模型无需改客户端）"
    local gst gen
    gst=$(systemctl is-active dspark-model-gateway 2>/dev/null)
    gen=$(systemctl is-enabled dspark-model-gateway 2>/dev/null)
    if [[ "$gst" == "active" && "$gen" == "enabled" ]]; then ok "dspark-model-gateway: $gst/$gen"
    else err "dspark-model-gateway: ${gst:-未安装}/${gen:-?}（sudo systemctl enable --now dspark-model-gateway）"; ((fails++)); fi
    local gsc gk
    gsc=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:8002/v1/models 2>/dev/null || echo 000)
    [[ "$gsc" == "401" ]] && ok "网关 :8002 无 key 拒绝 401（鉴权生效）" \
      || { err "网关无 key 返回 $gsc（预期 401；200=鉴权失效，000=服务挂）"; ((fails++)); }
    if [[ -f "$VLLM_KEY_FILE" ]]; then
      gk=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -H "Authorization: Bearer $(cat "$VLLM_KEY_FILE")" http://127.0.0.1:8002/v1/models 2>/dev/null || echo 000)
      [[ "$gk" == "200" ]] && ok "网关带 key 200（内网入口 http://$HEAD_IP:8002/v1，模型名 dspark）" \
        || { err "网关带 key 返回 $gk（预期 200；查 key 文件/网关日志）"; ((fails++)); }
    else err "key 文件缺失: $VLLM_KEY_FILE（网关会 fail-closed 503）"; ((fails++)); fi

    info "10/10 公网暴露（frpc → 网关:8002 + shim:8001；$PUB_DOMAIN HTTPS；所有 vLLM 形态常开）"
    local tu; tu=$(systemctl is-enabled "$FRPC_UNIT" 2>/dev/null)
    if [[ "$(api_code)" != "200" ]]; then
        if [[ "$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)" == "active" ]]; then
            err "$FRPC_UNIT 仍在运行：vLLM 未健康时隧道应关闭（sudo systemctl stop $FRPC_UNIT）"
            ((fails++))
        else echo "    vLLM 未运行，隧道已停（正确）"; fi
    elif [[ -z "$tu" ]]; then
        warn "$FRPC_UNIT 未安装（公网未暴露；如需公网见 README 第 4 节部署 frpc）"
    else
        local ta; ta=$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)
        if [[ "$ta" == "active" && "$tu" == "enabled" ]]; then ok "$FRPC_UNIT: $ta/$tu"
        else err "$FRPC_UNIT: ${ta:-未知}/$tu（sudo systemctl start $FRPC_UNIT）"; ((fails++)); fi
        [[ -f "$VLLM_KEY_FILE" ]] && ok "key 文件存在 ($(cut -c1-14 "$VLLM_KEY_FILE")…)" \
            || { err "key 文件缺失: $VLLM_KEY_FILE"; ((fails++)); }
        local pc pp
        pc=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "$PUB_BASE/v1/models" 2>/dev/null || echo 000)
        if [[ "$pc" == "401" ]]; then ok "$PUB_DOMAIN/v1 无 key 401（网关全链路通 + 鉴权）"
        else err "$PUB_DOMAIN/v1 探测 $pc（401=正常；000 查外网/DNS/frpc；502 查 Caddy→8090）"; ((fails++)); fi
        pp=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "$PUB_BASE/shim/v1/models" 2>/dev/null || echo 000)
        if [[ "$pp" == "401" ]]; then ok "$PUB_DOMAIN/shim/v1 无 key 401（shim 全链路通 + 鉴权）"
        else err "$PUB_DOMAIN/shim/v1 探测 $pp（401=正常；502 查 frpc 是否注册 8091/Caddy 路由）"; ((fails++)); fi
    fi

    echo; hr
    if (( fails == 0 )); then ok "巡检全部通过"; else err "$fails 项异常"; fi
    return $fails
}

cmd_models() {
    local cur; cur=$(active_name)
    echo "已注册模型（当前: ${C_G}$cur${C_0}）："
    local r name desc
    for r in "${MODELS[@]}"; do
        IFS='|' read -r name _ _ _ desc _ _ <<< "$r"
        if [[ "$name" == "$cur" ]]; then echo "  ${C_G}●${C_0} $name  $desc"
        else echo "  ○ $name  $desc"; fi
    done
    echo; echo "切换: $0 use <短名>   （例: $0 use qwen38）"
}

cmd_current() {
    local n; n=$(active_name)
    echo "当前模型短名 : $n"
    echo "sparkrun配方 : $(m_field "$n" 2)"
    echo "API模型ID    : $(m_field "$n" 3)"
    echo "说明         : $(m_field "$n" 5)"
    local _img; _img=$(m_field "$n" 6)
    echo "镜像         : ${_img:-（配方默认）}"
    if [[ "$(api_code)" == "200" ]]; then ok "服务运行中 $API"; else warn "服务未运行（$0 start）"; fi
}

# ---------- 模型生命周期：注册/缓存/配方/下载/同步 ----------
# spark1 的 hf CLI 在 sparkrun venv 内；spark2 是独立 ~/.venv/hf——按机解析
_hf_bin_local() {
    local p
    for p in "$HOME/.venv/sparkrun/bin/hf" "$HOME/.venv/hf/bin/hf"; do
        [[ -x "$p" ]] && { printf '%s\n' "$p"; return 0; }
    done
    return 1
}
# 远端（$1=ip）：探测首个存在的 hf 路径并回显；$2=py 时回显同 venv 的 python
_hf_bin_remote() {
    local ip="$1" want="${2:-hf}"
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$ip" '
        for p in "$HOME/.venv/sparkrun/bin" "$HOME/.venv/hf/bin"; do
            if [ -x "$p/hf" ]; then
                if [ "$1" = py ]; then [ -x "$p/python" ] && echo "$p/python"; else echo "$p/hf"; fi
                exit 0
            fi
        done; exit 1' _ "$want"
}
HF_BIN="$(_hf_bin_local || echo "$HOME/.venv/sparkrun/bin/hf")"
HF_PY="${HF_BIN%/bin/hf}/bin/python"

cmd_models_json() {
    local cur; cur=$(active_name)
    # 双机权重就位探测：本机循环 + spark2 一次 SSH 批量（snapshots 文件数，0=未下载）
    local ip="$WORKER_FAB"; rssh "$ip" "true" 2>/dev/null || ip="$WORKER"
    local r name model rem='H=$HOME/.cache/huggingface/hub'
    local rows=()
    for r in "${MODELS[@]}"; do
        IFS='|' read -r name _ model _ _ _ _ _ <<< "$r"
        rem+="; find \$H/models--${model/\//--}/snapshots \( -type f -o -type l \) 2>/dev/null | wc -l"
        rows+=("$r")
    done
    local n2all; n2all=$(rssh "$ip" "$rem" 2>/dev/null)
    local i=0 n2
    local -a n2arr; mapfile -t n2arr <<< "$n2all"
    for r in "${rows[@]}"; do
        IFS='|' read -r name _ model _ _ _ _ _ <<< "$r"
        local n1; n1=$(find "$(hf_cache_dir "$model")/snapshots" \( -type f -o -type l \) 2>/dev/null | wc -l)
        n2=$(tr -d '[:space:]' <<< "${n2arr[$i]:-0}"); [[ "$n2" =~ ^[0-9]+$ ]] || n2=0
        printf '%s|%s|%s\n' "$r" "${n1:-0}" "$n2"
        ((i++))
    done | "$HF_PY" -c '
import json, sys
cur = sys.argv[1]
out = []
for line in sys.stdin:
    p = line.rstrip("\n").split("|")
    # 末两段固定是 head/worker 文件数；前面是注册行（可能含第8字段 extra）
    c1s, c2s = p[-2], p[-1]
    base = p[:-2]
    base += [""] * (8 - len(base))
    name, recipe, model, nothink, desc, image, kind = base[0], base[1], base[2], base[3], base[4], base[5], base[6]
    try:
        c1, c2 = int(c1s or 0), int(c2s or 0)
    except ValueError:
        c1 = c2 = 0
    cached = "both" if kind == "docker-direct" or (c1 > 0 and c2 > 0) else ("spark1" if c1 > 0 else "no")
    out.append({"name": name, "recipe": recipe, "model": model, "nothink": nothink,
                "desc": desc, "image": image, "kind": kind or "sparkrun",
                "current": name == cur,
                "builtin": name in ("deepseek", "qwen38", "glm53"),
                "cached": cached})
print(json.dumps(out, ensure_ascii=False))' "$cur"
}

# repo_id -> HF 缓存目录名（org/name → models--org--name）
hf_cache_dir() { echo "$HOME/.cache/huggingface/hub/models--${1/\//--}"; }

# 某 repo 权重是否在双机都就位（snapshots 下至少 1 个文件/链接；TP=2 要求双机一致）
weights_on_both() {
    local repo="$1" ip d n2
    [[ $(find "$(hf_cache_dir "$repo")/snapshots" \( -type f -o -type l \) 2>/dev/null | wc -l) -gt 0 ]] || return 1
    ip="$WORKER_FAB"; rssh "$ip" "true" 2>/dev/null || ip="$WORKER"
    d="\$HOME/.cache/huggingface/hub/models--${repo/\//--}"
    n2=$(rssh "$ip" "find $d/snapshots \\( -type f -o -type l \\) 2>/dev/null | wc -l" 2>/dev/null)
    [[ "${n2:-0}" -gt 0 ]] || return 1
}
# 切换前的权重就位闸：docker-direct（glm53）权重在 ~/models 由启动器自管，不查 HF 缓存
weights_ready() {
    local kind="$1" repo="$2"
    [[ "$kind" == "docker-direct" ]] && return 0
    weights_on_both "$repo"
}

# 万兆 NAS：下载/双机同步完成后增量备份 HF 缓存仓库（best-effort，失败不影响主流程）
NAS_URL="//192.168.31.215/personal_folder"
NAS_MOUNT=/mnt/nas
nas_backup_repo() {
    local repo="$1"
    local src; src=$(hf_cache_dir "$repo")
    [[ -d "$src" ]] || return 0
    local base; base=$(basename "$src")
    local dst="$NAS_MOUNT/models-hf/hub/$base"
    local logdir="$HOME/.local/state/dspark-backup"; mkdir -p "$logdir"
    local ts; ts=$(date +%Y%m%d-%H%M%S)
    local log="$logdir/nas-${repo//\//__}.$ts.log" rc="$logdir/.rc.$$"
    # automount 按需触发；NAS 不可达则跳过（挂载单元 TimeoutSec=20）
    if ! timeout 25 bash -c "mkdir -p '$dst'" 2>/dev/null; then
        warn "NAS 备份跳过：$NAS_MOUNT 不可达（不影响下载/同步结果；下次完成时自动续传）"
        return 0
    fi
    local total; total=$(du -sm "$src" 2>/dev/null | cut -f1); total=${total:-?}
    info "增量备份 → NAS $dst（本机 ${total}MiB；日志 $log）"
    ( timeout 14400 rsync -a --partial-dir=.rsync-tmp --exclude='*.incomplete' \
        "$src/" "$dst/" >"$log" 2>&1; echo $? > "$rc" ) &
    local p=$!
    while kill -0 "$p" 2>/dev/null; do
        local n; n=$(du -sm "$dst" 2>/dev/null | cut -f1); n=${n:-0}
        echo "[$(date +%T)] NAS 备份中 ${n}MiB / ${total}MiB"
        sleep 20
    done
    wait "$p"; local r; r=$(cat "$rc" 2>/dev/null || echo 1); rm -f "$rc"
    if [[ "$r" == 0 ]]; then
        ok "NAS 备份完成：$dst（恢复示例：rsync -a '$dst/' '$src/'）"
    else
        warn "NAS 备份未成功（rc=$r，详见 $log；下次自动续传，不影响本次结果）"
    fi
    return 0
}

cmd_register() {
    local name="$1" recipe="${2:-}" nothink="${3:-{\}}"
    [[ "$name" =~ ^[a-z0-9][a-z0-9-]{0,20}$ ]] || { err "短名非法：^[a-z0-9][a-z0-9-]{0,20}$"; return 1; }
    [[ "$recipe" =~ ^@(official|eugr|local)/[A-Za-z0-9_.-]+$ ]] \
      || { err "配方非法，形如 @official/xxx 或 @eugr/xxx"; return 1; }
    if m_row "$name" >/dev/null; then err "短名 $name 已存在（unregister 后再注册）"; return 1; fi
    "$HF_PY" -c 'import json,sys;json.loads(sys.argv[1])' "$nothink" 2>/dev/null \
      || { err "关思考参数不是合法 JSON（默认填 {} 即可）"; return 1; }
    info "查询配方 $recipe …"
    local show; show=$(sparkrun show "$recipe" 2>&1) || { err "sparkrun 找不到配方：$recipe"; echo "$show" | tail -5; return 1; }
    local model desc
    model=$(echo "$show" | awk -F': *' '/^Model:/{print $2; exit}')
    desc=$(echo "$show" | awk -F': *' '/^Description:/{print $2; exit}')
    [[ "$model" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9_.-]+$ ]] \
      || { err "配方中的 Model 字段异常: '$model'"; return 1; }
    desc=$(echo "$desc" | tr '|' '/' | tr '\n' ' ' | cut -c1-80)
    [[ -z "$desc" ]] && desc="$recipe"
    mkdir -p "$(dirname "$MODELS_LOCAL")"
    ( flock 9
      grep -q "^$name|" "$MODELS_LOCAL" 2>/dev/null && { echo "DUP"; exit 0; }
      printf '%s|%s|%s|%s|%s||\n' "$name" "$recipe" "$model" "$nothink" "$desc" >> "$MODELS_LOCAL"
      echo "OK"
    ) 9>"$MODELS_LOCAL.lock" | { read -r r; [[ "$r" == "OK" ]] || { err "短名冲突（并发注册？）"; return 1; }; }
    ok "已注册: $name → $model（$recipe）"
    local d; d=$(hf_cache_dir "$model")
    if [[ -d "$d" ]]; then ok "权重已在本机缓存: $d"
    else warn "权重未下载：$0 download $model （或 Web 模型管理页一键下载）"; fi
    echo "  切换: $0 use $name    注销: $0 unregister $name"
}

cmd_unregister() {
    local name="$1"
    [[ -n "$name" ]] || { err "用法: $0 unregister <短名>"; return 1; }
    for b in $BUILTIN_MODELS; do [[ "$b" == "$name" ]] && { err "内置模型不可注销"; return 1; }; done
    [[ "$(active_name)" == "$name" ]] && { err "$name 是当前 active 模型，请先 use 到其他模型"; return 1; }
    [[ -f "$MODELS_LOCAL" ]] || { err "$name 不在用户注册表（$MODELS_LOCAL 不存在）"; return 1; }
    grep -q "^$name|" "$MODELS_LOCAL" || { err "$name 不在用户注册表"; return 1; }
    local tmp; tmp=$(mktemp)
    ( flock 9; grep -v "^$name|" "$MODELS_LOCAL" > "$tmp" || true; cat "$tmp" > "$MODELS_LOCAL" ) 9>"$MODELS_LOCAL.lock"
    rm -f "$tmp"
    ok "已注销 $name（权重缓存不删除；如需清盘手工 rm -rf 缓存目录）"
}

# 双机 HF 缓存扫描（JSON：repo、双机大小/分片数、注册关联、一致性）
cmd_cache() {
    local scan
    scan='H=$HOME/.cache/huggingface/hub; for d in "$H"/models--*; do [ -d "$d" ] || continue; '
    scan+='b=$(basename "$d"); mb=$(du -sm "$d" 2>/dev/null | cut -f1); '
    scan+='n=$(find "$d/snapshots" \( -type f -o -type l \) 2>/dev/null | wc -l); '
    scan+='r=${b#models--}; printf "%s\t%s\t%s\n" "${r/--//}" "$mb" "$n"; done'
    local t1 t2
    t1=$(bash -c "$scan")
    t2=$(rssh "$WORKER_FAB" "$scan" 2>/dev/null || rssh "$WORKER" "$scan" 2>/dev/null)
    { echo "$t1" | sed 's/^/spark1\t/'; echo "$t2" | sed 's/^/spark2\t/';
      local r name model
      for r in "${MODELS[@]}"; do IFS='|' read -r name _ model _ _ _ _ <<< "$r"; echo -e "reg\t$model\t$name"; done
    } | "$HF_PY" -c '
import json, sys
hosts, regs = {}, {}
for line in sys.stdin:
    p = line.rstrip("\n").split("\t")
    if len(p) < 3 or not p[0].strip():
        continue
    if p[0] in ("spark1", "spark2") and len(p) >= 4:
        try:
            repo, mb, n = p[1], int(p[2] or 0), int(p[3] or 0)
        except ValueError:
            continue
        hosts.setdefault(repo, {})[p[0]] = {"mb": mb, "files": n}
    elif p[0] == "reg":
        regs[p[1]] = p[2]
out = []
for repo in sorted(hosts):
    s1, s2 = hosts[repo].get("spark1"), hosts[repo].get("spark2")
    both = bool(s1 and s2)
    out.append({"repo": repo, "spark1_mb": s1["mb"] if s1 else 0,
                "spark2_mb": s2["mb"] if s2 else 0,
                "spark1_files": s1["files"] if s1 else 0,
                "spark2_files": s2["files"] if s2 else 0,
                "on_both": both, "size_match": both and s1["mb"] == s2["mb"],
                "registered_as": regs.get(repo, "")})
print(json.dumps(out, ensure_ascii=False))'
}

cmd_recipes() {
    local kw="${1:-}"
    [[ "$kw" =~ ^[A-Za-z0-9_.@/-]{0,40}$ ]] || { err "关键字含非法字符"; return 1; }
    exec sparkrun search "$kw"
}

cmd_recipe_show() {
    local recipe="${1:-}"
    [[ "$recipe" =~ ^@(official|eugr|local)/[A-Za-z0-9_.-]+$ ]] || { err "配方名非法"; return 1; }
    exec sparkrun show "$recipe"
}

# download <repo_id|@registry/recipe> [--host spark1|spark2|both] [--workers N] [--no-mirror] [--proxy] [--include GLOB ...]
# 代理端口预检：$1 空=本机 127.0.0.1，否则为远端 ip（经 ssh 测其本机 127.0.0.1）
_proxy_listening() {
    local t="timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/7890'"
    if [[ -z "${1:-}" ]]; then
        eval "$t" 2>/dev/null
    else
        ssh -o BatchMode=yes -o ConnectTimeout=3 "$1" "$t" 2>/dev/null
    fi
}

cmd_download() {
    local repo="$1"; shift || true
    # 允许直接传配方名：@reg/recipe → sparkrun show 解析真实权重仓库 ID
    if [[ "$repo" == @* ]]; then
        [[ "$repo" =~ ^@(official|eugr|local|sparkrun-transitional)/[A-Za-z0-9_.-]+$ ]] \
          || { err "配方名非法，形如 @official/name"; return 1; }
        local model
        model=$(sparkrun show "$repo" 2>/dev/null | sed -n 's/^Model:[[:space:]]*//p' | head -1)
        model="${model%%:*}"   # GGUF 配方可能是 repo:Q4_K_M，剥掉文件后缀
        [[ -n "$model" ]] || { err "无法从配方 $repo 解析 Model 行（sparkrun show 失败）"; return 1; }
        info "配方 $repo → 权重仓库 $model"
        repo="$model"
    fi
    [[ "$repo" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9_.-]+$ ]] \
      || { err "repo_id 形如 org/name（HuggingFace 仓库 ID）；配方名请带 @ 前缀，如 @official/xxx"; return 1; }
    # 拦截"把配方名去掉 @ 当仓库 ID"：这些 registry 前缀下的仓库 HF 上不存在，只会 401
    case "${repo%%/*}" in
      official|eugr|sparkrun-transitional)
        err "$repo 是 sparkrun 配方名而不是 HuggingFace 仓库 ID（HF 无此仓库，必然 401）。"
        err "正确做法：下载框填 @$repo（自动解析权重仓库），或先 '$0 recipe-show @$repo' 看 Model 行。"
        return 1 ;;
    esac
    local host=spark1 workers=8 mirror=1 use_proxy=0 includes=()
    while [[ $# -gt 0 ]]; do case "$1" in
      --host) host="$2"; shift 2 ;;
      --workers) workers="$2"; shift 2 ;;
      --no-mirror) mirror=0; shift ;;
      --proxy) use_proxy=1; shift ;;
      --include) includes+=("$2"); shift 2 ;;
      *) err "未知参数 $1"; return 1 ;;
    esac; done
    [[ "$host" =~ ^(spark1|spark2|both)$ ]] || { err "--host 仅 spark1|spark2|both"; return 1; }
    [[ "$workers" =~ ^[0-9]+$ && "$workers" -ge 1 && "$workers" -le 16 ]] || { err "--workers 1-16"; return 1; }
    local g; for g in "${includes[@]:-}"; do
      [[ -z "$g" ]] && continue
      [[ "$g" =~ ^[A-Za-z0-9_.*?\[\]{}/-]+$ ]] || { err "include glob 含非法字符: $g"; return 1; }
    done
    [[ "$host" == "both" && ${#includes[@]} -gt 0 ]] \
      && { err "双机分片模式不支持自定义 --include（自动按 safetensors 分片对半）"; return 1; }

    # 代理预检：默认 hf-mirror 不需要代理；勾选代理时 mihomo 必须已在对应机器监听
    if (( use_proxy )); then
        local rip=""
        if [[ "$host" != "spark1" ]]; then
            if rssh "$WORKER_FAB" "true" 2>/dev/null; then rip="$WORKER_FAB"; else rip="$WORKER"; fi
        fi
        if [[ "$host" == "spark1" ]]; then
            _proxy_listening || { err "spark1 的 127.0.0.1:7890 未监听：先 '$0 proxy up'，或取消代理（默认 hf-mirror 无需代理）"; return 1; }
        elif [[ "$host" == "spark2" ]]; then
            _proxy_listening "$rip" || { err "spark2($rip) 的 127.0.0.1:7890 未监听：代理需在 spark2 本机启动，或改用默认 hf-mirror 通道"; return 1; }
        else
            _proxy_listening || { err "spark1 的 7890 未监听：先 '$0 proxy up'，或取消代理"; return 1; }
            _proxy_listening "$rip" || { err "spark2($rip) 的 7890 未监听：双机代理模式需两机均运行代理；spark2 请手工启动，或改用默认 hf-mirror（推荐）"; return 1; }
        fi
    fi

    local env_extra=()
    (( mirror )) && env_extra+=(HF_ENDPOINT=https://hf-mirror.com)
    env_extra+=(HF_HUB_DISABLE_XET=1)
    (( use_proxy )) && env_extra+=(HTTP_PROXY=http://127.0.0.1:7890 HTTPS_PROXY=http://127.0.0.1:7890)
    local hf_args=(download "$repo" --max-workers "$workers")
    if [[ "$host" == "both" ]]; then
        _dl_both "$repo" "$workers" "$mirror" "$use_proxy" || return 1
        info "双机分片下载完成，开始 200G 内网双向合并 …"
        cmd_model_sync "$repo"
        return
    fi
    local g
    for g in "${includes[@]:-}"; do [[ -n "$g" ]] && hf_args+=(--include "$g"); done
    info "下载 $repo → $host（workers=$workers mirror=$mirror proxy=$use_proxy）"
    if [[ "$host" == "spark1" ]]; then
        env "${env_extra[@]}" "$HF_BIN" "${hf_args[@]}"
    else
        # 远程（fabric 优先）：hf CLI 路径按远端实际 venv 解析（spark2 为 ~/.venv/hf）
        local remote ip rhf
        if rssh "$WORKER_FAB" "true" 2>/dev/null; then ip="$WORKER_FAB"; else ip="$WORKER"; fi
        rhf=$(_hf_bin_remote "$ip") || { err "$ip 上找不到 hf CLI（~/.venv/sparkrun 或 ~/.venv/hf）"; return 1; }
        remote=$(printf '%q ' "${env_extra[@]}" "$rhf" "${hf_args[@]}")
        info "远程主机 $ip（$rhf）"
        ssh -o BatchMode=yes "$ip" "$remote"
    fi
    local rc=$?
    if (( rc == 0 )); then
        ok "下载完成。双机部署还需同步到另一台：$0 model-sync $repo"
        if [[ "$host" == "spark1" ]]; then
            nas_backup_repo "$repo"
        else
            info "本次只下载到 spark2，未备份 NAS；执行 $0 model-sync $repo 合并到双机后会自动备份"
        fi
    else err "下载失败（rc=$rc）：检查网络/mihomo（$0 proxy up）或换 --proxy 通道"; fi
    return $rc
}

# 双机自动分片：safetensors 对半 → spark2 显式 include 后半，spark1 全量 exclude 后半
_dl_both() {
    local repo="$1" workers="$2" mirror="$3" use_proxy="$4"
    info "列出 $repo 文件清单（safetensors 分片对半切）…"
    local envs=()
    [[ "$mirror" == "1" ]] && envs+=(HF_ENDPOINT=https://hf-mirror.com)
    (( use_proxy )) && envs+=(HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890)
    local files
    files=$(env "${envs[@]}" "$HF_PY" -c '
import sys,os
from huggingface_hub import HfApi
ep=os.environ.get("HF_ENDPOINT")
api=HfApi(endpoint=ep) if ep else HfApi()
print("\n".join(api.list_repo_files(sys.argv[1])))' "$repo") || return 1
    local shards=() f
    while IFS= read -r f; do [[ "$f" == *.safetensors ]] && shards+=("$f"); done <<< "$files"
    if [[ ${#shards[@]} -lt 2 ]]; then
        err "仅发现 ${#shards[@]} 个 safetensors 分片，无法双机对半；请用 --host spark1 整库下载"
        return 1
    fi
    local half=$(( ${#shards[@]} / 2 ))
    info "共 ${#shards[@]} 个分片：spark1 下 1-$half（+全部非分片文件），spark2 下 $((half+1))-${#shards[@]}"
    # spark1：全量下载但 exclude spark2 负责的后半分片（hf 1.8 支持 include/exclude 混用）
    # spark2：只 include 后半分片
    local s1args=(download "$repo" --max-workers "$workers")
    local s2args=(download "$repo" --max-workers "$workers")
    local i=0 f
    for f in "${shards[@]}"; do
        i=$((i+1))
        if (( i <= half )); then :; else s1args+=(--exclude "$f"); s2args+=(--include "$f"); fi
    done
    local logdir; logdir="$HOME/.local/state/dspark-dl"; mkdir -p "$logdir"
    local ts; ts=$(date +%Y%m%d-%H%M%S)
    local l1="$logdir/${repo//\//__}.$ts.spark1.log" l2="$logdir/${repo//\//__}.$ts.spark2.log"
    info "明细日志: $l1 / $l2（任务台显示 15s 心跳与最终结果）"
    local ip; if rssh "$WORKER_FAB" "true" 2>/dev/null; then ip="$WORKER_FAB"; else ip="$WORKER"; fi
    local rhf; rhf=$(_hf_bin_remote "$ip") || { err "$ip 上找不到 hf CLI（~/.venv/sparkrun 或 ~/.venv/hf）"; return 1; }
    info "spark2 使用 $ip 的 $rhf（hf $(ssh -o BatchMode=yes "$ip" "$rhf --version 2>/dev/null | head -1" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)）"
    local remote; remote=$(printf '%q ' HF_HUB_DISABLE_XET=1 "${envs[@]}" "$rhf" "${s2args[@]}")
    (
      env "${envs[@]}" HF_HUB_DISABLE_XET=1 "$HF_BIN" "${s1args[@]}" >"$l1" 2>&1
      echo $? > "$l1.rc"
    ) &
    local p1=$!
    (
      ssh -o BatchMode=yes "$ip" "$remote" >"$l2" 2>&1
      echo $? > "$l2.rc"
    ) &
    local p2=$!
    # 进度心跳：每 15s 报两机已落盘大小
    while kill -0 "$p1" 2>/dev/null || kill -0 "$p2" 2>/dev/null; do
        local d; d=$(hf_cache_dir "$repo")
        local m1 m2
        m1=$(du -sm "$d" 2>/dev/null | cut -f1); m1=${m1:-0}
        m2=$(rssh "$ip" "du -sm '$d' 2>/dev/null | cut -f1" 2>/dev/null); m2=${m2:-0}
        echo "[$(date +%T)] spark1 ${m1}MiB | spark2 ${m2}MiB"
        sleep 15
    done
    wait "$p1"; local r1=$(cat "$l1.rc" 2>/dev/null || echo 1)
    wait "$p2"; local r2=$(cat "$l2.rc" 2>/dev/null || echo 1)
    rm -f "$l1.rc" "$l2.rc"
    info "spark1 rc=$r1（tail: $(tail -n1 "$l1" 2>/dev/null)）"
    info "spark2 rc=$r2（tail: $(tail -n1 "$l2" 2>/dev/null)）"
    (( r1 == 0 && r2 == 0 )) || return 1
}

# model-sync <repo_id>：CX7 fabric 双向 rsync 合并 HF 缓存，再清 .incomplete
cmd_model_sync() {
    local repo="${1:-}"
    [[ "$repo" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9_.-]+$ ]] || { err "repo_id 形如 org/name"; return 1; }
    local d; d=$(hf_cache_dir "$repo")
    local ip; if rssh "$WORKER_FAB" "true" 2>/dev/null; then ip="$WORKER_FAB"; else ip="$WORKER"; fi
    [[ -d "$d" ]] || { err "本机缓存不存在: $d（先 download）"; return 1; }
    info "双向 rsync（$ip，200G fabric，无压缩）…"
    ssh -o BatchMode=yes "$ip" "mkdir -p '$d'"
    rsync -aH -e "ssh -o Compression=no" "$d/" "$ip:$d/" &
    local p1=$!
    rsync -aH -e "ssh -o Compression=no" "$ip:$d/" "$d/" &
    local p2=$!
    wait "$p1" || { err "spark1→spark2 同步失败"; return 1; }
    wait "$p2" || { err "spark2→spark1 同步失败"; return 1; }
    find "$d/blobs" -name '*.incomplete' -delete 2>/dev/null || true
    rssh "$ip" "find '$d/blobs' -name '*.incomplete' -delete 2>/dev/null" || true
    info "同步后双机占用："
    echo "  spark1 $(du -sh "$d" | cut -f1) ($(find "$d/snapshots" -type f | wc -l) 文件)"
    echo "  spark2 $(rssh "$ip" "du -sh '$d' | cut -f1") ($(rssh "$ip" "find '$d/snapshots' -type f | wc -l") 文件)"
    ok "双机权重一致"
    nas_backup_repo "$repo"
}

frpc_apply() {  # $1=llm 时确保隧道开（公网→网关:8002，网关强制 key，所有 vLLM 模型均可暴露）；$1=other 时关（stop/ComfyUI）
    local en; en=$(systemctl is-enabled "$FRPC_UNIT" 2>/dev/null)
    [[ -z "$en" ]] && return 0   # 未安装该单元则不介入
    if [[ "$1" == "llm" ]]; then
        if [[ "$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)" != "active" ]]; then
            sudo -n systemctl start "$FRPC_UNIT" 2>/dev/null \
                && ok "公网隧道 $FRPC_UNIT 已启动（$PUB_DOMAIN -> 网关:8002）"
        fi
    else
        if [[ "$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)" == "active" ]]; then
            sudo -n systemctl stop "$FRPC_UNIT" 2>/dev/null \
                && warn "已停公网隧道 $FRPC_UNIT（vLLM 未运行：stop/ComfyUI 形态；需要可 sudo systemctl start $FRPC_UNIT）"
        fi
    fi
}

cmd_use() {
    local target=${1:-}
    m_row "$target" >/dev/null || { err "未知模型: ${target:-（未指定）}"; echo; cmd_models; return 1; }
    local cur; cur=$(active_name)
    local recipe; recipe=$(m_field "$target" 2)
    local kind; kind=$(m_field "$target" 7); [[ -z "$kind" ]] && kind=sparkrun

    if [[ "$target" == "$cur" && "$(api_code)" == "200" ]]; then
        warn "$target 已在运行，无需切换"; return 0
    fi

    # 停服前先确认双机权重就位（注册≠下载；缺权重直接中止，不动当前服务）
    local wmodel; wmodel=$(m_field "$target" 3)
    if ! weights_ready "$kind" "$wmodel"; then
        err "权重未在双机就绪：$wmodel（TP=2 要求两机都有；已中止，当前服务未受影响）"
        echo "  双机下载（推荐）: $0 download $wmodel --host both"
        echo "  已单机下载过    : $0 model-sync $wmodel"
        echo "  面板操作        : 模型管理 → 下载权重（both），完成后再点「切换」"
        return 1
    fi

    info "切换: $cur → $target"
    info "编排: $kind  配方/启动器: $recipe (TP=2)"
    local img; img=$(m_field "$target" 6)
    [[ -n "$img" ]] && info "镜像: $img"
    # ComfyUI 与 vLLM 统一内存互斥：切模型前若 H3 在跑先停掉（等价 switch 的对端清理）
    if comfy_running; then
        info "检测到 ComfyUI H3 运行中，先停止两机 worker/分发器（统一内存互斥）"
        comfy_stop_all || { err "停止 ComfyUI 失败，中止切换"; return 1; }
        wait_mem_release
    fi
    local j; j=$(job_id)
    if [[ -n "$j" ]]; then
        svc_stop "$j" || { err "停止旧服务失败，中止切换"; return 1; }
        wait_mem_release
    fi
    [[ "$kind" == "docker-direct" ]] && ensure_mem_glm
    run_recipe "$target" || { err "启动失败（$0 logs 查看）"; return 1; }
    local hwait=12; [[ "$kind" == "docker-direct" ]] && hwait=25
    if wait_health "$hwait" "$kind"; then
        mkdir -p "$(dirname "$ACTIVE_FILE")"
        echo "$target" > "$ACTIVE_FILE"
        # glm53 看门狗可能在 ComfyUI 期间被停用，服务健康后自动恢复
        [[ "$kind" == "docker-direct" ]] && glm_watchdog_on
        frpc_apply llm
        ok "已切换到 $target，真实模型ID: $(m_field "$target" 3)"
        ok "Dify/外部工具固定入口无需改动：模型名 dspark，endpoint http://$HEAD_IP:8002/v1（公网 $PUB_BASE/v1），key 见 $0 access"
    else
        warn "active 仍记录为旧模型 $cur；服务就绪后可重跑 $0 use $target，或用 $0 status 查看实际状态"
    fi
}

cmd_start() {
    local n; n=$(active_name)
    if [[ "$(api_code)" == "200" ]]; then warn "服务已在运行（$n），无需重复启动"; return 0; fi
    local j; j=$(job_id)
    [[ -n "$j" ]] && { warn "发现残留服务 $j，先清理"; svc_stop "$j" || true; wait_mem_release; }
    local kind; kind=$(m_field "$n" 7); [[ -z "$kind" ]] && kind=sparkrun
    info "启动 $n : $(m_field "$n" 2) (TP=2, $kind)"
    local _img; _img=$(m_field "$n" 6); [[ -n "$_img" ]] && info "镜像: $_img"
    [[ "$kind" == "docker-direct" ]] && ensure_mem_glm
    run_recipe "$n" || { err "启动失败"; return 1; }
    [[ "$kind" == "docker-direct" ]] && wait_health 25 "$kind" || wait_health 12 "$kind"
    frpc_apply llm
}

cmd_stop() {
    local j; j=$(job_id)
    if [[ -z "$j" ]]; then warn "未发现运行中的服务"; return 0; fi
    svc_stop "$j" && ok "已停止"
    frpc_apply other
}

cmd_logs() {
    local j; j=$(job_id)
    [[ -z "$j" ]] && { err "未发现运行中的服务"; return 1; }
    if [[ "$j" == "docker-direct" ]]; then
        info "head(spark1) 日志（Ctrl-C 退出；worker 日志在 spark2: docker logs $GLM_NAME）"
        exec docker logs -f --tail "${1:-200}" "$GLM_NAME"
    fi
    exec sparkrun logs "${j#sparkrun:}"
}

cmd_elog() {
    local c; c=$(node_container 0)
    [[ -z "$c" ]] && { err "head 容器未运行"; return 1; }
    if [[ "$c" == "$GLM_NAME" ]]; then
        exec docker logs -f --tail "${1:-200}" "$GLM_NAME"
    fi
    exec docker exec -it "$c" tail -n "${1:-200}" -f /tmp/sparkrun_serve.log
}

cmd_monitor() {
    local ts; ts=$(date '+%F %T')
    for hl in "${HOSTS_LABEL[@]}"; do
        local ip=${hl%%:*}; name=${hl##*:}
        local iplabel="$ip"; [[ "$ip" != "$HEAD" ]] && iplabel="$ip (fabric优先)"
        echo "${C_B}== $name ($iplabel) @ $ts ==${C_0}"
        rssh "$ip" "
            nvidia-smi --query-gpu=utilization.gpu,temperature.gpu,power.draw --format=csv,noheader 2>/dev/null \
              | awk -F', ' '{printf \"  GPU  利用率 %s 温度 %s 功耗 %s\n\",\$1,\$2,\$3}'
            free -m | awk 'NR==2{printf \"  统一内存 %.0f / %.0f GiB\n\",\$3/1024,\$2/1024}'
            awk '{print \"  load \"\$1\" \"\$2\" \"\$3\"  (1/5/15min)\"}' /proc/loadavg
            df -h / | awk 'NR==2{printf \"  磁盘 / 已用 %s\n\",\$5}'"
    done
    echo "${C_B}== vLLM 运行指标 ($API) ==${C_0}"
    if [[ "$(api_code)" == "200" ]]; then
        curl -s -m 5 "$API/metrics" | awk '
            /^vllm:kv_cache_usage_perc\{/ {printf "  KV cache 占用 %.1f%%\n", $2*100}
            /^vllm:num_requests_running\{/  {r=$2}
            /^vllm:num_requests_waiting\{/  {w=$2}
            END {printf "  请求: 运行中 %s  排队中 %s\n", r+0, w+0}'
    else
        warn "API 不可达"
    fi
}

cmd_net() {
    info "管理网 ↔ spark2"
    ping -c3 -W2 "$WORKER" | tail -2
    for ip in "${CX7_IPS[@]}"; do
        info "CX7 $ip (MTU9000 巨帧)"
        if ping -c3 -W2 -M do -s 8972 "$ip" >/tmp/.dspark_ping 2>&1; then
            ok "通: $(grep -oE '[0-9]+% packet loss' /tmp/.dspark_ping)"
            grep rtt /tmp/.dspark_ping
        else err "不通"; tail -3 /tmp/.dspark_ping; fi
    done
    rm -f /tmp/.dspark_ping
}

cmd_proxy() {
    local act=${1:-st}
    case "$act" in
      st)
        pgrep -x mihomo >/dev/null && ok "mihomo 运行中, PID $(pgrep -x mihomo|tr '\n' ' ')" || warn "mihomo 未运行"
        ss -tln 2>/dev/null | grep -q ':7890' && ok "监听 127.0.0.1:7890" || warn "7890 未监听"
        ;;
      up)
        [[ -x "$MIHOMO_DIR/mihomo" ]] || { err "$MIHOMO_DIR/mihomo 不存在"; return 1; }
        if pgrep -x mihomo >/dev/null; then warn "已在运行"; else
            # 后台化三件套缺一不可：setsid 新会话 + 整组 fd 重定向（含 </dev/null）
            # + exec 变身为 mihomo。否则中间 bash 会以调用方管道为 stdout 长期存活，
            # 经 Popen(stdout=PIPE) 调用（如运维 Web）时父进程永远收不到 EOF
            setsid bash -c 'cd "$1" && exec ./mihomo -d "$1"' _ "$MIHOMO_DIR" \
                >>"$MIHOMO_DIR/mihomo.log" 2>&1 </dev/null &
            sleep 2; ok "已启动"; fi
        ;;
      down) pkill -x mihomo && ok "已停止" || warn "本就未运行" ;;
      test)
        info "经代理访问 GitHub（直连对照见下）"
        local p d
        p=$(curl -s -o /dev/null -m 15 -x http://127.0.0.1:7890 -w '%{time_total}s HTTP%{http_code}' https://api.github.com 2>&1) && ok "代理: $p" || err "代理访问失败"
        d=$(curl -s -o /dev/null -m 8 -w '%{time_total}s HTTP%{http_code}' https://api.github.com 2>&1) || d="超时/失败"
        info "直连: $d"
        ;;
      *) err "用法: $0 proxy st|up|down|test"; return 1 ;;
    esac
}

wd_timer_state() {  # 输出 "active/enabled" 形式的 timer 状态
    local a e
    a=$(systemctl is-active glm53-watchdog.timer 2>/dev/null)
    e=$(systemctl is-enabled glm53-watchdog.timer 2>/dev/null)
    echo "$a/$e"
}
cmd_watchdog() {  # status|on|off|recover|log|pm [N]
    local sub="${1:-status}"
    case "$sub" in
      on|enable)
          sudo -n systemctl enable --now glm53-watchdog.timer || { err "启用失败（检查 sudo 免密）"; return 1; }
          ok "看门狗已启用并开始每 2 分钟判活（开机自启已打开）"
          systemctl list-timers glm53-watchdog.timer --no-pager 2>/dev/null | head -2
          ;;
      off|disable)
          warn "关闭后 glm53 运行时崩溃将不再自动双机重建（模型服务本身不受影响，重启后也不会再启用）"
          if [[ "${DSPARK_ASSUME_YES:-0}" == "1" ]]; then ans=yes
          else read -r -p "确认关闭看门狗？输入 yes: " ans; fi
          [[ "$ans" == "yes" ]] || { info "已取消，看门狗仍为 $(wd_timer_state)"; return 0; }
          sudo -n systemctl disable --now glm53-watchdog.timer || { err "关闭失败"; return 1; }
          ok "看门狗已关闭（当前停用 + 禁用开机自启）。重新启用: $0 watchdog on"
          ;;
      status|st)
          [[ -x "$GLM_WATCHDOG" ]] || { err "缺少 $GLM_WATCHDOG（glm53 专属组件）"; return 1; }
          "$GLM_WATCHDOG" status
          ;;
      recover)
          [[ -x "$GLM_WATCHDOG" ]] || { err "缺少 $GLM_WATCHDOG"; return 1; }
          warn "将立即执行双机成对重建（head→worker stop → 等内存 → worker→head 启动）"
          if [[ "${DSPARK_ASSUME_YES:-0}" == "1" ]]; then ans=yes
          else read -r -p "确认？输入 yes 继续: " ans; fi
          [[ "$ans" == "yes" ]] || { info "已取消"; return 0; }
          "$GLM_WATCHDOG" recover
          ;;
      log|logs)
          exec journalctl -u glm53-watchdog -f --no-pager
          ;;
      pm)
          local n="${2:-1}"
          shopt -s nullglob
          local files=( "$HOME"/.local/state/glm53-watchdog/postmortem-*.log )
          [[ ${#files[@]} -gt 0 ]] || { info "暂无 postmortem 崩溃现场（看门狗尚未触发过自动恢复）"; return 0; }
          info "崩溃现场（新→旧，显示最新 $n 份）："
          ls -lt "${files[@]}" | head -$((n+1))
          echo
          local latest; latest=$(ls -t "${files[@]}" | head -1)
          info "最新一份尾部 40 行: $latest"
          tail -40 "$latest"
          ;;
      *) err "用法: $0 watchdog [on|off|status|recover|log|pm [N]]"; return 1 ;;
    esac
}

# ---------- Python: ask / bench ----------
run_py() {  # $1=ask|bench  $2=问题文本(ask)
local _n; _n=$(active_name)
DSPARK_MODEL="$(m_field "$_n" 3)" DSPARK_NOTHINK="$(m_field "$_n" 4)" \
python3 - "$1" "${2:-}" <<'PY'
import json, os, sys, time, urllib.request
mode, question = sys.argv[1], sys.argv[2]
API = "http://" + os.environ.get("DSPARK_HEAD", "192.168.31.51") + ":8000"
MODEL = os.environ["DSPARK_MODEL"]
NOTHINK = json.loads(os.environ["DSPARK_NOTHINK"])  # 各模型关闭思考的参数不同
def apply_nothink(payload):
    # reasoning_effort 是顶层请求参数（glm53 强制思考，仅可 low/high/max）；
    # thinking / enable_thinking 等才是 chat_template_kwargs（deepseek/qwen）
    top, tmpl = {}, {}
    for k, v in NOTHINK.items():
        (top if k == "reasoning_effort" else tmpl)[k] = v
    if top: payload.update(top)
    if tmpl: payload["chat_template_kwargs"] = tmpl
if mode == "ask":
    payload = {"model":MODEL,"messages":[{"role":"user","content":question}],
               "max_tokens":4096,"stream":True}
    apply_nothink(payload)
else:
    q = ("请围绕'张量并行如何加速大模型推理'写一篇结构完整、尽量详尽的技术说明，"
         "包含背景、原理、通信开销、适用场景，不少于800字。")
    payload = {"model":MODEL,"messages":[{"role":"user","content":q}],
               "max_tokens":4096,"stream":True,"stream_options":{"include_usage":True}}
    apply_nothink(payload)
req = urllib.request.Request(API+"/v1/chat/completions",
      data=json.dumps(payload).encode(), headers={"Content-Type":"application/json"})
t0=time.time(); first=None; ntok=0; gen_t0=None; usage=None
with urllib.request.urlopen(req, timeout=300) as r:
    for raw in r:
        raw=raw.decode().strip()
        if not raw.startswith("data:"): continue
        p=raw[5:].strip()
        if p=="[DONE]": break
        c=json.loads(p)
        if c.get("usage"): usage=c["usage"]
        ch=c.get("choices") or []
        if not ch: continue
        d=ch[0].get("delta",{}); piece=d.get("content")
        if piece:
            if first is None: first=time.time()-t0; gen_t0=time.time()
            ntok+=1
            if mode=="ask": sys.stdout.write(piece); sys.stdout.flush()
if mode=="ask":
    print(f"\n\n--- TTFT {first:.2f}s, 流式分片 {ntok} ---")
else:
    wall=time.time()-t0
    ct=usage["completion_tokens"] if usage else ntok
    gen=time.time()-gen_t0 if gen_t0 else wall
    print(f"TTFT 首 token 延迟 : {first:.2f} s")
    print(f"生成 token 数     : {ct}")
    print(f"纯生成速度        : {ct/gen:.1f} tok/s")
    print(f"端到端(含TTFT)    : {ct/wall:.1f} tok/s  (总耗时 {wall:.1f}s)")
PY
}

# ---------- ComfyUI MiniMax H3 双机 ----------
# 判断 vLLM 是否占用统一内存：本机或 spark2 任一存在 spark_* 容器即为忙。
# 注：spark2 不可达时 wssh 返回非 0，视为"不忙"（由后续 worker 启动自行报错）
comfy_vllm_busy() {
    docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^spark_' && return 0
    wssh "docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^spark_'" 2>/dev/null
}
# 分发器以 root 在 spark1 后台运行（要读 /root 下日志目录），venv 内自带 aiohttp
comfy_dispatch_start() {
    sudo -n bash -c 'mkdir -p /root/minnimax-h3/logs
      nohup '"$COMFY_VENV_PY"' '"$COMFY_DISPATCHER"' \
        > /root/minnimax-h3/logs/dispatcher.log 2>&1 &
      echo $! > /root/minnimax-h3/logs/dispatcher.pid'
}
comfy_dispatch_stop() {
    sudo -n bash -c 'kill "$(cat /root/minnimax-h3/logs/dispatcher.pid 2>/dev/null)" 2>/dev/null
      rm -f /root/minnimax-h3/logs/dispatcher.pid'
}
comfy_dispatch_alive() {
    sudo -n bash -c 'kill -0 "$(cat /root/minnimax-h3/logs/dispatcher.pid 2>/dev/null)" 2>/dev/null'
}
# 检测权重下载是否在跑（download-models.sh，或其写到 comfydata 的 wget）。
# 括号技巧防止 pgrep 匹配到执行命令的 shell 自身
comfy_download_busy() {
    pgrep -f '[d]ownload-models\.sh' >/dev/null 2>&1 \
        || pgrep -f '[w]get .*comfydata/models' >/dev/null 2>&1
}
# 任一 ComfyUI 组件在跑即视为占用：分发器 pid 活着，或本机/spark2 的 :8191 有响应
comfy_running() {
    comfy_dispatch_alive && return 0
    curl -s -o /dev/null -m 2 http://127.0.0.1:8191/system_stats && return 0
    wssh "curl -s -o /dev/null -m 2 http://127.0.0.1:8191/system_stats" >/dev/null 2>&1 && return 0
    return 1
}
# 停掉 ComfyUI 全部组件（分发器 + 两机 worker；comfyctl stop 会等 worker 进程退出）
comfy_stop_all() {
    info "停止分发器与两机 ComfyUI worker"
    comfy_dispatch_stop
    sudo -n bash "$COMFY_CTL" stop
    wssh "sudo -n bash $COMFY_CTL stop" || true
}
# 拉起 ComfyUI 全部组件。返回 0：spark1 worker 已起且分发器 :8188 返回 200
comfy_up_all() {
    [[ -f "$COMFY_CTL" ]] || { err "缺少 $COMFY_CTL（先在两机跑 comfy-deploy/host-deploy.sh）"; return 1; }
    info "启动 spark1 worker（:8191）"
    sudo -n bash "$COMFY_CTL" start || return 1
    info "启动 spark2 worker（fabric 优先，:8191）"
    wssh "sudo -n bash $COMFY_CTL start" || { warn "spark2 启动失败（不影响 spark1 单机使用）"; }
    info "启动统一入口分发器（:8188）"
    comfy_dispatch_alive || comfy_dispatch_start
    local i code=""
    for i in $(seq 1 15); do
        code=$(curl -s -o /dev/null -w '%{http_code}' -m 2 "$COMFY_URL/_dispatcher/status" || true)
        [[ "$code" == "200" ]] && break; sleep 1
    done
    if [[ "$code" == "200" ]]; then
        ok "ComfyUI H3 已就绪：浏览器打开 $COMFY_URL"
        info "工作流（两机共享）: ~/comfydata/workflows-h3/（推荐先用 *_fp8.json）"
        return 0
    fi
    warn "两 worker 已启动，但分发器 :8188 未响应（HTTP $code）"
    err  "排查: $0 comfy logs d"
    return 1
}
# glm53 看门狗是 vLLM 侧组件：ComfyUI 期间必须停用（否则它发现容器缺失会自动重建抢内存），
# 切回 glm53 后必须恢复（崩溃自动双机重建依赖它）
comfy_watchdog_off() {
    if systemctl is-enabled glm53-watchdog.timer >/dev/null 2>&1; then
        info "停用 glm53 看门狗（切回 vLLM 时由 switch 自动恢复）"
        sudo -n systemctl disable --now glm53-watchdog.timer >/dev/null 2>&1 \
            || warn "看门狗停用失败（sudo?），它可能自动重建 vLLM 抢占内存"
    fi
}
glm_watchdog_on() {
    if systemctl is-enabled glm53-watchdog.timer >/dev/null 2>&1; then
        ok "glm53 看门狗已启用（active/enabled）"
    else
        info "重新启用 glm53 看门狗（每 2 分钟判活 + 崩溃双机重建）"
        sudo -n systemctl enable --now glm53-watchdog.timer >/dev/null 2>&1 \
            && ok "glm53 看门狗已启用" || warn "看门狗启用失败，手动执行: $0 watchdog on"
    fi
}

# 一键双向切换：glm53(vLLM 双机) ↔ comfy(ComfyUI H3 双机)
# 幂等：目标形态已在运行时直接提示；任一端停止后都等两机内存释放再起新端
cmd_switch() {
    local target="${1:-}"
    case "$target" in
      comfy|comfyui|h3)
        info "一键切换: vLLM 模型服务 → ComfyUI MiniMax H3"
        if comfy_running && ! comfy_vllm_busy; then
            warn "ComfyUI H3 已在运行，无需切换：$COMFY_URL"
            return 0
        fi
        # 1) 停 vLLM（sparkrun 或 glm53 裸 docker），2) 等两机统一内存释放
        local j; j=$(job_id)
        if [[ -n "$j" ]]; then
            svc_stop "$j" || { err "停止 vLLM 失败，中止切换"; return 1; }
            wait_mem_release
        fi
        # 3) 关看门狗（可能是上次异常退出留下的启用态），4) 起 ComfyUI
        comfy_watchdog_off
        frpc_apply other
        comfy_up_all
        ;;
      glm53|glm|vllm)
        info "一键切换: ComfyUI MiniMax H3 → glm53 vLLM 双机"
        # 1) 停 ComfyUI（若在跑）
        if comfy_running; then
            comfy_stop_all || { err "停止 ComfyUI 失败，中止切换"; return 1; }
        fi
        # 2) 处理 vLLM 侧在跑的容器
        local j; j=$(job_id)
        if [[ -n "$j" ]]; then
            if [[ "$j" == "docker-direct" && "$(api_code)" == "200" ]]; then
                warn "glm53 已在运行且 API 健康，无需切换"
                glm_watchdog_on
                frpc_apply llm
                mkdir -p "$(dirname "$ACTIVE_FILE")"; echo glm53 > "$ACTIVE_FILE"
                return 0
            fi
            warn "发现 $j 容器（API 不健康或为其他模型），先停止"
            svc_stop "$j" || { err "停止旧服务失败，中止切换"; return 1; }
        fi
        # 3) 等内存（ComfyUI/旧容器释放），glm53 冷盘 repack 前再保 40GiB 底线
        wait_mem_release
        ensure_mem_glm
        # 4) 双机启动 + 健康检查（冷启动 7-10 分钟）
        run_recipe glm53 || { err "启动失败（$0 logs 查看）"; return 1; }
        if wait_health 25 docker-direct; then
            mkdir -p "$(dirname "$ACTIVE_FILE")"; echo glm53 > "$ACTIVE_FILE"
            glm_watchdog_on   # 5) 恢复看门狗
            frpc_apply llm
            ok "已切换到 glm53，模型ID: $(m_field glm53 3)"
            ok "Dify/外部工具固定入口：模型名 dspark，$PUB_BASE/v1 或 http://$HEAD_IP:8002/v1（$0 access 查看 key）"
        else
            err "glm53 在限定时间内未就绪；看门狗未自动启用，服务恢复后执行 $0 watchdog on"
            return 1
        fi
        ;;
      *) err "用法: $0 switch <glm53|comfy>"; return 1 ;;
    esac
}
cmd_comfy() {
    local sub="${1:-st}"
    case "$sub" in
      up|start)   cmd_switch comfy ;;          # 一键：自动停 vLLM/关看门狗/起 ComfyUI
      down|stop)
        comfy_stop_all
        # 不自动恢复看门狗：down 可能只为临时清空内存排查；切回 glm53 走 switch 会自动启用
        if ! systemctl is-enabled glm53-watchdog.timer >/dev/null 2>&1; then
          warn "glm53 看门狗仍为停用状态；切回 vLLM 用: $0 switch glm53（或手动 $0 watchdog on）"
        fi
        ;;
      st|status)
        sudo -n bash "$COMFY_CTL" status
        wssh "sudo -n bash $COMFY_CTL status" 2>/dev/null || echo "spark2: 不可达"
        if comfy_dispatch_alive; then
          echo "分发器: 运行中 ($COMFY_URL)"
          curl -s -m 3 "$COMFY_URL/_dispatcher/status" | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception:
    print("  (状态读取失败)"); raise SystemExit
for n,s in d["workers"].items():
    state = "健康" if s["ok"] else "离线"
    print("  %-7s %s 运行%s 排队%s  %s" % (n, state, s["running"], s["pending"], s["device"]))'
        else echo "分发器: 已停止"; fi
        ;;
      log|logs)
        case "${2:-1}" in
          1|spark1) exec sudo -n tail -n 80 -f /root/minnimax-h3/logs/comfyui.log ;;
          2|spark2) exec wssh "sudo -n tail -n 80 -f /root/minnimax-h3/logs/comfyui.log" ;;
          d|dispatcher) exec sudo -n tail -n 80 -f /root/minnimax-h3/logs/dispatcher.log ;;
          *) err "用法: $0 comfy logs [1|2|d]"; return 1 ;;
        esac
        ;;
      *) err "用法: $0 comfy [up|down|st|logs [1|2|d]]（up 等价于 switch comfy）"; return 1 ;;
    esac
}

usage() { sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; }

# ---------- 运维 Web 面板（systemd 托管：开机自启 + 崩溃自动重启；免密、仅管理网） ----------
ops_web_unit_content() {
  cat <<EOF
[Unit]
Description=DGX Spark ops web panel (dspark.sh HTTP shell)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/文档
ExecStart=/usr/bin/python3 $OPS_WEB_PY
Restart=always
RestartSec=3
Environment=OPSWEB_PORT=$OPS_WEB_PORT

[Install]
WantedBy=multi-user.target
EOF
}

ops_web_ensure_unit() {
  [[ -f "$OPS_WEB_PY" ]] || { err "缺少 $OPS_WEB_PY"; return 1; }
  local cur
  cur=$(sudo -n systemctl cat "$OPS_WEB_UNIT" 2>/dev/null | sed -n '/^# \/etc\/systemd/,$p' | grep -v '^# /etc' || true)
  if [[ "$cur" != "$(ops_web_unit_content)" ]]; then
    info "安装/更新 systemd 单元 $OPS_WEB_UNIT_FILE"
    ops_web_unit_content | sudo -n tee "$OPS_WEB_UNIT_FILE" >/dev/null \
      || { err "写入单元失败（需要 sudo -n）"; return 1; }
    sudo -n systemctl daemon-reload
  fi
}

cmd_web() {
    local sub="${1:-st}"
    case "$sub" in
      up|start)
        ops_web_ensure_unit || return 1
        sudo -n systemctl enable --now "$OPS_WEB_UNIT" >/dev/null 2>&1 \
          || { err "启动失败：sudo systemctl status $OPS_WEB_UNIT"; return 1; }
        sleep 1.5
        if [[ "$(systemctl is-active "$OPS_WEB_UNIT")" == "active" ]]; then
          ok "Web 面板已启动（systemd active/enabled，崩溃自动重启、开机自启）"
        else
          err "启动失败，日志：journalctl -u $OPS_WEB_UNIT -e"; sudo -n journalctl -u "$OPS_WEB_UNIT" -n 10 --no-pager; return 1
        fi
        cmd_web url
        ;;
      down|stop)
        sudo -n systemctl stop "$OPS_WEB_UNIT" 2>/dev/null \
          && ok "Web 面板已停止（仍 enabled，开机会自动拉起；彻底禁用: sudo systemctl disable $OPS_WEB_UNIT）" \
          || warn "服务未运行或停止失败"
        ;;
      st|status)
        local a e
        a=$(systemctl is-active "$OPS_WEB_UNIT" 2>/dev/null); e=$(systemctl is-enabled "$OPS_WEB_UNIT" 2>/dev/null)
        if [[ "$a" == "active" ]]; then
          ok "运行中: $a/$e 端口 $OPS_WEB_PORT（日志 journalctl -u $OPS_WEB_UNIT -f）"
          curl -s -o /dev/null -w '  本机 HTTP %{http_code}（免密，200 正常）\n' \
            -m 3 "http://127.0.0.1:$OPS_WEB_PORT/api/info"
        else err "未运行（$0 web up；当前 $a/$e）"; return 1; fi
        ;;
      url|info)
        echo "  地址: http://$HEAD_IP:$OPS_WEB_PORT"
        echo "  认证: 免密（家庭内网）；仅接受管理网 192.168.31.0/24、fabric 与本机来源，其余 403"
        echo "  托管: systemd $OPS_WEB_UNIT（Restart=always 崩溃自动重启 + enabled 开机自启）"
        ;;
      pw|passwd)
        info "面板已改为免密访问，无密码可改（仅管理网来源可访问）" ;;
      *) err "用法: $0 web [up|down|st|url]（免密，systemd 托管）"; return 1 ;;
    esac
}

# ---------- 公网暴露（frp 隧道 → Caddy HTTPS → vLLM/shim） ----------
cmd_tunnel() {
  case "${1:-st}" in
    st|status)
      local fails=0 a e code
      a=$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)
      e=$(systemctl is-enabled "$FRPC_UNIT" 2>/dev/null)
      if [[ "$a" == "active" && "$e" == "enabled" ]]; then
        ok "$FRPC_UNIT 服务: $a/$e"
      else
        err "$FRPC_UNIT 服务: ${a:-未安装}/${e:-—}（应为 active/enabled）"; ((fails++)); fi
      [[ -f "$VLLM_KEY_FILE" ]] \
        && ok "vLLM API key 已配置 ($(cut -c1-14 "$VLLM_KEY_FILE")…，$VLLM_KEY_FILE)" \
        || { err "key 文件缺失: $VLLM_KEY_FILE"; ((fails++)); }
      info "公网链路探测 $PUB_DOMAIN（Caddy:443 → frps → frpc → 网关:8002 / shim:8001）"
      local probe_path label
      for probe_path in "/v1/models:网关:8002" "/shim/v1/models:shim:8001"; do
        local p="${probe_path%%:*}"; label="${probe_path#*:}"
        # 无 key 必须 401：证明整条链路通且对应服务强制鉴权
        code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 "$PUB_BASE$p" 2>/dev/null || echo 000)
        if [[ "$code" == "401" ]]; then ok "[$label] $p 无 key 401（链路通 + 鉴权生效）"
        else err "[$label] $p 无 key 返回 $code（预期 401；000=外网/DNS/隧道/Caddy 路由，200=鉴权失效）"; ((fails++)); fi
        if [[ -f "$VLLM_KEY_FILE" ]]; then
          code=$(curl -s -o /dev/null -w '%{http_code}' -m 15 \
            -H "Authorization: Bearer $(cat "$VLLM_KEY_FILE")" "$PUB_BASE$p" 2>/dev/null || echo 000)
          [[ "$code" == "200" ]] && ok "[$label] 带 key 200（固定模型名 dspark）" \
            || { err "[$label] 带 key 返回 $code（预期 200）"; ((fails++)); }
        fi
      done
      if [[ -f "$VLLM_KEY_FILE" ]]; then
        local body
        body=$(curl -s -m 15 -H "Authorization: Bearer $(cat "$VLLM_KEY_FILE")" "$PUB_BASE/v1/models" 2>/dev/null)
        [[ "$body" == *'"id": "dspark"'* || "$body" == *'"id":"dspark"'* ]] \
          && ok "公网模型列表为固定名 dspark" \
          || { err "公网模型列表异常: ${body:0:120}"; ((fails++)); }
      fi
      return $fails
      ;;
    url|info)
      local n0 m0; n0=$(active_name); m0=$(m_field "$n0" 3)
      echo "  固定模型名      : dspark（切换任何模型都不变；$0 access 查完整信息）"
      echo "  当前实际模型    : $n0 ($m0)"
      echo "  公网网关 API    : $PUB_BASE/v1（推荐；全量经 shim）"
      echo "  公网 shim API   : $PUB_BASE/shim/v1（Dify 构建模式 legacy functions 直连入口）"
      echo "  内网网关 API    : http://$HEAD_IP:8002/v1"
      echo "  内网 shim API   : http://$HEAD_IP:8001/v1"
      if [[ -f "$VLLM_KEY_FILE" ]]; then
        echo "  API Key(Bearer) : $(cat "$VLLM_KEY_FILE")"
        echo "  key 文件(双机)  : $VLLM_KEY_FILE"
      else warn "key 文件不存在: $VLLM_KEY_FILE（网关/shim fail-closed）"; fi
      echo "  内网直连(排障)  : :8000/v1 真实模型名"
      echo "  隧道服务        : systemctl status $FRPC_UNIT（日志 $0 tunnel log）"
      ;;
    key)
      [[ -f "$VLLM_KEY_FILE" ]] || { err "key 文件不存在: $VLLM_KEY_FILE"; return 1; }
      cat "$VLLM_KEY_FILE"
      ;;
    rotate)
      local ans cur0
      cur0=$(active_name)
      if [[ "$cur0" == "glm53" ]]; then
        warn "轮换后将重启双机 glm53（约 8 分钟中断），并需更新所有客户端的 key"
      else
        warn "当前为 $cur0（sparkrun 引擎不校验 key，安全边界是 :8002 网关）：轮换只改 key 文件，网关热读取立即生效，无需重启"
      fi
      if [[ "${DSPARK_ASSUME_YES:-0}" == "1" ]]; then ans=yes
      else read -r -p "确认轮换 key？输入 yes: " ans; fi
      [[ "$ans" == "yes" ]] || { info "已取消"; return 0; }
      local nk; nk="sk-dspark-$(openssl rand -hex 20)"
      printf '%s\n' "$nk" > "$VLLM_KEY_FILE"; chmod 600 "$VLLM_KEY_FILE"
      scp -q "$VLLM_KEY_FILE" "$WORKER_FAB:.config/dspark/vllm_api_key" \
        || scp -q "$VLLM_KEY_FILE" "$WORKER:.config/dspark/vllm_api_key" \
        || { err "同步 key 到 spark2 失败"; return 1; }
      wssh "chmod 600 $VLLM_KEY_FILE"
      ok "新 key 已写双机: $nk（网关即时生效）"
      if [[ "$cur0" == "glm53" ]]; then
        info "重启 glm53 使引擎 --api-key 生效…"
        cmd_stop; wait_mem_release; cmd_start || { err "重启失败，查 $0 elog"; return 1; }
      fi
      ok "完成。请立即更新 Dify/其他客户端（或运维面板复制新接入信息）；旧 key 已失效"
      ;;
    log|logs)
      exec journalctl -u "$FRPC_UNIT" -f --no-pager
      ;;
    *) err "用法: $0 tunnel [st|url|key|rotate|log]"; return 1 ;;
  esac
}

# 当前模型全部接入信息（地址/key/模型名/服务态），供面板与外部工具配置；--json 给 Web
cmd_access() {
  local name model kind hcode key="" fa fe ga ge probe=""
  name=$(active_name)
  model=$(m_field "$name" 3)
  kind=$(m_field "$name" 7); [[ -z "$kind" ]] && kind=sparkrun
  hcode=$(api_code)
  [[ -f "$VLLM_KEY_FILE" ]] && key=$(cat "$VLLM_KEY_FILE")
  fa=$(systemctl is-active "$FRPC_UNIT" 2>/dev/null)
  fe=$(systemctl is-enabled "$FRPC_UNIT" 2>/dev/null)
  ga=$(systemctl is-active dspark-model-gateway 2>/dev/null)
  ge=$(systemctl is-enabled dspark-model-gateway 2>/dev/null)
  local healthy=false; [[ "$hcode" == "200" ]] && healthy=true
  local fixed="dspark"
  local lan="http://$HEAD_IP:8002/v1" pub="$PUB_BASE/v1"
  local shimlan="http://$HEAD_IP:8001/v1" shimpub="$PUB_BASE/shim/v1"
  local eng="$API/v1" shm="$shimlan"
  # 隧道在跑且引擎健康才探公网（401=链路通+鉴权生效；000=不通；200=鉴权失效）
  local probe="" sprobe=""
  if [[ "$fa" == "active" && "$healthy" == true ]]; then
    probe=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "$pub/models" 2>/dev/null || echo 000)
    sprobe=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "$shimpub/models" 2>/dev/null || echo 000)
  fi
  local shim_ok=false; [[ "$healthy" == true ]] && shim_ok=true
  local curl_ex="curl -H 'Authorization: Bearer $key' $pub/chat/completions -H 'Content-Type: application/json' -d '{\"model\":\"dspark\",\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}'"
  local curl_shim="curl -H 'Authorization: Bearer $key' $shimpub/chat/completions -H 'Content-Type: application/json' -d '{\"model\":\"dspark\",\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}'"

  if [[ "${1:-}" == "--json" ]]; then
    ACC_NAME="$name" ACC_MODEL="$model" ACC_KIND="$kind" ACC_HEALTHY="$healthy" \
    ACC_KEY="$key" ACC_LAN="$lan" ACC_PUB="$pub" ACC_ENG="$eng" ACC_SHM="$shm" \
    ACC_SHIMPUB="$shimpub" \
    ACC_SHIM_OK="$shim_ok" ACC_FA="$fa" ACC_FE="$fe" ACC_GA="$ga" ACC_GE="$ge" \
    ACC_PROBE="$probe" ACC_SPROBE="$sprobe" ACC_CURL="$curl_ex" ACC_CURL_SHIM="$curl_shim" python3 - <<'PY'
import json, os
print(json.dumps({
    "fixed_model": "dspark",
    "active": {"name": os.environ.get("ACC_NAME", ""),
               "model": os.environ.get("ACC_MODEL", ""),
               "kind": os.environ.get("ACC_KIND", ""),
               "healthy": os.environ.get("ACC_HEALTHY") == "true"},
    "api_key": os.environ.get("ACC_KEY", ""),
    "lan_url": os.environ.get("ACC_LAN", ""),
    "public_url": os.environ.get("ACC_PUB", ""),
    "engine_url": os.environ.get("ACC_ENG", ""),
    "shim_url": os.environ.get("ACC_SHM", ""),
    "public_shim_url": os.environ.get("ACC_SHIMPUB", ""),
    "shim_available": os.environ.get("ACC_SHIM_OK") == "true",
    "services": {"gateway": f'{os.environ.get("ACC_GA","")}/{os.environ.get("ACC_GE","")}',
                 "frpc": f'{os.environ.get("ACC_FA","")}/{os.environ.get("ACC_FE","")}'},
    "public_probe": os.environ.get("ACC_PROBE", ""),
    "public_shim_probe": os.environ.get("ACC_SPROBE", ""),
    "curl": os.environ.get("ACC_CURL", ""),
    "curl_shim": os.environ.get("ACC_CURL_SHIM", ""),
}, ensure_ascii=False))
PY
    return
  fi

  hr; info "当前模型：$name"
  echo "  真实模型ID : $model"
  echo "  编排类型   : $kind"
  echo "  引擎健康   : $([[ $healthy == true ]] && echo '是 (/v1/health 200)' || echo "否 (HTTP $hcode)")"
  hr; info "统一接入（推荐；切换模型后以下信息全部不变；模型名一律 dspark，同一把 key）"
  echo "  模型名     : dspark（固定）"
  echo "  API Key    : ${key:-（缺失！网关/shim fail-closed）}"
  echo "  公网网关   : $pub"
  echo "  公网 shim  : $shimpub（legacy functions 直连入口；与网关等价，Dify 构建模式可用）"
  echo "  内网网关   : $lan"
  echo "  内网 shim  : $shimlan"
  echo "  隧道服务   : $FRPC_UNIT ${fa:-未安装}/$fe；网关 dspark-model-gateway ${ga:-未安装}/$ge"
  if [[ -n "$probe" ]]; then
    [[ "$probe" == "401" ]] && ok "公网网关无 key 探测 401（链路通 + 鉴权生效）" || err "公网网关无 key 探测 $probe（401 才正常）"
    [[ "$sprobe" == "401" ]] && ok "公网 shim 无 key 探测 401（链路通 + 鉴权生效）" || err "公网 shim 无 key 探测 $sprobe（401 才正常）"
  else echo "  公网探测   : 跳过（隧道未运行或引擎未健康）"; fi
  hr; info "直连地址（仅内网排障；:8000 只认真实模型名）"
  echo "  原生引擎   : $eng  模型名 $model$([[ "$kind" == "docker-direct" ]] && echo '（glm53 需带同一把 key）')"
  hr; info "curl 示例（公网）"
  echo "  $curl_ex"
  echo "  $curl_shim"
}

# ---------- 入口 ----------
case "${1:-}" in
  models|ls) [[ "${2:-}" == "--json" ]] && cmd_models_json || cmd_models ;;
  access|ac) cmd_access "${2:-}" ;;
  current)   cmd_current ;;
  use)       [[ $# -ge 2 ]] || { echo "用法: $0 use <模型短名>；可用: ${MODELS[*]%%|*}"; exit 1; }; cmd_use "$2" ;;
  switch|to) [[ $# -ge 2 ]] || { echo "用法: $0 switch <glm53|comfy>"; exit 1; }; cmd_switch "$2" ;;
  status)    cmd_status ;;
  check)     cmd_check ;;
  start)     cmd_start ;;
  stop)      cmd_stop ;;
  restart)   cmd_stop; wait_mem_release; cmd_start ;;
  logs)      cmd_logs ;;
  elog)      cmd_elog "${2:-}" ;;
  monitor)   cmd_monitor ;;
  net)       cmd_net ;;
  proxy)     cmd_proxy "${2:-st}" ;;
  ask)       [[ $# -ge 2 ]] || { echo "用法: $0 ask \"你的问题\""; exit 1; }; run_py ask "$2" ;;
  bench)     run_py bench ;;
  watchdog|wd) cmd_watchdog "${2:-status}" "${3:-1}" ;;
  comfy)      cmd_comfy "${2:-st}" "${3:-}" ;;
  web)        cmd_web "${2:-st}" ;;
  tunnel|tu)  cmd_tunnel "${2:-st}" ;;
  cache)     cmd_cache ;;
  recipes)   shift; cmd_recipes "${1:-}" ;;
  recipe-show) [[ $# -ge 2 ]] || { echo "用法: $0 recipe-show <@registry/name>"; exit 1; }; cmd_recipe_show "$2" ;;
  download)  shift; [[ $# -ge 1 ]] || { echo "用法: $0 download <org/name> [--host spark1|spark2|both] [--workers N] [--no-mirror] [--proxy] [--include GLOB ...]"; exit 1; }; cmd_download "$@" ;;
  model-sync) [[ $# -ge 2 ]] || { echo "用法: $0 model-sync <org/name>"; exit 1; }; cmd_model_sync "$2" ;;
  register)  shift; [[ $# -ge 2 ]] || { echo "用法: $0 register <短名> <@registry/recipe> [关思考JSON]"; exit 1; }; cmd_register "$@" ;;
  unregister) [[ $# -ge 2 ]] || { echo "用法: $0 unregister <短名>"; exit 1; }; cmd_unregister "$2" ;;
  h|help|--help|-h) usage ;;
  *) echo "未知命令: ${1:-}"; echo; usage; exit 1 ;;
esac

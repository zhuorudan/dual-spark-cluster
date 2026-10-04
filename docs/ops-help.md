# 双 Spark 模型管理运维 · 帮助文档

本页说明运维 Web 面板（`http://<HEAD_IP>:8200`）中**模型全套生命周期管理**的用法：查询、下载、部署（注册）、切换、停止、注销，以及双机权重同步与常见问题排查。

> 面板免密，但仅接受管理网 `192.168.31.0/24`、CX7 fabric `192.168.0.0/24`、`192.168.1.0/24` 与本机来源；其他网段一律 403。面板由 systemd 单元 `dspark-opsweb.service` 托管，开机自启、崩溃自动拉起。

## 1. 集群拓扑速览

| 角色 | 主机名 | 管理网 | CX7 fabric（200G） | 说明 |
| --- | --- | --- | --- | --- |
| head | spark1 | <HEAD_IP> | 192.168.0.51 | 面板/API 入口、vLLM node0 |
| worker | spark2 | <WORKER_LAN_IP> | 192.168.0.52 | vLLM node1，SSH fabric 优先、管理网兜底 |

- 两台均为 NVIDIA DGX Spark（GB10，aarch64），张量并行（TP=2）要求**模型权重在两机缓存中都存在且一致**。
- 权重缓存目录：`~/.cache/huggingface/hub/models--org--name`（两机同路径）。
- 大权重内网合并走 CX7 fabric，rsync 关闭压缩；156GB 全量约 2 分钟。

## 2. 模型生命周期总览

```
配方市场查询 ──► 下载权重 ──► 双机同步 ──► 注册到可切换列表 ──► 切换(use) ──► 停止
(recipes)      (download)   (model-sync)  (register)           服务中      (stop)
                                                                          └─► 注销(unregister)
```

1. **查**：在「模型管理」页查看已注册模型、双机缓存占用与一致性，并搜索 sparkrun 配方市场获取可用配方与 HF 仓库 ID。
2. **下**：对 HF 仓库执行 `download`，支持单机或双机分片并行下载，默认走 hf-mirror 镜像。
3. **合**：双机模式下载完成后自动双向 rsync；单机下载后可点「双向同步」补齐另一台。
4. **注册**：把 `@registry/recipe` 配方以短名登记到可切换列表（仅写用户配置文件，不改脚本）。
5. **切**：在已注册模型上点「切换」，面板自动双机停旧服务、等显存释放、按新配方拉起并等待健康检查。
6. **停 / 注销**：停止只关容器；注销只移除注册行，**都不会删除权重文件**。

## 3. 「模型管理」页面说明

### 3.1 已注册模型

数据来自 `dspark.sh models --json`。字段：短名、sparkrun 配方、HF 模型 ID、说明、镜像覆盖标记。

| 按钮 | 作用 | 备注 |
| --- | --- | --- |
| 切换 | `dspark.sh use <短名>`，双机重建服务 | 中断当前 API；sparkrun 模型约 2-6 分钟，glm53 冷启动 7-10 分钟 |
| 停止 | 停止当前 active 模型的双机容器 | 不改变 active 记录，可用 start 拉回 |
| 注销 | 从用户注册表移除该短名 | 仅「自注册」模型可注销；内置模型与当前运行模型受保护 |

- **内置模型**：`deepseek`、`qwen38`、`glm53`，写在 `dspark.sh` 中，不可注销。
- **自注册模型**：保存在 `~/.config/dspark/models.local.conf`（行格式同内置：`短名|配方|模型ID|关思考JSON|说明|镜像|类型`），升级脚本不会丢失。

### 3.2 双机 HF 权重缓存

数据来自 `dspark.sh cache`：逐机 `du`/`find` 扫描缓存目录并输出 JSON。

- **双机一致**（绿）：两机目录大小一致，可直接用于 TP=2。
- **大小不一致**（红）：可能下载中断，先点「双向同步」做 rsync 合并。
- **仅单机**（黄）：点「补下载到 sparkN」自动把仓库 ID 与目标机填入下载表单；或先同步（同步会把已有文件推到另一台）。
- 「已注册」列显示该权重是否被某个短名引用，便于识别孤儿缓存。

### 3.3 配方市场

- 关键字搜索（留空列全部）调用 `sparkrun search`，展示配方名、Runtime、TP、节点范围、显存占比、HF 模型 ID。
- **详情**：`sparkrun show @reg/name`，查看模型 ID、`tensor_parallel`、`max_model_len`、默认开关与完整启动命令——注册时「关思考参数」可参考其默认值。
- **下载**：把该配方对应的 HF 模型 ID 填入下载表单（也可直接填 `@配方名`，自动解析；按钮已自动填好正确 ID）。
- **注册**：把配方名填入注册表单并自动生成短名（可改）。
- 注册源当前为 `@official` 与 `@eugr`（`@local` 仅命令行支持）。

### 3.4 下载权重表单

| 项 | 说明 |
| --- | --- |
| HF 仓库 ID | 两种填法：① HF 仓库 `org/name`（如 `nvidia/MiniMax-M2.7-NVFP4`）；② 直接填配方名 `@official/xxx`，提交后自动 `sparkrun show` 解析出权重仓库。**注意 `official/xxx`（不带 @）不是合法仓库，必被拒绝**；最省事是点配方行的「下载」按钮自动填入 |
| 目标机器 | `spark1` / `spark2` / `both`（默认） |
| 并发数 | `hf download --max-workers`，默认 8，建议 4-12 |
| 不走镜像 | 直连 huggingface.co（默认**勾选镜像** hf-mirror.com） |
| 走代理 | 附加 mihomo `http://127.0.0.1:7890`。默认 hf-mirror **无需勾选**；勾选后提交时会预检端口：spark1 需先 `proxy up`，双机模式还要求 spark2 本机也有代理监听，否则任务直接拒绝并提示 |
| include | glob 过滤（如 `*.json`），仅单机模式可用；留空整库下载 |

下载是**长任务**：提交后自动跳到任务台并展开该任务，SSE 实时显示输出（1 秒级推送，无需手动刷新）。双机模式每 15 秒打印两机已落盘 MiB 心跳；逐文件的 tqdm 进度条在两机明细日志 `~/.local/state/dspark-dl/*.log`（任务开头会打印完整路径）。下载任务**不与切换互斥**（可并行排队），但双机合并完成前不要切换到该模型。

**任务台实时查看说明**：

- 任务行点击即展开/折叠；展开中的运行中任务标题显示「● 实时」，列表每 5 秒自动刷新状态且**保持展开与历史内容**，向上滚动翻历史不会被新输出打断（滚回底部即恢复跟随）。
- 导航栏「任务台」徽标显示运行中任务数；未展开的运行中任务也在后台实时更新状态。
- 结束后行内显示 `done rc=0`（绿点）或非零 rc（红点）；输出保留到面板重启。看到 rc=0 且心跳停止、自动 rsync 合并完成才算下载真正结束。

下载固定设置 `HF_HUB_DISABLE_XET=1`：xethub 通道在当前网络下会 401，走普通 HTTPS 拉取才能稳定。

**双机分片原理（both）**：用 HfApi 列出仓库全部文件，把 `*.safetensors` 按序号对半——spark1 下载前半分片 + 全部非分片文件（`--exclude` 后半），spark2 经 fabric SSH 只下载后半分片（`--include`），两机各吃一半家庭带宽；全部结束后自动执行双向 rsync 合并，并清理 `*.incomplete`。分片数少于 2 的小仓库不支持 both，请用单机下载。

### 3.5 注册部署表单

- **短名**：小写字母开头，仅小写字母/数字/中划线，最长 21 字符（与已有内置/自注册名不可重复）。
- **配方**：`@official/xxx` 或 `@eugr/xxx`；提交时面板会先 `sparkrun show` 校验存在性并自动取模型 ID 与说明。
- **关思考参数 JSON**：不同模型字段不同，不确定时留空（默认 `{}`）：
  - DeepSeek 系：`{"thinking":false}`
  - Qwen3 系：`{"enable_thinking":false}`
  - GLM-5.3：`{"reasoning_effort":"low"}`（仅接受 low/high/max）
- 注册只写配置，**不会自动下载或切换**。已注册模型表格每行带权重状态标签：
  「权重就绪」（双机缓存都在）才可点「切换」；「权重缺失·先下载」时该行只有「去下载权重」按钮
  （自动把仓库 ID 和 both 填入下方下载表单）；「仅 spark1 有权重」时给「先双向同步」按钮。
- 命令行同样有闸：`dspark.sh use <短名>` 在双机权重未就绪时会直接拒绝（rc=1），
  **不会停掉当前正在服务的模型**，并提示 `download --host both` / `model-sync` 命令。
- 注销不删权重；如需清盘，手工 `rm -rf ~/.cache/huggingface/hub/models--org--name`（两机都要清）。

## 4. 切换与停止的注意事项

- 切换是**最伤筋动骨的操作**：面板会双机 stop、轮询等待两机显存释放，再按 worker→head 顺序拉起。
- 历史教训：glm53 重启存在时序竞态（worker 连到旧 head、gloo 组网可卡 30 分钟）。`switch`/`use` 已固化「双机停干净 → 等内存 → 再启动」流程，**不要手工只重启单机容器**。
- 冷启动期间 `/v1/health` 不可用属正常；任务台日志结束并返回 rc=0、仪表盘卡片变绿才算成功。
- 正在做大权重下载/rsync 时可以切换其他模型，但**不要切换到正在下载的那个**。
- ComfyUI H3 形态与 vLLM 互斥，形态切换会自动停对端并管好看门狗。

### 4.1 切换后 Dify/外部工具要改什么？——什么都不用（统一模型网关）

spark1 的 `dspark-model-gateway`（:8002，systemd 自启/自愈，巡检第 9 项）对客户端固定暴露一个模型名，
面板切换任何模型都不需要改 Dify/其他工具的模型名和地址：

- **接入信息一屏查全**：本页顶部「接入信息（Dify / 外部工具配置）」卡片列出
  当前模型的公网/内网 Base URL、固定模型名、**API Key 完整值**、隧道/网关状态和 curl 示例，均带复制按钮；
  命令行等价物：`dspark.sh access`。
- Dify 供应商 OpenAI-API-compatible：模型名 **`dspark`**，
  endpoint 内网 `http://<HEAD_IP>:8002/v1`、公网 `https://llm.your-domain.com/v1`，
  **API Key 必须填卡片里的 key**（网关与 glm53 引擎共用同一把；无 key/错 key 返回 401）。
- 网关对**所有模型统一**经 :8001 shim 转发到当前 active 引擎（:8000）：shim 模型无关，
  Dify 构建模式的 legacy `functions` 协议在 deepseek/qwen38/glm53/自注册模型上都可用；
  非 agent 普通请求经 shim 原样透传，无副作用（同时自动注入 key）。
- **公网所有 vLLM 形态常开**（不再限 glm53）：公网只打到网关 :8002 与 shim :8001 两道强制 key 闸门，不转发裸引擎；
  stop / ComfyUI 形态两条隧道一起自动关。切换进行中的请求返回 502/503，健康后自动恢复。
- `tunnel rotate` 轮换 key 后网关热生效（glm53 引擎需重启，脚本自动做），
  但所有客户端要同步换 key——来本卡片复制新值即可。
- 需要在 Dify 里同时区分多个真实模型名时，才用直连方式：构建模式 Agent endpoint 用
  `http://<HEAD_IP>:8001/v1`（公网用 `https://llm.your-domain.com/shim/v1`），
  **模型名也可以统一填 `dspark`**（shim 自动按 active 改写，真实模型名同样兼容）；纯对话才用 :8000（见 README「Dify 配置」）。

## 5. 特殊形态：glm53（docker-direct）

`glm53`（`Intel/GLM-5.3-Flash-W4A16-AutoRound`，MTP3 + PMU128）**不走 sparkrun 配方市场**，而是由 `dspark.sh` 直接编排裸 docker 容器（镜像 `spark-recipes/glm53-autoround-mtp3-pmu128:20260903-difyrc`），原因是其启动参数、双机组网与补丁均为定制。

- 切换/停止/看门狗/公网隧道都用面板上既有内置按钮，不需要也不应该在配方市场重新注册它。
- 配方市场里的 `@eugr/glm-5.3-flash` 对应的是另一个权重（`local-inference-lab/GLM-5.3-Flash-NVFP4-Spark`，sparkrun 标准配方），两者不是一回事，勿混用。

## 6. 公网访问

- 两个等价入口，模型名都固定 **`dspark`**、同一把 Bearer key（无 key 一律 401）：
  - 网关（推荐）：`https://llm.your-domain.com/v1` — Caddy → frps 8090 → frpc → :8002（内部全量经 shim）
  - Dify 协议 shim 直连：`https://llm.your-domain.com/shim/v1` — Caddy 去 `/shim` 前缀 → frps 8091 → frpc → :8001
- **所有 vLLM 形态两个入口都可用**（deepseek/qwen38/glm53/自注册）；stop / ComfyUI 形态两条隧道一起自动关。
- shim 自身也强制 Bearer key（不只依赖 glm53 引擎鉴权），故非 glm53 模型经公网 shim 调用同样安全。
- 地址、key、模型名、双路隧道状态与两条 curl 示例：模型管理页顶部「接入信息」卡片（等价 `dspark.sh access`）。
- 轮换 key：`dspark.sh tunnel rotate`；glm53 会自动重启引擎（约 8 分钟中断），其他形态网关/shim 热生效；完成后所有客户端需换新 key。
- 排障：公网 shim 返回 502 → frpc 是否注册 8091（`dspark.sh tunnel log`）或公网 Caddy 的 /shim 路由；返回 200 但 401 → key 不对；`dspark.sh tunnel st` 会分别探测两条公网链路。

## 7. 故障排查入口

| 现象 | 处理 |
| --- | --- |
| 下载报 `Repository Not Found / 401` 且仓库名像 `official/xxx` | 把配方名误当成了 HF 仓库 ID。改为 `@official/xxx` 或点配方行「下载」按钮；真实权重仓库以 recipe-show 的 Model 行为准（本例是 `nvidia/MiniMax-M2.7-NVFP4`） |
| 下载 401 / xethub 报错 | 确认未关闭镜像且保留 `HF_HUB_DISABLE_XET=1`（面板默认已带）；或勾「走代理」并先 `proxy up`（任务启动时会预检 7890 端口） |
| 下载慢 | hf-mirror 仍慢时改 mihomo 代理通道；家庭带宽上限约 300Mbps，双机 both 模式接近翻倍 |
| 缓存双机不一致 | 模型管理页「双向同步」；仍不一致看 `~/.local/state/dspark-dl/*.log` 与 `.rc` |
| 切换后长时间不健康 | 任务台看日志；疑似组网卡死用「手动双机重建」（watchdog recover），不要单机重启 |
| 注册提示找不到配方 | 用配方市场搜索确认配方名与 registry 前缀；`@local` 配方只能在 CLI 注册 |
| 权重在但切换报缺文件 | 检查缓存页文件数两机是否相等；rsync 同步后再切 |
| 面板打不开 | spark1 上 `dspark.sh web st` 或 `systemctl status dspark-opsweb`；日志 `~/.local/state/dspark-opsweb/opsweb.log` |
| 403 | 客户端不在允许网段；面板设计上不做公网暴露 |

更深入的排查（10 项巡检、CX7 巨帧、引擎日志、看门狗崩溃现场）见「服务控制」「日志」页与主文档 README。

## 8. 命令行速查（ssh spark1 后）

```bash
./dspark.sh models --json                 # 结构化模型列表（含当前态/内置标记）
./dspark.sh cache                         # 双机缓存 JSON（大小/文件数/一致性/注册关联）
./dspark.sh recipes [关键字]              # sparkrun 配方市场搜索
./dspark.sh recipe-show @eugr/xxx         # 配方详情
./dspark.sh download org/name             # 默认：spark1 + hf-mirror + 8 并发
./dspark.sh download @official/xxx        # 直接给配方名，自动解析权重仓库
./dspark.sh download org/name --host both # 双机分片并行，完成自动 rsync 合并
./dspark.sh download org/name --host spark2 --proxy --workers 12
./dspark.sh model-sync org/name           # 200G fabric 双向 rsync
./dspark.sh register myname @eugr/xxx '{"enable_thinking":false}'
./dspark.sh unregister myname             # 只删注册行，不删权重
./dspark.sh use myname                    # 切换
./dspark.sh stop                          # 停止当前服务
```

全部模型管理动作都有面板二次确认；写注册表用 flock 防并发损坏，切换类动作面板侧全局串行，下载/同步/注册不参与互斥。

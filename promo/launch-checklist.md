# 发布操作清单（Launch Checklist）

> 帖子文案在 `launch-posts.md`。本清单是执行顺序和渠道细节。

## 发布前（一次性）

- [ ] **改 GitHub 密码**（密码已出现在聊天记录里）— Settings → Password and authentication
- [ ] 开启 2FA（Passkey 或 TOTP）
- [ ] GitHub 头像/简介完善（纯技术风：头像用非真人图即可，bio 写 "Homelab LLM infrastructure. Dual DGX Spark."）
- [ ] 检查仓库 social preview 图（Settings → General → Social preview，可传一张自制架构图 1280×640）

## 发布节奏（重要：先 Reddit 后 V2EX）

### Day 0（周二或周三，欧美上午 = 北京晚上 8-11 点）

- [ ] **Reddit r/LocalLLaMA** 发英文帖（流量最大的黄金渠道）
  - 用 Title B（数据流标题）
  - 发帖后 1 小时内自己回一条补充Technical details（性能表+功耗）
  - 当晚守 2 小时回评论——前 3 小时的互动决定是否上 Hot
- [ ] 同步发 **X/Twitter**（如果有账号）：一句话 + 架构图 + GitHub 链接，@ NVIDIA 开发者关系账号运气好会被转

### Day 1

- [ ] **V2EX** 分享创造节点发中文帖
  - 用标题 A，正文照抄，结尾加"欢迎拍砖"
  - V2EX 对纯推仓库有敌意，重点回评论，态度谦虚
- [ ] **即刻**「一起玩AI」圈子、朋友圈（圈子里有同行）
- [ ] **掘金/知乎**：把 DEPLOY-CN.md 拆成文章《双 DGX Spark 生产级集群搭建全记录》，文末挂仓库

### Day 3-7

- [ ] 回复所有 issue/评论（这个阶段回复率=项目寿命）
- [ ] 补 MiniMax-M2.7 实测数据进 README + CHANGELOG v0.2
- [ ] 观察数据：star 来源（GitHub Traffic Insights 能看 referrer）决定后续内容投入方向

## 渠道特性备忘

| 渠道 | 特点 | 关键动作 |
|---|---|---|
| r/LocalLLaMA | 140万+人，DGX Spark 是近期热词 | 数据说话，别营销腔；周二-周四发 |
| V2EX | 程序员为主，反感广告但尊重干货 | 分享创造节点，自嘲式开头 |
| 知乎/掘金 | 长尾搜索流量 | 教程向，SEO 关键词"DGX Spark 集群" |
| 即刻 | AI 圈子密度高，传播快但浅 | 短内容+图 |

## 数据追踪

- 每周记录：stars / forks /Traffic Insights（views、unique visitors、clone 数）
- 判断信号：clone > 10/周 说明真有人用 → 加速 Pro 版开发
- issue 里出现的第一个真实外部用户问题 = 市场验证信号，值得截图留证

## 变现触发点

- **star ≥ 50**：挂 GitHub Sponsors
- **star ≥ 100 或 clone 周均 > 20**：出 Pro license 页（¥299/年：多集群管理/企微钉钉告警/远程看板）
- **知乎专栏关注 ≥ 500**：开付费小册《家用双 Spark：私有 AI 集群从零到生产》

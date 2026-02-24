# LOF Premium/Discount Feishu Bot

这个项目仅依赖 **GitHub Actions** 定时运行，不需要本地常驻执行。

## 功能
- 拉取 A 股全部 LOF 场内基金实时行情（AKShare/东方财富）
- 计算折溢价：`premium_pct = (price / iopv - 1) * 100`
- 生成两张 Top10 表：
  - 溢价 Top10（`premium_pct` 降序）
  - 折价 Top10（`premium_pct` 升序）
- 通过飞书自定义机器人发送一张交互式卡片（同卡片包含两张表）

## 定时规则
- GitHub Actions 使用 UTC cron
- 北京时间 14:20 = UTC 06:20
- 工作日（周一到周五）执行：`20 6 * * 1-5`

## 配置步骤
1. 在飞书群添加“自定义机器人”，复制 webhook URL。
2. 进入 GitHub 仓库 `skill-ku`：
   - `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`
   - Name: `FEISHU_WEBHOOK_URL`
   - Value: 你的飞书 webhook URL

## 手动验证一次
1. 打开 GitHub 仓库 `Actions` 页面。
2. 选择工作流 **LOF Premium/Discount Feishu Push**。
3. 点击 **Run workflow** 手动触发。
4. 成功后，你会在飞书群看到一张卡片，包含：
   - 推送时间（北京时间）
   - 溢价 Top10 表
   - 折价 Top10 表

## 目录
- `src/run.py`: 数据抓取、计算、卡片构建、推送逻辑
- `.github/workflows/lof.yml`: 定时/手动触发的 GitHub Actions 工作流
- `requirements.txt`: Python 依赖

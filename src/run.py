import datetime as dt
import os
import time
from typing import Tuple

import akshare as ak
import pandas as pd
import requests


def get_lof_df() -> pd.DataFrame:
    """Fetch LOF real-time spot data and normalize required columns."""
    last_err = None
    for attempt in range(1, 4):
        try:
            print(f"[INFO] 拉取 LOF 实时行情（尝试 {attempt}/3）...")
            df = ak.fund_lof_spot_em()
            print(f"[INFO] 拉取完成，原始行数: {len(df)}")
            break
        except Exception as exc:
            last_err = exc
            if attempt < 3:
                time.sleep(2)
    else:
        raise RuntimeError(f"拉取 LOF 实时行情失败（已重试 3 次）: {last_err}") from last_err

    rename_map = {
        "代码": "code",
        "名称": "name",
        "最新价": "price",
        "IOPV实时估值": "iopv",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required = {"code", "name", "price", "iopv"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            "数据源字段缺失: "
            f"{sorted(missing)}; 实际字段: {list(df.columns)}"
        )

    df = df[["code", "name", "price", "iopv"]].copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["iopv"] = pd.to_numeric(df["iopv"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["price", "iopv"])
    df = df[df["iopv"] > 0]
    after = len(df)
    if after == 0:
        raise RuntimeError(
            f"清洗后无可用 LOF 数据（原始 {before} 条，保留 {after} 条）"
        )

    df["premium_pct"] = (df["price"] / df["iopv"] - 1.0) * 100.0
    print(f"[INFO] 清洗后有效行数: {after}")
    return df


def top_tables(df: pd.DataFrame, n: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    premium_top = df.sort_values("premium_pct", ascending=False).head(n).copy()
    discount_top = df.sort_values("premium_pct", ascending=True).head(n).copy()
    return premium_top, discount_top


def df_to_markdown_table(df: pd.DataFrame) -> str:
    """Render markdown without extra dependencies (no tabulate needed)."""
    view = df[["code", "name", "price", "iopv", "premium_pct"]].copy()
    view["price"] = view["price"].map(lambda v: f"{v:.4f}")
    view["iopv"] = view["iopv"].map(lambda v: f"{v:.4f}")
    view["premium_pct"] = view["premium_pct"].map(lambda v: f"{v:+.2f}%")

    headers = ["code", "name", "price", "iopv", "premium_pct"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| --- | --- | --- | --- | --- |",
    ]

    for _, row in view.iterrows():
        cells = [str(row[h]).replace("|", "\\|") for h in headers]
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


def clamp_markdown(md: str, limit: int = 12000) -> str:
    if len(md) <= limit:
        return md
    return md[:limit] + "\n\n（表格过长，已截断）"


def build_feishu_card(
    title: str,
    push_time_cn: str,
    premium_table_md: str,
    discount_table_md: str,
) -> dict:
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue",
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**A股 LOF 场内基金折溢价榜**\n\n"
                            f"- 推送时间（北京时间）: {push_time_cn}\n"
                            "- 定时规则: 工作日 14:20（UTC 06:20）"
                        ),
                    },
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "**溢价 Top10（premium_pct 最高）**"},
                },
                {"tag": "div", "text": {"tag": "lark_md", "content": premium_table_md}},
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "**折价 Top10（premium_pct 最低）**"},
                },
                {"tag": "div", "text": {"tag": "lark_md", "content": discount_table_md}},
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "_数据来源：AKShare/东方财富（LOF 场内实时行情）_",
                    },
                },
            ],
        },
    }


def feishu_post(webhook: str, payload: dict) -> None:
    try:
        print("[INFO] 正在推送飞书卡片...")
        resp = requests.post(webhook, json=payload, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"飞书 webhook 推送失败: {exc}") from exc

    try:
        body = resp.json()
    except ValueError:
        print("[WARN] 飞书返回非 JSON，按 HTTP 状态视为成功")
        return

    # Feishu webhook commonly returns one of:
    # {"code": 0, "msg": "success"} or {"StatusCode": 0, "StatusMessage": "success"}
    if "code" in body:
        code = body.get("code")
        if code != 0:
            raise RuntimeError(
                f"飞书业务返回失败: code={code}, msg={body.get('msg')}, body={body}"
            )
    elif "StatusCode" in body:
        code = body.get("StatusCode")
        if code != 0:
            raise RuntimeError(
                "飞书业务返回失败: "
                f"StatusCode={code}, StatusMessage={body.get('StatusMessage')}, body={body}"
            )

    print(f"[INFO] 飞书推送完成，返回: {body}")


def main() -> None:
    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("缺少环境变量 FEISHU_WEBHOOK_URL，请在 GitHub Secrets 中配置")

    tz = dt.timezone(dt.timedelta(hours=8))
    now = dt.datetime.now(tz)
    push_time_cn = now.strftime("%Y-%m-%d %H:%M")
    print(f"[INFO] 当前北京时间: {push_time_cn}")

    df = get_lof_df()
    premium_top, discount_top = top_tables(df, n=10)
    print(
        "[INFO] Top10 计算完成: "
        f"premium={len(premium_top)} 条, discount={len(discount_top)} 条"
    )

    premium_md = clamp_markdown(df_to_markdown_table(premium_top), limit=12000)
    discount_md = clamp_markdown(df_to_markdown_table(discount_top), limit=12000)

    payload = build_feishu_card(
        title="LOF 折溢价 Top10",
        push_time_cn=push_time_cn,
        premium_table_md=premium_md,
        discount_table_md=discount_md,
    )
    print("[INFO] 卡片构建完成")
    feishu_post(webhook, payload)


if __name__ == "__main__":
    main()

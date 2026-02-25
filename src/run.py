import datetime as dt
import os
import time
from typing import Tuple

import akshare as ak
import pandas as pd
import requests


def _pick_column(columns: list, keyword: str) -> str:
    for col in columns:
        if keyword in str(col):
            return str(col)
    return ""


def get_lof_estimation_fallback_df(codes: pd.Series) -> pd.DataFrame:
    """
    Fetch LOF estimated NAV (used as IOPV fallback) from Eastmoney valuation API.
    """
    symbols = ("LOF", "场内交易基金")
    frames = []
    code_set = set(codes.astype(str).str.zfill(6).tolist())

    for symbol in symbols:
        last_err = None
        for attempt in range(1, 4):
            try:
                print(f"[INFO] 拉取 {symbol} 净值估算（尝试 {attempt}/3）...")
                est_df = ak.fund_value_estimation_em(symbol=symbol)
                break
            except Exception as exc:
                last_err = exc
                if attempt < 3:
                    time.sleep(2)
        else:
            print(f"[WARN] 拉取 {symbol} 净值估算失败: {last_err}")
            continue

        if est_df.empty:
            print(f"[WARN] {symbol} 净值估算返回空数据")
            continue

        code_col = _pick_column(list(est_df.columns), "基金代码")
        iopv_col = _pick_column(list(est_df.columns), "估算数据-估算值")
        if not code_col or not iopv_col:
            print(
                f"[WARN] {symbol} 净值估算缺少关键列: code_col={code_col}, "
                f"iopv_col={iopv_col}, 实际列={list(est_df.columns)}"
            )
            continue

        tmp = est_df[[code_col, iopv_col]].copy()
        tmp.columns = ["code", "iopv_est"]
        tmp["code"] = tmp["code"].astype(str).str.extract(r"(\d{6})", expand=False)
        tmp["code"] = tmp["code"].str.zfill(6)
        tmp["iopv_est"] = pd.to_numeric(tmp["iopv_est"], errors="coerce")
        tmp = tmp[tmp["code"].isin(code_set)]
        tmp = tmp.dropna(subset=["code"]).drop_duplicates("code")
        tmp = tmp[tmp["iopv_est"] > 0]
        if not tmp.empty:
            frames.append(tmp)
            print(f"[INFO] {symbol} 净值估算可用条数: {len(tmp)}")
        else:
            print(f"[WARN] {symbol} 净值估算无可用 IOPV")

    if not frames:
        return pd.DataFrame(columns=["code", "iopv_est"])

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates("code", keep="first")
    return merged


def get_lof_open_nav_fallback_df(codes: pd.Series) -> pd.DataFrame:
    """
    Fallback to open-fund daily unit NAV when valuation estimate is unavailable.
    """
    code_set = set(codes.astype(str).str.zfill(6).tolist())
    last_err = None
    for attempt in range(1, 4):
        try:
            print(f"[INFO] 拉取开放式基金净值（尝试 {attempt}/3）...")
            nav_df = ak.fund_open_fund_daily_em()
            break
        except Exception as exc:
            last_err = exc
            if attempt < 3:
                time.sleep(2)
    else:
        print(f"[WARN] 拉取开放式基金净值失败: {last_err}")
        return pd.DataFrame(columns=["code", "iopv_nav"])

    if nav_df.empty:
        print("[WARN] 开放式基金净值返回空数据")
        return pd.DataFrame(columns=["code", "iopv_nav"])

    code_col = _pick_column(list(nav_df.columns), "基金代码")
    nav_cols = [c for c in nav_df.columns if "单位净值" in str(c)]
    if not code_col or not nav_cols:
        print(
            f"[WARN] 开放式基金净值缺少关键列: code_col={code_col}, "
            f"nav_cols={nav_cols}, 实际列={list(nav_df.columns)}"
        )
        return pd.DataFrame(columns=["code", "iopv_nav"])

    tmp = nav_df[[code_col] + nav_cols].copy()
    tmp.rename(columns={code_col: "code"}, inplace=True)
    tmp["code"] = tmp["code"].astype(str).str.extract(r"(\d{6})", expand=False)
    tmp["code"] = tmp["code"].str.zfill(6)
    tmp = tmp[tmp["code"].isin(code_set)]

    for col in nav_cols:
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")

    # Prefer latest unit NAV; if missing then fallback to earlier day unit NAV.
    tmp["iopv_nav"] = tmp[nav_cols].bfill(axis=1).iloc[:, 0]
    tmp = tmp[["code", "iopv_nav"]]
    tmp = tmp.dropna(subset=["code"]).drop_duplicates("code")
    tmp = tmp[tmp["iopv_nav"] > 0]
    print(f"[INFO] 开放式基金净值可用条数: {len(tmp)}")
    return tmp


def _fetch_lof_codes() -> pd.DataFrame:
    """Get LOF fund list via akshare fund_name_em, filter to exchange-traded LOF codes."""
    import re
    df = ak.fund_name_em()
    # Filter: name contains 'LOF' and code starts with 16/50/51 (exchange-traded)
    lof = df[df["\u57fa\u91d1\u7b80\u79f0"].str.contains("LOF", case=False, na=False)].copy()
    lof = lof[lof["\u57fa\u91d1\u4ee3\u7801"].str.match(r"^(16|50|51)\d{4}$")]
    lof = lof.rename(columns={"\u57fa\u91d1\u4ee3\u7801": "code", "\u57fa\u91d1\u7b80\u79f0": "name"})
    lof = lof[["code", "name"]].copy()
    lof["code"] = lof["code"].astype(str).str.zfill(6)
    # 排除定开基金（名称含"定开"或"定期开放"）
    lof = lof[~lof["name"].str.contains(r"定开|定期开放", na=False)]
    lof = lof.drop_duplicates("code")
    return lof.reset_index(drop=True)


def _fetch_prices_tencent(codes: list) -> dict:
    """Batch fetch real-time prices from Tencent qt.gtimg.cn.
    Returns dict: code -> {"price": float, "volume": int}.
    Only includes entries with price > 0 and volume > 0 (actually traded).
    """
    symbols = []
    for c in codes:
        c = str(c).zfill(6)
        if c.startswith(("16", "15")):
            symbols.append(f"sz{c}")
        elif c.startswith(("50", "51")):
            symbols.append(f"sh{c}")
        else:
            symbols.append(f"sz{c}")

    result = {}
    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            for line in resp.text.strip().split("\n"):
                line = line.strip().rstrip(";")
                if "=" not in line:
                    continue
                _, val = line.split("=", 1)
                val = val.strip('"')
                parts = val.split("~")
                if len(parts) < 7:
                    continue
                code = parts[2]
                try:
                    price = float(parts[3])
                    volume = int(parts[6])
                    if price > 0 and volume > 0:
                        result[code.zfill(6)] = {"price": price, "volume": volume}
                except (ValueError, IndexError):
                    pass
        except Exception as exc:
            print(f"[WARN] Tencent batch {i} failed: {exc}")
        if i + batch_size < len(symbols):
            time.sleep(0.3)

    return result


def get_lof_df() -> pd.DataFrame:
    """Fetch LOF real-time spot data using Tencent quotes + Eastmoney fund list."""
    print("[INFO] 获取 LOF 基金列表...")
    df = _fetch_lof_codes()
    print(f"[INFO] LOF 基金列表: {len(df)} 只")

    print("[INFO] 从腾讯行情获取实时价格...")
    price_map = _fetch_prices_tencent(df["code"].tolist())
    print(f"[INFO] 腾讯行情返回有成交的基金: {len(price_map)} 只")

    df["price"] = df["code"].map(lambda c: price_map.get(c, {}).get("price"))
    # 只保留有场内成交的
    before_filter = len(df)
    df = df.dropna(subset=["price"]).copy()
    print(f"[INFO] 过滤无成交后: {len(df)} 只（排除 {before_filter - len(df)} 只）")

    required = {"code", "name", "price"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            "数据源字段缺失: "
            f"{sorted(missing)}; 实际字段: {list(df.columns)}"
        )

    if "iopv" not in df.columns:
        print("[WARN] LOF 行情接口未返回 IOPV 列，启用净值估算回填")
        df["iopv"] = pd.NA

    df = df[["code", "name", "price", "iopv"]].copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["iopv"] = pd.to_numeric(df["iopv"], errors="coerce")

    iopv_missing_before = int(df["iopv"].isna().sum())

    est_df = get_lof_estimation_fallback_df(df["code"])
    if not est_df.empty:
        df = df.merge(est_df, on="code", how="left")
        df["iopv"] = df["iopv"].fillna(df["iopv_est"])
        df = df.drop(columns=["iopv_est"])
    iopv_missing_after_est = int(df["iopv"].isna().sum())

    nav_df = get_lof_open_nav_fallback_df(df["code"])
    if not nav_df.empty:
        df = df.merge(nav_df, on="code", how="left")
        df["iopv"] = df["iopv"].fillna(df["iopv_nav"])
        df = df.drop(columns=["iopv_nav"])
    iopv_missing_after_nav = int(df["iopv"].isna().sum())

    print(
        "[INFO] IOPV 回填统计: "
        f"回填前缺失={iopv_missing_before}, "
        f"估算回填后缺失={iopv_missing_after_est}, "
        f"净值回填后缺失={iopv_missing_after_nav}"
    )

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


def truncate_name(name: str, limit: int = 12) -> str:
    text = "" if name is None else str(name).strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def build_table_rows(df: pd.DataFrame, max_rows: int = 10) -> list[dict]:
    view = df[["code", "name", "price", "iopv", "premium_pct"]].copy()
    view["code"] = view["code"].astype(str).str.zfill(6)
    view["name"] = view["name"].map(lambda v: truncate_name(v, limit=12))
    view["price"] = view["price"].map(lambda v: f"{v:.4f}")
    view["iopv"] = view["iopv"].map(lambda v: f"{v:.4f}")
    view["premium_pct"] = view["premium_pct"].map(lambda v: f"{v:+.2f}%")

    rows = []
    for _, row in view.head(max_rows).iterrows():
        rows.append(
            {
                "code": str(row["code"]),
                "name": str(row["name"]),
                "price": str(row["price"]),
                "iopv": str(row["iopv"]),
                "premium_pct": str(row["premium_pct"]),
            }
        )

    while len(rows) < max_rows:
        rows.append(
            {
                "code": "-",
                "name": "-",
                "price": "-",
                "iopv": "-",
                "premium_pct": "-",
            }
        )
    return rows


def build_table_component(df: pd.DataFrame, max_rows: int = 10) -> dict:
    return {
        "tag": "table",
        "page_size": max_rows,
        "row_height": "low",
        "header_style": {
            "text_align": "left",
            "text_size": "normal",
            "background_style": "none",
            "text_color": "grey",
            "bold": True,
            "lines": 1,
        },
        "columns": [
            {
                "name": "code",
                "display_name": "代码",
                "data_type": "text",
                "horizontal_align": "left",
                "vertical_align": "center",
                "width": "84px",
            },
            {
                "name": "name",
                "display_name": "名称",
                "data_type": "text",
                "horizontal_align": "left",
                "vertical_align": "center",
                "width": "132px",
            },
            {
                "name": "price",
                "display_name": "市价",
                "data_type": "text",
                "horizontal_align": "left",
                "vertical_align": "center",
                "width": "96px",
            },
            {
                "name": "iopv",
                "display_name": "IOPV",
                "data_type": "text",
                "horizontal_align": "left",
                "vertical_align": "center",
                "width": "96px",
            },
            {
                "name": "premium_pct",
                "display_name": "折溢价",
                "data_type": "text",
                "horizontal_align": "left",
                "vertical_align": "center",
                "width": "96px",
            },
        ],
        "rows": build_table_rows(df, max_rows=max_rows),
    }


def build_feishu_card(
    title: str,
    push_time_cn: str,
    sample_count: int,
    premium_top: pd.DataFrame,
    discount_top: pd.DataFrame,
) -> dict:
    max_premium = premium_top["premium_pct"].iloc[0] if not premium_top.empty else float("nan")
    max_discount = discount_top["premium_pct"].iloc[0] if not discount_top.empty else float("nan")
    max_premium_text = f"{max_premium:+.2f}%" if pd.notna(max_premium) else "N/A"
    max_discount_text = f"{max_discount:+.2f}%" if pd.notna(max_discount) else "N/A"
    premium_leader = (
        f"{premium_top['code'].iloc[0]} {truncate_name(premium_top['name'].iloc[0])} {max_premium_text}"
        if not premium_top.empty
        else "暂无数据"
    )
    discount_leader = (
        f"{discount_top['code'].iloc[0]} {truncate_name(discount_top['name'].iloc[0])} {max_discount_text}"
        if not discount_top.empty
        else "暂无数据"
    )

    return {
        "msg_type": "interactive",
        "card": {
            "schema": "2.0",
            "config": {
                "wide_screen_mode": True,
                "width_mode": "fill",
                "enable_forward": True,
                "update_multi": True,
            },
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "subtitle": {"tag": "plain_text", "content": "工作日 14:20 自动推送"},
                "template": "blue",
            },
            "body": {
                "elements": [
                    {
                        "tag": "markdown",
                        "content": (
                            "**关键指标**  \n"
                            f"时间：{push_time_cn}（北京时间） ｜ "
                            f"样本数：{sample_count} ｜ "
                            f"最大溢价：{max_premium_text} ｜ "
                            f"最大折价：{max_discount_text}"
                        ),
                        "text_align": "left",
                    },
                    {"tag": "hr"},
                    {
                        "tag": "column_set",
                        "flex_mode": "bisect",
                        "horizontal_spacing": "medium",
                        "columns": [
                            {
                                "tag": "column",
                                "width": "weighted",
                                "weight": 1,
                                "vertical_align": "top",
                                "elements": [
                                    {
                                        "tag": "div",
                                        "text": {
                                            "tag": "plain_text",
                                            "content": "溢价 Top10 概览",
                                            "text_size": "normal",
                                            "text_color": "default",
                                        },
                                    },
                                    {
                                        "tag": "markdown",
                                        "content": f"榜首：`{premium_leader}`",
                                        "text_align": "left",
                                    },
                                ],
                            },
                            {
                                "tag": "column",
                                "width": "weighted",
                                "weight": 1,
                                "vertical_align": "top",
                                "elements": [
                                    {
                                        "tag": "div",
                                        "text": {
                                            "tag": "plain_text",
                                            "content": "折价 Top10 概览",
                                            "text_size": "normal",
                                            "text_color": "default",
                                        },
                                    },
                                    {
                                        "tag": "markdown",
                                        "content": f"榜首：`{discount_leader}`",
                                        "text_align": "left",
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "plain_text",
                            "content": "溢价 Top10",
                            "text_size": "normal",
                            "text_color": "default",
                        },
                    },
                    build_table_component(premium_top, max_rows=10),
                    {
                        "tag": "div",
                        "text": {
                            "tag": "plain_text",
                            "content": "折价 Top10",
                            "text_size": "normal",
                            "text_color": "default",
                        },
                    },
                    build_table_component(discount_top, max_rows=10),
                    {"tag": "hr"},
                    {
                        "tag": "markdown",
                        "content": (
                            "口径：`premium_pct=(price/IOPV-1)*100`  \n"
                            "数据：AKShare/东方财富 LOF 场内实时行情"
                        ),
                        "text_align": "left",
                    },
                ]
            },
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

    payload = build_feishu_card(
        title="LOF 折溢价 Top10",
        push_time_cn=push_time_cn,
        sample_count=len(df),
        premium_top=premium_top,
        discount_top=discount_top,
    )
    print("[INFO] 卡片构建完成")
    feishu_post(webhook, payload)


if __name__ == "__main__":
    main()

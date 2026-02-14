import os
import hashlib
from io import BytesIO, StringIO
import psycopg
import numpy as np
import pandas as pd
from dateutil import parser

import boto3
from snowflake.connector.pandas_tools import write_pandas

# 🔁 PROVIDED CONNECTIONS (ALREADY DEFINED ELSEWHERE)
from connections.snowflake_conn import get_snowflake_conn
from connections.postgre_conn import get_postgre_conn


# ================= CLIENTS =================
MACRO_BUCKET = "pushparag-equity-bucket"
MACRO_KEY = "historical-equity/macro_data.csv"

s3 = boto3.client("s3")


# ================= RISK CONFIGS =================
HAIRCUT = {
    "Cash": 0.00, "Treasury": 0.02, "CorporateBond": 0.10,
    "Equity": 0.15, "CommodityETF": 0.12
}
INITIAL_MARGIN = {
    "FX": 0.05, "Commodity": 0.08, "Bond": 0.04, "Equity": 0.12
}
COLLATERAL_MENU = {
    "FX": [("Cash", 0.70), ("Treasury", 0.25), ("CorporateBond", 0.05)],
    "Commodity": [("Treasury", 0.60), ("Cash", 0.25), ("CommodityETF", 0.15)],
    "Bond": [("Treasury", 0.80), ("Cash", 0.15), ("CorporateBond", 0.05)],
    "Equity": [("Cash", 0.50), ("Treasury", 0.30), ("Equity", 0.20)],
}
AGREEMENT_CHOICES = [
    ("CSA", "US", 0.45), ("CSA", "UK", 0.25),
    ("CSA", "EU", 0.20), ("GMRA", "UK", 0.05),
    ("GMSLA", "US", 0.05),
]


# ================= UTIL FUNCTIONS =================
def load_latest(table):
    query = f'''
        SELECT *
        FROM "{table}"
        WHERE "date" > (SELECT MAX("date") FROM "COLLATERAL")
    '''
    with get_snowflake_conn() as ctx:
        with ctx.cursor() as cs:
            cs.execute(query)
            return cs.fetch_pandas_all()


def load_macro():
    obj = s3.get_object(Bucket=MACRO_BUCKET, Key=MACRO_KEY)
    df = pd.read_csv(BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df[df["date"].notna()]


def fast_counterparty(series):
    arr = np.array(["CP_A", "CP_B", "CP_C", "CP_D", "CP_E"])
    idx = series.astype(str).apply(
        lambda x: int(hashlib.sha256(x.encode()).hexdigest(), 16) % 5
    )
    return arr[idx]


def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def generate_trade_id(row):
    parts = [row["collateral_type"], row["ticker"], row["date"].strftime("%Y%m%d")]

    if pd.notna(row.get("commodity_sym")):
        parts.append(str(row["commodity_sym"]).replace("=", "_"))

    if pd.notna(row.get("asset_manager")):
        parts.append(str(row["asset_manager"]).split()[0])

    return "_".join(parts)


def build_detail(df, asset_class, sym, exp):
    if df.empty:
        return pd.DataFrame()

    required_cols = ["date", sym, exp, "sector", "industry", "asset_manager"]
    if asset_class == "Commodity":
        required_cols.append("commodity")

    df = ensure_cols(df, required_cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]

    out = pd.DataFrame()
    out["date"] = df["date"]
    out["asset_class"] = asset_class
    out["ticker"] = df[sym].astype(str)
    out["sector"] = df.get("sector", "Unknown")
    out["industry"] = df.get("industry", "Unknown")
    out["counterparty"] = fast_counterparty(df[sym])
    out["exposure_before_collateral"] = df[exp].abs().astype(float)

    out["asset_manager"] = df["asset_manager"].fillna(np.nan)
    out["commodity_sym"] = df["commodity"] if asset_class == "Commodity" else np.nan

    collateral_map = {
        "FX": "FXFwd",
        "Commodity": "Futures",
        "Bond": "IRS",
        "Equity": "EqFwd"
    }
    out["collateral_type"] = collateral_map[asset_class]
    out["trade_id"] = out.apply(generate_trade_id, axis=1)

    ac_pairs = [(a, j) for (a, j, p) in AGREEMENT_CHOICES]
    probs = [p for (*_, p) in AGREEMENT_CHOICES]
    idx = np.random.choice(len(ac_pairs), len(out), p=np.array(probs) / sum(probs))
    a, j = zip(*[ac_pairs[i] for i in idx])
    out["agreement_type"], out["jurisdiction"] = a, j

    collateral_types, collateral_probs = zip(*COLLATERAL_MENU[asset_class])
    out["collateral_type"] = np.random.choice(
        collateral_types, len(out),
        p=np.array(collateral_probs) / sum(collateral_probs)
    )

    out["haircut"] = out["collateral_type"].map(HAIRCUT)
    out["initial_margin_rate"] = INITIAL_MARGIN[asset_class]

    out["required_collateral"] = out["exposure_before_collateral"] * out["initial_margin_rate"]
    out["collateral_value"] = out["required_collateral"] * 0.9
    out["effective_collateral"] = out["collateral_value"] * (1 - out["haircut"])
    out["net_exposure"] = (out["exposure_before_collateral"] - out["effective_collateral"]).clip(0)
    out["collateral_ratio"] = (
        out["effective_collateral"] / out["exposure_before_collateral"]
    ).fillna(0)

    out["margin_call_flag"] = (out["net_exposure"] > 0).astype(int)
    out["margin_call_amount"] = out["net_exposure"]
    out["reuse_flag"] = np.random.binomial(1, 0.35, len(out))
    out["reused_value"] = out["collateral_value"] * out["reuse_flag"] * 0.8
    out["funding_cost"] = out["collateral_value"] * 0.01 * (
        out["haircut"] + out["initial_margin_rate"]
    )
    out["liquidity_score"] = 1 - (out["haircut"] + out["initial_margin_rate"])

    out["collateral_id"] = out["trade_id"].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )

    return out.drop(columns=["commodity_sym", "asset_manager", "collateral_type"], errors="ignore")


# ================= AIRFLOW ENTRYPOINT =================
def run_collateral_pipeline():
    np.random.seed(42)

    fx_df = load_latest("FX")
    eq_df = load_latest("EQUITY")
    cm_df = load_latest("COMMODITY")
    bd_df = load_latest("BONDS")

    fx = build_detail(fx_df, "FX", "ticker", "exposure_amount")
    eq = build_detail(eq_df, "Equity", "ticker", "mtm_value")
    cm = build_detail(cm_df, "Commodity", "ticker", "exposure_amount")
    bd = build_detail(bd_df, "Bond", "ticker", "bond_price")

    df_detail = pd.concat([fx, eq, cm, bd], ignore_index=True)
    if df_detail.empty:
        return "NO_NEW_ROWS"

    df_model = df_detail.groupby(
        ["date", "asset_class", "ticker", "counterparty"], as_index=False
    ).agg({
        "exposure_before_collateral": "sum",
        "required_collateral": "sum",
        "collateral_value": "sum",
        "effective_collateral": "sum",
        "net_exposure": "sum",
        "margin_call_amount": "sum",
        "funding_cost": "sum",
        "liquidity_score": "mean",
    })

    df_model["collateral_ratio"] = (
        df_model["effective_collateral"] / df_model["exposure_before_collateral"]
    ).fillna(0)
    df_model["margin_call_flag"] = (df_model["net_exposure"] > 0).astype(int)

    macro = load_macro()
    df_detail["month_year"] = df_detail["date"].dt.to_period("M").astype(str)
    macro["month_year"] = macro["date"].dt.to_period("M").astype(str)

    df_detail = df_detail.merge(
        macro.drop(columns=["date"]),
        on="month_year",
        how="inner"
    ).drop(columns=["month_year"])

    df_detail["date"] = pd.to_datetime(df_detail["date"]).dt.date
    df_model["date"] = pd.to_datetime(df_model["date"]).dt.date

    with get_snowflake_conn() as ctx:
        write_pandas(ctx, df_model, "COLLATERAL_MODEL", chunk_size=100000)
        write_pandas(ctx, df_detail, "COLLATERAL", chunk_size=100000)

    # ================= POSTGRES DETAIL =================
    BATCH_SIZE = 100_000

    with get_postgre_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            quoted_cols = [f'"{c}"' for c in df_detail.columns]
            copy_sql = (
                f"COPY public.collateral_data ({','.join(quoted_cols)}) "
                f"FROM STDIN WITH CSV"
            )

            for start in range(0, len(df_detail), BATCH_SIZE):
                end = start + BATCH_SIZE
                chunk = df_detail.iloc[start:end]

                buf = StringIO()
                chunk.to_csv(buf, index=False, header=False)
                buf.seek(0)

                with cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())
        pg_conn.commit()

    # ================= POSTGRES MODEL =================
    with get_postgre_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            quoted_cols = [f'"{c}"' for c in df_model.columns]
            copy_sql = (
                f"COPY public.collateral_model_data ({','.join(quoted_cols)}) "
                f"FROM STDIN WITH CSV"
            )

            for start in range(0, len(df_model), BATCH_SIZE):
                end = start + BATCH_SIZE
                chunk = df_model.iloc[start:end]

                buf = StringIO()
                chunk.to_csv(buf, index=False, header=False)
                buf.seek(0)

                with cur.copy(copy_sql) as copy:
                    copy.write(buf.getvalue())
        pg_conn.commit()

    return "SUCCESS"

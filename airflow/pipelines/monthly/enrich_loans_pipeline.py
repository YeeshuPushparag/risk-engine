import time
import pandas as pd
import boto3
from io import BytesIO
from connections.snowflake_conn import get_snowflake_conn

S3_BUCKET = "monthly-loans"
LOAN_BASE_KEY = "loan_synthetic_base.parquet"
OUTPUT_KEY = "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"

s3 = boto3.client("s3")


# =========================
# HELPERS
# =========================

def fast_parse_dates(series, chunk_size=250_000):
    """Chunked date parsing to avoid heartbeat timeout."""
    result = []

    for i in range(0, len(series), chunk_size):
        chunk = series.iloc[i:i+chunk_size]
        parsed = pd.to_datetime(chunk, errors="coerce", dayfirst=True)
        mask = parsed.isna()
        if mask.any():
            parsed[mask] = pd.to_datetime(chunk[mask], format="%d-%m-%Y", errors="coerce")
        result.append(parsed)
        time.sleep(0.01)  # yield to Celery heartbeat

    return pd.concat(result, ignore_index=True)


def load_snowflake_table(name, last_month):
    where_clause = ""
    params = ()

    if last_month is not None:
        param_date = f"{last_month.year}-{last_month.month:02d}-01"
        where_clause = """
            WHERE DATE_TRUNC('MONTH', TO_DATE("date", 'YYYY-MM-DD'))
                  > DATE_TRUNC('MONTH', TO_DATE(%s, 'YYYY-MM-DD'))
        """
        params = (param_date,)

    query = f'SELECT * FROM "{name}" {where_clause}'

    with get_snowflake_conn() as ctx:
        cur = ctx.cursor()
        cur.execute(query, params)
        df = cur.fetch_pandas_all()
    return df


def read_s3_parquet(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return None


def write_s3_parquet(df, key):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)


# =========================
# PIPELINE
# =========================

def run_enrich_loans_pipeline():
    print("Loading base loans from S3...")
    loans = read_s3_parquet(LOAN_BASE_KEY)
    if loans is None:
        raise RuntimeError("loan_synthetic_base.parquet not found in S3")

    # Chunked date parsing
    loans["issue_date"] = fast_parse_dates(loans["issue_date"])
    loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])

    prev = read_s3_parquet(OUTPUT_KEY)
    last_processed_month = None
    if prev is not None:
        prev["date"] = fast_parse_dates(prev["date"])
        last_processed_month = prev["date"].dt.to_period("M").max()
        print(f"Last processed month: {last_processed_month}")

    print("Loading incremental Snowflake data...")
    fx = load_snowflake_table("FX", last_processed_month)
    bonds = load_snowflake_table("BONDS", last_processed_month)
    commod = load_snowflake_table("COMMODITY", last_processed_month)
    deriv = load_snowflake_table("DERIVATIVES", last_processed_month)
    collateral = load_snowflake_table("COLLATERAL", last_processed_month)

    # Chunked date parsing for Snowflake data
    for df in [fx, bonds, commod, deriv, collateral]:
        df["date"] = fast_parse_dates(df["date"])
        df["month_year"] = df["date"].dt.to_period("M")

    if last_processed_month is not None:
        fx = fx[fx["month_year"] > last_processed_month]
        bonds = bonds[bonds["month_year"] > last_processed_month]
        commod = commod[commod["month_year"] > last_processed_month]
        deriv = deriv[deriv["month_year"] > last_processed_month]
        collateral = collateral[collateral["month_year"] > last_processed_month]

    if all(df.empty for df in [fx, bonds, commod, deriv, collateral]):
        print("No new data to process.")
        return "NO_NEW_DATA"

    print("Aggregating monthly data...")
    fx_month = fx.groupby(["ticker", "month_year"], as_index=False).agg(
        fx_rate=("fx_rate", "mean"),
        fx_volatility=("fx_volatility", "mean"),
        carry_daily=("carry_daily", "mean")
    )

    bonds_month = bonds.groupby(["ticker", "month_year"], as_index=False).agg(
        credit_spread=("credit_spread", "mean"),
        yield_to_maturity=("yield_to_maturity", "mean"),
        credit_rating=("credit_rating", lambda x: x.mode().iloc[0] if not x.mode().empty else None)
    )

    commod_month = commod.groupby(["sector", "month_year"], as_index=False).agg(
        close=("close", "mean"),
        vol_20d=("vol_20d", "mean")
    )

    deriv_month = deriv.groupby(["ticker", "month_year"], as_index=False).agg(
        notional=("notional", "mean"),
        exposure_before_collateral=("exposure_before_collateral", "mean"),
        collateral_value=("collateral_value", "mean"),
        net_exposure=("net_exposure", "mean"),
        collateral_ratio=("collateral_ratio", "mean"),
        margin_call_flag=("margin_call_flag", lambda x: int((x == 1).any())),
        pnl=("pnl", "mean")
    )

    collat_month = collateral.groupby(["ticker", "month_year"], as_index=False).agg(
        counterparty=("counterparty", lambda x: "|".join(sorted(set(map(str, x)))[:3])),
        funding_cost=("funding_cost", "mean"),
        liquidity_score=("liquidity_score", "mean"),
        margin_call_amount=("margin_call_amount", "mean")
    )

    # =========================
    # Safe loans × month expansion
    # =========================
    month_years = pd.concat([
        fx_month["month_year"], bonds_month["month_year"], commod_month["month_year"],
        deriv_month["month_year"], collat_month["month_year"]
    ]).unique()

    month_df = pd.DataFrame({"month_year": month_years})
    month_df["month_start"] = month_df["month_year"].dt.to_timestamp()

    # Streamed expansion to avoid cartesian explosion
    expanded_rows = []
    for i, loan in loans.iterrows():
        valid_months = month_df[
            (month_df["month_start"] >= loan["issue_date"]) &
            (month_df["month_start"] <= loan["maturity_date"])
        ].copy()

        if valid_months.empty:
            continue

        for col in loans.columns:
            valid_months[col] = loan[col]

        expanded_rows.append(valid_months)

        if i % 500 == 0:
            time.sleep(0.01)  # yield to heartbeat

    loans_expanded = pd.concat(expanded_rows, ignore_index=True)
    loans_expanded["date"] = loans_expanded["month_start"].dt.date

    print("Merging enrichment data...")
    merged = loans_expanded.merge(fx_month, on=["ticker", "month_year"], how="inner") \
        .merge(bonds_month, on=["ticker", "month_year"], how="left") \
        .merge(commod_month, on=["sector", "month_year"], how="left") \
        .merge(deriv_month, on=["ticker", "month_year"], how="left") \
        .merge(collat_month, on=["ticker", "month_year"], how="left")

    merged.drop(columns=["month_year", "month_start"], inplace=True)

    if prev is not None:
        merged = pd.concat([prev, merged], ignore_index=True)
        merged.drop_duplicates(subset=["loan_id", "date"], inplace=True)

    write_s3_parquet(merged, OUTPUT_KEY)
    print("Pipeline completed successfully.")

    return "SUCCESS"

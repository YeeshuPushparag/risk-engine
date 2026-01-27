import pandas as pd
import boto3
import gc
from io import BytesIO
from datetime import datetime
from connections.snowflake_conn import get_snowflake_conn

S3_BUCKET = "monthly-loans"
LOAN_BASE_KEY = "loan_synthetic_base.parquet"
OUTPUT_KEY = "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet"

s3 = boto3.client("s3")

# =========================
# HELPERS
# =========================

def fast_parse_dates(series):
    """Optimized date parsing with explicit format detection."""
    # Try ISO format first (YYYY-MM-DD)
    result = pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')
    
    # Fill remaining NaNs with DD-MM-YYYY format
    mask = result.isna()
    if mask.any():
        result[mask] = pd.to_datetime(
            series[mask], 
            format='%d-%m-%Y', 
            errors='coerce'
        )
    
    return result

def read_s3_parquet(key, columns=None):
    """Read specific columns from S3 to save memory."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()), columns=columns)
    except s3.exceptions.NoSuchKey:
        return None

def write_s3_parquet(df, key):
    """Write with compression to save space."""
    buf = BytesIO()
    df.to_parquet(buf, index=False, compression='snappy')
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)

def load_snowflake_table(name, last_month):
    """Optimized Snowflake query with direct pandas read."""
    print(f"Loading {name} from Snowflake...")
    
    where_clause = ""
    params = {}
    
    if last_month is not None:
        # Get the first day of the month after last_processed_month
        next_month = last_month + 1
        param_date = f"{next_month.year}-{next_month.month:02d}-01"
        
        where_clause = """
            WHERE DATE >= DATE(%(param_date)s)
        """
        params = {'param_date': param_date}
    
    query = f"""
        SELECT * 
        FROM "{name}"
        {where_clause}
        ORDER BY "date"
    """
    
    with get_snowflake_conn() as conn:
        # Use pandas read_sql directly for better performance
        df = pd.read_sql_query(query, conn, params=params)
    
    print(f"{name} loaded: {len(df):,} rows")
    return df

def aggregate_to_monthly(df, group_cols, agg_dict):
    """Efficient monthly aggregation."""
    if df.empty:
        return pd.DataFrame()
    
    # Convert to month period first
    df = df.copy()
    df['month_year'] = df['date'].dt.to_period('M')
    
    # Group and aggregate
    result = df.groupby(group_cols + ['month_year'], as_index=False).agg(agg_dict)
    
    return result

# =========================
# MAIN PIPELINE
# =========================

def run_enrich_loans_pipeline():
    """Main pipeline function with performance optimizations."""
    print("Starting loan enrichment pipeline...")
    
    # 1. Load only necessary columns from base loans
    print("Loading base loans...")
    required_columns = ['loan_id', 'ticker', 'sector', 'issue_date', 'maturity_date']
    loans = read_s3_parquet(LOAN_BASE_KEY, columns=required_columns)
    
    if loans is None:
        raise RuntimeError("loan_synthetic_base.parquet not found in S3")
    
    # Parse dates (this should be fast)
    loans["issue_date"] = fast_parse_dates(loans["issue_date"])
    loans["maturity_date"] = fast_parse_dates(loans["maturity_date"])
    
    # 2. Determine last processed month (read only metadata if possible)
    print("Checking previous processed data...")
    prev = read_s3_parquet(OUTPUT_KEY, columns=['loan_id', 'date'])
    last_processed_month = None
    
    if prev is not None and not prev.empty:
        prev["date"] = fast_parse_dates(prev["date"])
        last_processed_month = prev["date"].dt.to_period("M").max()
        print(f"Last processed month: {last_processed_month}")
    
    # 3. Load Snowflake data incrementally
    print("Loading market data from Snowflake...")
    
    # Load only what we need
    tables = {
        'FX': ['ticker', 'date', 'fx_rate', 'fx_volatility', 'carry_daily'],
        'BONDS': ['ticker', 'date', 'credit_spread', 'yield_to_maturity', 'credit_rating'],
        'COMMODITY': ['sector', 'date', 'close', 'vol_20d'],
        'DERIVATIVES': ['ticker', 'date', 'notional', 'exposure_before_collateral', 
                       'collateral_value', 'net_exposure', 'collateral_ratio', 
                       'margin_call_flag', 'pnl'],
        'COLLATERAL': ['ticker', 'date', 'counterparty', 'funding_cost', 
                      'liquidity_score', 'margin_call_amount']
    }
    
    # You could modify load_snowflake_table to accept column list
    # For now, load all and parse dates
    fx = load_snowflake_table("FX", last_processed_month)
    bonds = load_snowflake_table("BONDS", last_processed_month)
    commod = load_snowflake_table("COMMODITY", last_processed_month)
    deriv = load_snowflake_table("DERIVATIVES", last_processed_month)
    collateral = load_snowflake_table("COLLATERAL", last_processed_month)
    
    # Parse dates in Snowflake data
    for df in [fx, bonds, commod, deriv, collateral]:
        if not df.empty and 'date' in df.columns:
            df['date'] = fast_parse_dates(df['date'])
    
    # Free up memory by dropping old data if incremental
    if last_processed_month is not None:
        for df_name, df in [('fx', fx), ('bonds', bonds), ('commod', commod), 
                           ('deriv', deriv), ('collateral', collateral)]:
            if not df.empty:
                df['month_year'] = df['date'].dt.to_period('M')
                df = df[df['month_year'] > last_processed_month]
                if 'month_year' in df.columns:
                    df = df.drop(columns=['month_year'])
    
    # Check if there's new data
    new_data_frames = [fx, bonds, commod, deriv, collateral]
    if all(df.empty for df in new_data_frames):
        print("No new data to process.")
        return "NO_NEW_DATA"
    
    # 4. Aggregate to monthly level (reduces data volume significantly)
    print("Aggregating to monthly level...")
    
    fx_month = aggregate_to_monthly(
        fx, 
        ['ticker'], 
        {'fx_rate': 'mean', 'fx_volatility': 'mean', 'carry_daily': 'mean'}
    )
    
    bonds_month = aggregate_to_monthly(
        bonds,
        ['ticker'],
        {
            'credit_spread': 'mean',
            'yield_to_maturity': 'mean',
            'credit_rating': lambda x: x.mode().iloc[0] if not x.mode().empty else None
        }
    )
    
    commod_month = aggregate_to_monthly(
        commod,
        ['sector'],
        {'close': 'mean', 'vol_20d': 'mean'}
    )
    
    deriv_month = aggregate_to_monthly(
        deriv,
        ['ticker'],
        {
            'notional': 'mean',
            'exposure_before_collateral': 'mean',
            'collateral_value': 'mean',
            'net_exposure': 'mean',
            'collateral_ratio': 'mean',
            'margin_call_flag': lambda x: 1 if (x == 1).any() else 0,
            'pnl': 'mean'
        }
    )
    
    collat_month = aggregate_to_monthly(
        collateral,
        ['ticker'],
        {
            'counterparty': lambda x: '|'.join(sorted(set(map(str, x)))[:3]),
            'funding_cost': 'mean',
            'liquidity_score': 'mean',
            'margin_call_amount': 'mean'
        }
    )
    
    # Free memory from daily dataframes
    del fx, bonds, commod, deriv, collateral
    gc.collect()
    
    # 5. Get all unique months from all sources
    all_months = set()
    for df in [fx_month, bonds_month, commod_month, deriv_month, collat_month]:
        if not df.empty and 'month_year' in df.columns:
            all_months.update(df['month_year'].unique())
    
    if not all_months:
        print("No months to process.")
        return "NO_NEW_DATA"
    
    # Convert to DataFrame and sort
    months_df = pd.DataFrame({
        'month_year': sorted(all_months),
        'month_start': [period.to_timestamp() for period in sorted(all_months)]
    })
    
    # 6. Expand loans by months (optimized)
    print("Expanding loans by months...")
    
    # Filter loans that are active during our months range
    min_month = months_df['month_start'].min()
    max_month = months_df['month_start'].max()
    
    active_loans = loans[
        (loans['maturity_date'] >= min_month) & 
        (loans['issue_date'] <= max_month)
    ].copy()
    
    if active_loans.empty:
        print("No active loans for the given months.")
        return "NO_NEW_DATA"
    
    # Create expanded dataframe
    expanded_rows = []
    chunk_size = 1000  # Process in chunks
    
    for i in range(0, len(active_loans), chunk_size):
        chunk = active_loans.iloc[i:i+chunk_size]
        
        for _, loan in chunk.iterrows():
            # Find months where loan is active
            mask = (months_df['month_start'] >= loan['issue_date']) & \
                   (months_df['month_start'] <= loan['maturity_date'])
            
            active_months = months_df[mask]
            if not active_months.empty:
                # Create row for each active month
                for _, month_row in active_months.iterrows():
                    new_row = loan.to_dict()
                    new_row['month_year'] = month_row['month_year']
                    new_row['date'] = month_row['month_start'].date()
                    expanded_rows.append(new_row)
    
    loans_expanded = pd.DataFrame(expanded_rows)
    
    # 7. Merge with market data
    print("Merging with market data...")
    
    # Merge step by step
    merged = loans_expanded
    
    # Merge FX
    if not fx_month.empty:
        merged = merged.merge(
            fx_month, 
            on=['ticker', 'month_year'], 
            how='left'
        )
    
    # Merge Bonds
    if not bonds_month.empty:
        merged = merged.merge(
            bonds_month,
            on=['ticker', 'month_year'],
            how='left'
        )
    
    # Merge Commodity
    if not commod_month.empty:
        merged = merged.merge(
            commod_month,
            on=['sector', 'month_year'],
            how='left'
        )
    
    # Merge Derivatives
    if not deriv_month.empty:
        merged = merged.merge(
            deriv_month,
            on=['ticker', 'month_year'],
            how='left'
        )
    
    # Merge Collateral
    if not collat_month.empty:
        merged = merged.merge(
            collat_month,
            on=['ticker', 'month_year'],
            how='left'
        )
    
    # Drop month_year column
    if 'month_year' in merged.columns:
        merged = merged.drop(columns=['month_year'])
    
    print(f"Created {len(merged):,} new enriched rows")
    
    # 8. Append to previous data
    if prev is not None:
        # Load full previous data if needed
        full_prev = read_s3_parquet(OUTPUT_KEY)
        if full_prev is not None:
            full_prev["date"] = fast_parse_dates(full_prev["date"])
            merged = pd.concat([full_prev, merged], ignore_index=True)
            merged.drop_duplicates(subset=["loan_id", "date"], inplace=True)
    
    # 9. Save to S3
    print("Saving enriched data to S3...")
    write_s3_parquet(merged, OUTPUT_KEY)
    
    print(f"Pipeline completed successfully. Final dataset: {len(merged):,} rows")
    return "SUCCESS"
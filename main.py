from test_framework.utilities import sauth
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import pandas as pd
import redshift_connector
import json


def get_redshift_connection():
    """Get Redshift connection using credentials from AWS Secrets Manager."""
    db_secrets = json.loads(sauth.retrieve_aws_secret_by_db('DataQa/circleci_db_credentials'))
    conn = redshift_connector.connect(
        host=db_secrets['redshift_host'],
        port=int(db_secrets['redshift_port']),
        database=db_secrets['redshift_database'],
        user=db_secrets['redshift_user'],
        password=db_secrets['redshift_password']
    )
    return conn


def run_linear_match_rate(data_dump_date, ctx):
    """Run linear match rate query and write to Snowflake."""
    query = f"""
    WITH prod_counts AS (
        SELECT data_dump_date, COUNT(*) AS prod_row_count
        FROM gold_lake_prod.impressions_prod_output_roku_linear_optimization_feed
        WHERE data_dump_date = '{data_dump_date}'
        GROUP BY 1
    ),
    person_counts AS (
        SELECT data_dump_date, COUNT(*) AS person_row_count
        FROM audience_lake.person_processed_ad_content_event_vizio
        WHERE data_dump_date = '{data_dump_date}'
        GROUP BY 1
    )
    SELECT
        p.data_dump_date,
        p.prod_row_count,
        pc.person_row_count,
        CASE
            WHEN p.prod_row_count = 0 AND pc.person_row_count = 0 THEN 1.0
            WHEN pc.person_row_count = 0 THEN NULL
            ELSE (p.prod_row_count * 1.0 / pc.person_row_count)
        END AS match_rate
    FROM prod_counts p
    INNER JOIN person_counts pc ON p.data_dump_date = pc.data_dump_date;
    """

    conn = get_redshift_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print(f"[Linear] No data for {data_dump_date}")
        return

    df['RUN_DATE'] = datetime.now().strftime('%Y-%m-%d')
    df['RUN_TIME'] = datetime.now().strftime('%H:%M:%S')
    df.columns = map(str.upper, df.columns)

    try:
        write_pandas(ctx, df, 'ROKU_LINEAR_MATCH_RATE_LOG')
    except Exception:
        write_pandas(ctx, df, 'ROKU_LINEAR_MATCH_RATE_LOG', auto_create_table=True)

    print(f"[Linear] Wrote {df.shape[0]} rows to ROKU_LINEAR_MATCH_RATE_LOG")


def run_digital_match_rate(data_dump_date, ctx):
    """Run digital match rate query and write to Snowflake (4 tables)."""
    digital_query = f"""
    WITH pixel AS (
        SELECT aud.brand_id, aud.client AS brand_name, o.data_dump_date AS data_date,
               CASE WHEN p.mapped_name ILIKE '%roku%' THEN 'Roku' ELSE 'Non-Roku' END AS publisher,
               COUNT(*) AS total_imp
        FROM audience_lake_v8.processed_event_ott_pixel_core24 AS o
        LEFT JOIN ispot_db.pixel_audits AS aud ON o.site_id = aud.site_id
        LEFT JOIN data_science_canon.publisher_mapping_exclude_v2 p ON p.pixel_publisher_id = o.mapped_publisher_id
        WHERE o.data_dump_date = '{data_dump_date}'
          AND o.ott_source_id NOT IN (18, 30, 14, 90)
          AND aud.integration_partner = 'no'
          AND exclude IS NULL
        GROUP BY 1, 2, 3, 4
    ),
    amazon AS (
        SELECT aud.brand_id, aud.client AS brand_name, o.data_dump_date AS data_date,
               CASE WHEN p.mapped_name ILIKE '%roku%' THEN 'Roku' ELSE 'Non-Roku' END AS publisher,
               COUNT(*) AS total_imp
        FROM audience_lake_v8.processed_event_ott_s2s_core24 AS o
        LEFT JOIN ispot_db.pixel_audits AS aud ON o.site_id = aud.site_id
        LEFT JOIN data_science_canon.publisher_mapping_exclude_v2 p ON p.pixel_publisher_id = o.mapped_publisher_id
        WHERE o.data_dump_date = '{data_dump_date}'
          AND o.ott_source_id NOT IN (18, 30, 14, 90)
          AND aud.integration_partner = 'no'
          AND exclude IS NULL
        GROUP BY 1, 2, 3, 4
    ),
    total AS (
        SELECT brand_id, brand_name, data_date, publisher,
               SUM(total_imp) AS total_rows
        FROM (
            SELECT * FROM pixel
            UNION ALL
            SELECT * FROM amazon
        ) t
        GROUP BY 1, 2, 3, 4
    ),
    matched AS (
        SELECT data_dump_date, publisher, brand, brand_id, COUNT(*) AS matched_rows
        FROM gold_lake_prod.impressions_prod_output_roku_digital_optimization_feed
        WHERE data_dump_date = '{data_dump_date}'
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        t.data_date,
        t.brand_name,
        m.brand,
        t.brand_id,
        m.brand_id AS matched_brand_id,
        t.publisher,
        m.publisher AS matched_publisher,
        SUM(matched_rows) AS rows_matched,
        SUM(total_rows) AS rows_total,
        (SUM(matched_rows) / SUM(total_rows)::DOUBLE PRECISION) * 100 AS match_rate_pct
    FROM total t
    JOIN matched m
        ON t.brand_id = m.brand_id
        AND t.publisher = m.publisher
        AND t.data_date = m.data_dump_date
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    """

    conn = get_redshift_connection()
    df_digital = pd.read_sql(digital_query, conn)
    conn.close()

    if df_digital.empty:
        print(f"[Digital] No data for {data_dump_date}")
        return

    run_date = datetime.now().strftime('%Y-%m-%d')
    run_time = datetime.now().strftime('%H:%M:%S')

    # --- Table 1: General (by date) ---
    df_general = df_digital.groupby('data_date', as_index=False).agg(
        rows_matched=('rows_matched', 'sum'),
        rows_total=('rows_total', 'sum')
    )
    df_general['match_rate_pct'] = (df_general['rows_matched'] / df_general['rows_total']) * 100
    df_general['RUN_DATE'] = run_date
    df_general['RUN_TIME'] = run_time
    df_general.columns = map(str.upper, df_general.columns)
    _write_to_snowflake(ctx, df_general, 'ROKU_DIGITAL_MATCH_RATE_LOG')

    # --- Table 2: By Publisher ---
    df_by_pub = df_digital.groupby(['data_date', 'publisher'], as_index=False).agg(
        rows_matched=('rows_matched', 'sum'),
        rows_total=('rows_total', 'sum')
    )
    df_by_pub['match_rate_pct'] = (df_by_pub['rows_matched'] / df_by_pub['rows_total']) * 100
    df_by_pub['RUN_DATE'] = run_date
    df_by_pub['RUN_TIME'] = run_time
    df_by_pub.columns = map(str.upper, df_by_pub.columns)
    _write_to_snowflake(ctx, df_by_pub, 'ROKU_DIGITAL_BY_PUB_MATCH_RATE_LOG')

    # --- Table 3: By Brand ---
    df_by_brand = df_digital.groupby(['data_date', 'brand_id', 'brand_name'], as_index=False).agg(
        rows_matched=('rows_matched', 'sum'),
        rows_total=('rows_total', 'sum')
    )
    df_by_brand['match_rate_pct'] = (df_by_brand['rows_matched'] / df_by_brand['rows_total']) * 100
    df_by_brand['RUN_DATE'] = run_date
    df_by_brand['RUN_TIME'] = run_time
    df_by_brand.columns = map(str.upper, df_by_brand.columns)
    _write_to_snowflake(ctx, df_by_brand, 'ROKU_DIGITAL_BY_BRAND_MATCH_RATE_LOG')

    # --- Table 4: By Publisher + Brand ---
    df_by_all = df_digital.groupby(['data_date', 'brand_id', 'brand_name', 'publisher'], as_index=False).agg(
        rows_matched=('rows_matched', 'sum'),
        rows_total=('rows_total', 'sum')
    )
    df_by_all['match_rate_pct'] = (df_by_all['rows_matched'] / df_by_all['rows_total']) * 100
    df_by_all['RUN_DATE'] = run_date
    df_by_all['RUN_TIME'] = run_time
    df_by_all.columns = map(str.upper, df_by_all.columns)
    _write_to_snowflake(ctx, df_by_all, 'ROKU_DIGITAL_BY_PUB_BRAND_MATCH_RATE_LOG')


def _write_to_snowflake(ctx, df, table_name):
    """Write DataFrame to Snowflake, auto-creating table on first run."""
    try:
        write_pandas(ctx, df, table_name)
    except Exception:
        write_pandas(ctx, df, table_name, auto_create_table=True)
    print(f"  Wrote {df.shape[0]} rows to {table_name}")


def main():
    # Load config for dev/prod toggle
    with open('./config/build_config.json') as f:
        config = json.load(f)

    if config['sf_write_dev'] == 'True':
        sf_database = 'ISPOT_DEV'
    else:
        sf_database = 'ISPOT_PROD'

    # Get Snowflake connection via service account
    cs, ctx = sauth.snowflake_engine(
        user='SVC',
        snowflake_database=sf_database,
        snowflake_schema='ISPOT_QA'
    )

    # Use day - 2 as the data_dump_date (data needs 2 days to be fully available)
    data_dump_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    print(f"Running match rate monitoring for data_dump_date = {data_dump_date}")
    print(f"Writing to {sf_database}.ISPOT_QA")
    print()

    # Run Linear
    print("--- Linear Match Rate ---")
    run_linear_match_rate(data_dump_date, ctx)
    print()

    # Run Digital
    print("--- Digital Match Rate ---")
    run_digital_match_rate(data_dump_date, ctx)
    print()

    print("Done!")
    ctx.close()


if __name__ == "__main__":
    main()

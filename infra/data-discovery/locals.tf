locals {
  db_prefix = replace(var.name_prefix, "-", "_")

  # Excludes JSON files whose FILE NAME contains "model", case-insensitive.
  # Glue exclude patterns are case-sensitive, hence the character classes.
  # "**" crosses folders, "*" does not, so folder names containing "model" are not matched.
  model_json_exclusion = "**[mM][oO][dD][eE][lL]*.json"

  # Dataset roots exactly as they appear in the supplied bucket structures.
  equity_root      = "s3://${var.source_buckets.equity}/historical-equity"
  fx_root          = "s3://${var.source_buckets.fx}/historical-fx"
  bond_root        = "s3://${var.source_buckets.bond}/historical-bonds"
  commodity_root   = "s3://${var.source_buckets.commodity}/historical-commodity"
  loan_root        = "s3://${var.source_buckets.loan}"
  collateral_root  = "s3://${var.source_buckets.collateral}"
  derivatives_root = "s3://${var.source_buckets.derivatives}"
  platform_root    = "s3://${var.source_buckets.platform}"

  # One Glue database per source bucket.
  glue_databases = {
    equity      = { name = "${local.db_prefix}_equity_historical", description = "Equity historical pipeline data: raw, features, rolling window, DLQ, run metadata" }
    fx          = { name = "${local.db_prefix}_fx_historical", description = "FX historical pipeline data: raw, features, rolling window, run metadata" }
    bond        = { name = "${local.db_prefix}_bond_historical", description = "Bond historical pipeline data: raw, features, rolling window, run metadata" }
    commodity   = { name = "${local.db_prefix}_commodity_historical", description = "Commodity historical pipeline data: raw, features, rolling window, run metadata" }
    loan        = { name = "${local.db_prefix}_loan", description = "Loan datasets and loan pipeline run metadata" }
    collateral  = { name = "${local.db_prefix}_collateral", description = "Collateral pipeline run metadata" }
    derivatives = { name = "${local.db_prefix}_derivatives", description = "Derivatives pipeline run metadata" }
    platform    = { name = "${local.db_prefix}_risk_platform", description = "Streaming platform data: live/backfill/replay, Kafka raw, producer and consumer DLQs" }
  }

  # ---------------------------------------------------------------------------
  # Crawlers for folder-based (partitioned) datasets.
  # A crawler names each table after the last folder of its target path, so
  # table_prefix is used wherever two targets would otherwise collide.
  # ---------------------------------------------------------------------------
  pipeline_crawler_defs = {
    equity-pipeline = {
      database     = "equity"
      description  = "Equity raw, features, rolling window, DLQ and ingestion run metadata"
      table_prefix = ""
      targets = [
        { path = "${local.equity_root}/raw/" },
        { path = "${local.equity_root}/features/", exclusions = ["latest.json"] },
        { path = "${local.equity_root}/rolling/" },
        { path = "${local.equity_root}/dlq/" },
        { path = "${local.equity_root}/metadata/" },
      ]
    }
    equity-risk = {
      database     = "equity"
      description  = "Equity risk pipeline run summaries"
      table_prefix = "risk_"
      targets = [
        { path = "${local.equity_root}/equity-risk/metadata/" },
      ]
    }

    fx-pipeline = {
      database     = "fx"
      description  = "FX raw, features, rolling window and ingestion run metadata"
      table_prefix = ""
      targets = [
        { path = "${local.fx_root}/raw/" },
        { path = "${local.fx_root}/features/", exclusions = ["latest.json"] },
        { path = "${local.fx_root}/rolling/" },
        { path = "${local.fx_root}/metadata/" },
      ]
    }
    fx-risk = {
      database     = "fx"
      description  = "FX risk pipeline run summaries"
      table_prefix = "risk_"
      targets = [
        { path = "${local.fx_root}/fx-risk/metadata/" },
      ]
    }

    bond-pipeline = {
      database     = "bond"
      description  = "Bond raw, features, rolling window and per-run metadata files"
      table_prefix = ""
      targets = [
        { path = "${local.bond_root}/raw/" },
        { path = "${local.bond_root}/features/", exclusions = ["latest.json"] },
        { path = "${local.bond_root}/rolling/" },
        { path = "${local.bond_root}/run_metadata/" },
      ]
    }
    bond-risk = {
      database     = "bond"
      description  = "Bond risk pipeline run summaries"
      table_prefix = "risk_"
      targets = [
        { path = "${local.bond_root}/bond-risk/metadata/" },
      ]
    }

    commodity-pipeline = {
      database     = "commodity"
      description  = "Commodity raw, features, rolling window and ingestion run metadata"
      table_prefix = ""
      targets = [
        { path = "${local.commodity_root}/raw/" },
        { path = "${local.commodity_root}/features/", exclusions = ["latest.json"] },
        { path = "${local.commodity_root}/rolling/" },
        { path = "${local.commodity_root}/metadata/" },
      ]
    }
    commodity-risk = {
      database     = "commodity"
      description  = "Commodity risk pipeline run summaries"
      table_prefix = "risk_"
      targets = [
        { path = "${local.commodity_root}/commodity-risk/metadata/" },
      ]
    }

    loan-metadata = {
      database     = "loan"
      description  = "Loan enrichment and loan model pipeline run summaries"
      table_prefix = ""
      targets = [
        { path = "${local.loan_root}/metadata/enrich_loans_pipeline/" },
        { path = "${local.loan_root}/metadata/loans_model_pipeline/" },
      ]
    }
    collateral-metadata = {
      database     = "collateral"
      description  = "Collateral pipeline run summaries"
      table_prefix = ""
      targets = [
        { path = "${local.collateral_root}/metadata/" },
      ]
    }
    derivatives-metadata = {
      database     = "derivatives"
      description  = "Derivatives pipeline run summaries"
      table_prefix = ""
      targets = [
        { path = "${local.derivatives_root}/metadata/" },
      ]
    }

    platform-equity-data = {
      database     = "platform"
      description  = "Equity processed data by processing mode (live, backfill, replay)"
      table_prefix = "equity_data_"
      targets = [
        { path = "${local.platform_root}/equity/data/live/" },
        { path = "${local.platform_root}/equity/data/backfill/" },
        { path = "${local.platform_root}/equity/data/replay/" },
      ]
    }
    platform-fx-data = {
      database     = "platform"
      description  = "FX processed data by processing mode (live, backfill, replay)"
      table_prefix = "fx_data_"
      targets = [
        { path = "${local.platform_root}/fx/data/live/" },
        { path = "${local.platform_root}/fx/data/backfill/" },
        { path = "${local.platform_root}/fx/data/replay/" },
      ]
    }
    platform-kafka-raw-equity = {
      database     = "platform"
      description  = "Equity Kafka raw landing data (backfill, live)"
      table_prefix = "kafka_raw_equity_"
      targets = [
        { path = "${local.platform_root}/kafka_raw/equity/backfill/" },
        { path = "${local.platform_root}/kafka_raw/equity/live/" },
      ]
    }
    platform-kafka-raw-fx = {
      database     = "platform"
      description  = "FX Kafka raw landing data (backfill, live)"
      table_prefix = "kafka_raw_fx_"
      targets = [
        { path = "${local.platform_root}/kafka_raw/fx/backfill/" },
        { path = "${local.platform_root}/kafka_raw/fx/live/" },
      ]
    }
    platform-kafka-dlq-equity = {
      database     = "platform"
      description  = "Equity producer DLQ (backfill, live)"
      table_prefix = "producer_dlq_equity_"
      targets = [
        { path = "${local.platform_root}/kafka_dlq/equity/producer/backfill/" },
        { path = "${local.platform_root}/kafka_dlq/equity/producer/live/" },
      ]
    }
    platform-kafka-dlq-fx = {
      database     = "platform"
      description  = "FX producer DLQ (backfill, live)"
      table_prefix = "producer_dlq_fx_"
      targets = [
        { path = "${local.platform_root}/kafka_dlq/fx/producer/backfill/" },
        { path = "${local.platform_root}/kafka_dlq/fx/producer/live/" },
      ]
    }
    platform-equity-consumer-dlq = {
      database     = "platform"
      description  = "Equity consumer DLQ (live, backfill, replay)"
      table_prefix = "consumer_dlq_equity_"
      targets = [
        { path = "${local.platform_root}/equity/dlq/live/" },
        { path = "${local.platform_root}/equity/dlq/backfill/" },
        { path = "${local.platform_root}/equity/dlq/replay/" },
      ]
    }
    platform-fx-consumer-dlq = {
      database     = "platform"
      description  = "FX consumer DLQ (live, backfill, replay)"
      table_prefix = "consumer_dlq_fx_"
      targets = [
        { path = "${local.platform_root}/fx/dlq/live/" },
        { path = "${local.platform_root}/fx/dlq/backfill/" },
        { path = "${local.platform_root}/fx/dlq/replay/" },
      ]
    }
  }

  # ---------------------------------------------------------------------------
  # Crawlers for single files that sit directly in a bucket or prefix root.
  # Root folders also contain partitioned sub-folders, so a folder-level table
  # there would make Athena read everything beneath it. Each file therefore gets
  # its own single-file target. Files are small, so a full recrawl is cheap.
  # ---------------------------------------------------------------------------
  root_file_crawler_defs = {
    equity-root-files = {
      database     = "equity"
      description  = "Equity reference files and features/latest.json"
      table_prefix = "file_"
      recrawl      = "CRAWL_EVERYTHING"
      scheduled    = false
      targets = [
        for f in [
          "asset_managers.csv",
          "final_merged.parquet",
          "final_tickers.csv",
          "ticker_manager_mapping.parquet",
          "tickers50_asset_manager.csv",
          "tickers50.csv",
          "features/latest.json",
        ] : { path = "${local.equity_root}/${f}", single_file = true }
      ]
    }
    fx-root-files = {
      database     = "fx"
      description  = "FX final_merged.parquet and features/latest.json"
      table_prefix = "file_"
      recrawl      = "CRAWL_EVERYTHING"
      scheduled    = false
      targets = [
        for f in ["final_merged.parquet", "features/latest.json"] :
        { path = "${local.fx_root}/${f}", single_file = true }
      ]
    }
    bond-root-files = {
      database     = "bond"
      description  = "Bond synthetic_bond.csv and features/latest.json"
      table_prefix = "file_"
      recrawl      = "CRAWL_EVERYTHING"
      scheduled    = false
      targets = [
        for f in ["synthetic_bond.csv", "features/latest.json"] :
        { path = "${local.bond_root}/${f}", single_file = true }
      ]
    }
    commodity-root-files = {
      database     = "commodity"
      description  = "Commodity unique_tickers_sector.csv and features/latest.json"
      table_prefix = "file_"
      recrawl      = "CRAWL_EVERYTHING"
      scheduled    = false
      targets = [
        for f in ["unique_tickers_sector.csv", "features/latest.json"] :
        { path = "${local.commodity_root}/${f}", single_file = true }
      ]
    }
    loan-root-files = {
      database     = "loan"
      description  = "Loan base, enriched loan and macro datasets"
      table_prefix = "file_"
      recrawl      = "CRAWL_EVERYTHING"
      scheduled    = false
      targets = [
        for f in [
          "loan_enriched_fx_bonds_commod_derivatives_collateral.parquet",
          "loan_synthetic_base.parquet",
          "macro_data.csv",
        ] : { path = "${local.loan_root}/${f}", single_file = true }
      ]
    }
  }

  crawler_defs = merge(
    local.pipeline_crawler_defs,
    { for k, v in local.root_file_crawler_defs : k => v if var.catalog_root_files },
  )

  # Normalised crawler specification consumed by aws_glue_crawler.
  crawlers = {
    for k, c in local.crawler_defs : k => {
      database     = c.database
      description  = c.description
      table_prefix = c.table_prefix
      recrawl      = try(c.recrawl, var.recrawl_behavior)
      scheduled    = try(c.scheduled, true)
      targets = [
        for t in c.targets : {
          path        = t.path
          exclusions  = concat(try(t.exclusions, []), [local.model_json_exclusion])
          sample_size = try(t.single_file, false) ? null : var.crawler_sample_size
        }
      ]
    }
  }
}
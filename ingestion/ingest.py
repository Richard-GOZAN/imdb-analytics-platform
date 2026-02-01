#!/usr/bin/env python3
"""
IMDB Data Ingestion Pipeline

This script orchestrates the complete data ingestion workflow:
1. Download IMDB datasets from https://datasets.imdbws.com/
2. Convert TSV.GZ files to Parquet format
3. Upload Parquet files to Google Cloud Storage
4. Load data into BigQuery bronze layer
"""

import logging
import sys
from typing import List, Optional

import click

from ingestion.config import Config
from ingestion.utils import (
    cleanup_local_files,
    convert_to_parquet,
    download_file,
    load_to_bigquery,
    upload_to_gcs,
)

# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# Core Pipeline Functions
# =============================================================================


def process_table(
    table: str,
    force_download: bool = False,
    skip_bigquery: bool = False,
) -> bool:
    """
    Process a single IMDB table through the complete pipeline.

    Args:
        table: Table name (e.g., "title.basics")
        force_download: If True, re-download even if file exists
        skip_bigquery: If True, skip loading to BigQuery (for testing)

    Returns:
        True if successful, False if any step failed
    """
    logger.info(f"{'=' * 70}")
    logger.info(f"Processing table: {table}")
    logger.info(f"{'=' * 70}")

    try:
        # =====================================================================
        # Step 1: Download TSV file
        # =====================================================================
        filename = Config.get_table_filename(table)
        raw_path = Config.raw_data_folder / filename
        download_url = f"{Config.base_url}{filename}"

        if force_download or not raw_path.exists():
            download_file(download_url, raw_path)
        else:
            logger.info(f"Skipping download (file exists): {raw_path}")

        # =====================================================================
        # Step 2: Convert to Parquet
        # =====================================================================
        table_id = Config.get_table_id(table)
        parquet_path = Config.parquet_data_folder / f"{table_id}.parquet"

        if force_download or not parquet_path.exists():
            convert_to_parquet(raw_path, parquet_path)
        else:
            logger.info(f"Skipping conversion (file exists): {parquet_path}")

        # =====================================================================
        # Step 3: Upload to GCS
        # =====================================================================
        gcs_path = Config.get_gcs_path(table)
        gcs_uri = upload_to_gcs(parquet_path, gcs_path)

        # =====================================================================
        # Step 4: Load to BigQuery
        # =====================================================================
        if not skip_bigquery:
            load_to_bigquery(
                gcs_uri=gcs_uri,
                dataset_id=Config.bronze_dataset,
                table_id=table_id,
                write_disposition="WRITE_TRUNCATE",  # Overwrite on each run
            )
        else:
            logger.info("Skipping BigQuery load (--skip-bigquery flag)")

        logger.info(f"Successfully processed: {table}")
        return True

    except Exception as e:
        logger.error(f"Failed to process {table}: {e}", exc_info=True)
        return False


def run_pipeline(
    tables: Optional[List[str]] = None,
    force_download: bool = False,
    skip_bigquery: bool = False,
    fail_fast: bool = False,
    cleanup: bool = False,
) -> int:
    """
    Run the complete ingestion pipeline for multiple tables.

    Args:
        tables: List of tables to process (defaults to all tables)
        force_download: If True, re-download all files
        skip_bigquery: If True, skip BigQuery loading
        fail_fast: If True, stop on first failure
        cleanup: If True, delete local files after successful load

    Returns:
        Exit code (0 = success, 1 = partial failure, 2 = total failure)
    """
    # Use default tables if none specified
    tables = tables or Config.tables

    logger.info(f"{'=' * 70}")
    logger.info("IMDB Data Ingestion Pipeline")
    logger.info(f"{'=' * 70}")
    logger.info(f"Project: {Config.project_id}")
    logger.info(f"Bucket: {Config.bucket_name}")
    logger.info(f"Dataset: {Config.bronze_dataset}")
    logger.info(f"Tables to process: {len(tables)}")
    logger.info(f"Force download: {force_download}")
    logger.info(f"{'=' * 70}")

    # Process each table
    results = {}
    for i, table in enumerate(tables, 1):
        logger.info(f"\n[{i}/{len(tables)}] Starting: {table}")

        success = process_table(
            table=table,
            force_download=force_download,
            skip_bigquery=skip_bigquery,
        )

        results[table] = success

        # Stop on first failure if fail_fast enabled
        if not success and fail_fast:
            logger.error("Stopping due to failure (--fail-fast enabled)")
            break

    # Summary
    logger.info(f"\n{'=' * 70}")
    logger.info("Pipeline Summary")
    logger.info(f"{'=' * 70}")

    successful = [t for t, success in results.items() if success]
    failed = [t for t, success in results.items() if not success]

    logger.info(f"Successful: {len(successful)}/{len(results)}")
    for table in successful:
        logger.info(f"  - {table}")

    if failed:
        logger.error(f"Failed: {len(failed)}/{len(results)}")
        for table in failed:
            logger.error(f"  - {table}")

    # Cleanup if requested and all succeeded
    if cleanup and not failed:
        logger.info("\nCleaning up local files...")
        cleanup_local_files(keep_parquet=True)

    # Determine exit code
    if not failed:
        logger.info("\nAll tables processed successfully!")
        return 0
    elif successful:
        logger.warning("\nPipeline completed with some failures")
        return 1
    else:
        logger.error("\nPipeline failed completely")
        return 2


# =============================================================================
# CLI Interface
# =============================================================================


@click.command()
@click.option(
    "--force-download",
    is_flag=True,
    help="Force re-download of all files (default: skip if exists)",
)
@click.option(
    "--skip-bigquery",
    is_flag=True,
    help="Skip loading to BigQuery (useful for testing)",
)
@click.option(
    "--tables",
    multiple=True,
    help="Specific tables to process (default: all tables)",
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on first failure (default: continue)",
)
@click.option(
    "--cleanup",
    is_flag=True,
    help="Delete local files after successful load",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without executing",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging (DEBUG level)",
)
def main(
    force_download: bool,
    skip_bigquery: bool,
    tables: tuple,
    fail_fast: bool,
    cleanup: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    """
    IMDB Data Ingestion Pipeline

    Downloads, converts, and loads IMDB datasets into BigQuery.

    \b
    Examples:
        # Full ingestion with download
        python ingest.py --force-download

        # Load existing files
        python ingest.py

        # Process specific tables
        python ingest.py --tables title.basics --tables title.ratings

        # Dry run to see what would happen
        python ingest.py --dry-run
    """
    # Set log level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate configuration
    try:
        Config.validate()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(2)

    # Dry run: just show configuration and exit
    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("\nConfiguration:")
        logger.info(f"  Project: {Config.project_id}")
        logger.info(f"  Bucket: {Config.bucket_name}")
        logger.info(f"  Dataset: {Config.bronze_dataset}")
        logger.info(f"  Tables: {tables or Config.tables}")
        logger.info(f"  Force download: {force_download}")
        logger.info(f"  Skip BigQuery: {skip_bigquery}")
        sys.exit(0)

    # Run pipeline
    exit_code = run_pipeline(
        tables=list(tables) if tables else None,
        force_download=force_download,
        skip_bigquery=skip_bigquery,
        fail_fast=fail_fast,
        cleanup=cleanup,
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()

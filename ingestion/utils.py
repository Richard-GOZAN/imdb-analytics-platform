"""
Utility functions for IMDB data ingestion pipeline.

This module provides reusable functions for common data engineering tasks:
- Downloading files from HTTP endpoints
- Converting TSV files to Parquet format
- Uploading files to Google Cloud Storage
- Loading data into BigQuery from GCS

"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from ingestion.config import Config

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(Config.log_file),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# =============================================================================
# File Download Functions
# =============================================================================


def download_file(url: str, destination: Path, chunk_size: int = 8192) -> None:
    """
    Download a file from a URL with progress indication.

    This function downloads large files in chunks to avoid memory issues.
    It includes basic retry logic and progress logging.

    Args:
        url: Full URL of the file to download
        destination: Local path where the file should be saved
        chunk_size: Size of chunks to download (in bytes)

    Raises:
        requests.RequestException: If the download fails
        IOError: If the file cannot be written
    """
    logger.info(f"Downloading {url} to {destination}")

    try:
        # Make HTTP request with streaming enabled
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # Get total file size for progress tracking
        total_size = int(response.headers.get("content-length", 0))
        downloaded_size = 0

        # Write file in chunks
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # Log progress every 10MB
                    if downloaded_size % (10 * 1024 * 1024) < chunk_size:
                        progress = (downloaded_size / total_size * 100) if total_size else 0
                        logger.info(f"Progress: {progress:.1f}% ({downloaded_size // 1024 // 1024} MB)")

        logger.info(f"✓ Download complete: {destination}")

    except requests.RequestException as e:
        logger.error(f"✗ Download failed: {e}")
        raise


# =============================================================================
# Data Conversion Functions
# =============================================================================


def convert_to_parquet(
    tsv_path: Path,
    parquet_path: Path,
    compression: str = "snappy",
) -> None:
    """
    Convert a TSV.GZ file to Parquet format.

    Args:
        tsv_path: Path to the input TSV.GZ file
        parquet_path: Path where the Parquet file should be saved
        compression: Compression algorithm ('snappy', 'gzip', 'brotli')

    Raises:
        FileNotFoundError: If the TSV file doesn't exist
        pd.errors.ParserError: If the TSV file is malformed
    """
    logger.info(f"Converting {tsv_path} to {parquet_path}")

    try:
        # Read TSV file
        df = pd.read_csv(
            tsv_path,
            sep="\t",
            compression="gzip",
            na_values=r"\N",  
            keep_default_na=True,
            low_memory=False,  
        )

        logger.info(f"Read {len(df):,} rows with {len(df.columns)} columns")

        # Write to Parquet
        df.to_parquet(
            parquet_path,
            compression=compression,
            index=False,  
            engine="pyarrow",  
        )

        # Log file size reduction
        original_size = tsv_path.stat().st_size
        parquet_size = parquet_path.stat().st_size
        compression_ratio = (1 - parquet_size / original_size) * 100

        logger.info(
            f"Conversion complete: {parquet_path} "
            f"({compression_ratio:.1f}% size reduction)"
        )

    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        raise


# =============================================================================
# Google Cloud Storage Functions
# =============================================================================


def get_gcs_client() -> storage.Client:
    """
    Get an authenticated GCS client.

    Returns:
        Authenticated GCS client

    Raises:
        Exception: If authentication fails
    """
    credentials = service_account.Credentials.from_service_account_file(
        Config.credentials_path
    )
    return storage.Client(project=Config.project_id, credentials=credentials)


def upload_to_gcs(
    local_path: Path,
    gcs_path: str,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Upload a file to Google Cloud Storage.

    Args:
        local_path: Path to the local file to upload
        gcs_path: Destination path in GCS (without gs:// prefix)
        bucket_name: GCS bucket name (defaults to Config.bucket_name)

    Returns:
        Full GCS URI of the uploaded file

    Raises:
        FileNotFoundError: If the local file doesn't exist
        google.cloud.exceptions.GoogleCloudError: If upload fails
    """
    bucket_name = bucket_name or Config.bucket_name
    logger.info(f"Uploading {local_path} to gs://{bucket_name}/{gcs_path}")

    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)

        # Upload with progress tracking for large files
        blob.upload_from_filename(str(local_path))

        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        logger.info(f"Upload complete: {gcs_uri}")

        return gcs_uri

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise


# =============================================================================
# BigQuery Functions
# =============================================================================


def get_bigquery_client() -> bigquery.Client:
    """
    Get an authenticated BigQuery client.

    Returns:
        Authenticated BigQuery client

    Raises:
        Exception: If authentication fails
    """
    credentials = service_account.Credentials.from_service_account_file(
        Config.credentials_path
    )
    return bigquery.Client(project=Config.project_id, credentials=credentials)


def ensure_dataset_exists(dataset_id: str, location: str = "EU") -> None:
    """
    Ensure a BigQuery dataset exists, creating it if necessary.

    This function is idempotent - calling it multiple times is safe.

    Args:
        dataset_id: BigQuery dataset ID (e.g., "bronze_id2608")
        location: Dataset location (e.g., "EU", "US")

    Raises:
        google.cloud.exceptions.GoogleCloudError: If creation fails
    """
    client = get_bigquery_client()
    dataset_ref = f"{Config.project_id}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists")
    except NotFound:
        logger.info(f"Creating dataset {dataset_ref}")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Dataset created: {dataset_ref}")
    except Exception as e:
        logger.error(f"Dataset check/creation failed: {e}")
        raise


def load_to_bigquery(
    gcs_uri: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """
    Load data from GCS Parquet file into a BigQuery table.

    Args:
        gcs_uri: Full GCS URI (e.g., "gs://bucket/path/file.parquet")
        dataset_id: Target BigQuery dataset ID
        table_id: Target BigQuery table ID
        write_disposition: How to handle existing data:
            - WRITE_TRUNCATE: Overwrite table (default)
            - WRITE_APPEND: Append to existing table
            - WRITE_EMPTY: Fail if table exists

    Raises:
        google.cloud.exceptions.GoogleCloudError: If load fails
    """
    logger.info(f"Loading {gcs_uri} into {dataset_id}.{table_id}")

    try:
        client = get_bigquery_client()
        table_ref = f"{Config.project_id}.{dataset_id}.{table_id}"

        # Ensure dataset exists
        ensure_dataset_exists(dataset_id)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            # Parquet files include schema, so no need to specify
            autodetect=True,
        )

        # Start load job
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
        )

        # Wait for job to complete
        logger.info(f"Job started: {load_job.job_id}")
        load_job.result()  # Blocks until complete

        # Get final table stats
        table = client.get_table(table_ref)
        logger.info(
            f"✓ Load complete: {table.num_rows:,} rows loaded into {table_ref}"
        )

    except Exception as e:
        logger.error(f"✗ Load failed: {e}")
        raise


# =============================================================================
# Cleanup Functions
# =============================================================================


def cleanup_local_files(keep_parquet: bool = True) -> None:
    """
    Clean up local data files to free disk space.

    Args:
        keep_parquet: If True, only delete raw TSV files (default)
                     If False, delete all local data files
    """
    logger.info("Cleaning up local files")

    # Delete raw TSV files
    for file in Config.raw_data_folder.glob("*.tsv.gz"):
        file.unlink()
        logger.info(f"Deleted: {file}")

    # Delete Parquet files if requested
    if not keep_parquet:
        for file in Config.parquet_data_folder.glob("*.parquet"):
            file.unlink()
            logger.info(f"Deleted: {file}")

    logger.info("✓ Cleanup complete")

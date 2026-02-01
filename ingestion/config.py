"""
Configuration module for IMDB data ingestion pipeline.

This module provides a centralized configuration class that loads settings
from environment variables. 
"""

import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Configuration class for the ingestion pipeline.

    Attributes:
        project_id: GCP project ID
        bucket_name: GCS bucket name for storing Parquet files
        bronze_dataset: BigQuery dataset for raw data
        silver_dataset: BigQuery dataset for transformed data
        region: GCP region (EU, US, etc.)
        base_url: Base URL for IMDB datasets
        raw_data_folder: Local folder for downloaded TSV files
        parquet_data_folder: Local folder for converted Parquet files
        tables: List of IMDB tables to ingest
    """

    # =========================================================================
    # Google Cloud Platform Configuration
    # =========================================================================

    project_id: str = os.getenv("GCP_PROJECT_ID", "ensai-2026")
    bucket_name: str = os.getenv("GCS_BUCKET_NAME", "imdb-data-id2608")
    bronze_dataset: str = os.getenv("BQ_BRONZE_DATASET", "bronze_id2608")
    silver_dataset: str = os.getenv("BQ_SILVER_DATASET", "silver_id2608")
    region: str = os.getenv("GCP_REGION", "EU")

    # Path to service account credentials
    credentials_path: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", "./credentials.json"
    )

    # =========================================================================
    # IMDB Data Configuration
    # =========================================================================

    base_url: str = os.getenv(
        "IMDB_BASE_URL", "https://datasets.imdbws.com/"
    )

    # Local data directories
    raw_data_folder: Path = Path(os.getenv("RAW_DATA_FOLDER", "data/raw"))
    parquet_data_folder: Path = Path(
        os.getenv("PARQUET_DATA_FOLDER", "data/parquet")
    )

    # List of IMDB tables to ingest
    # These correspond to the files available at https://datasets.imdbws.com/
    tables: List[str] = [
        "name.basics",      # Person information (actors, directors)
        "title.akas",       # Alternative titles (different languages/regions)
        "title.basics",     # Core movie/title information
        "title.crew",       # Director and writer assignments
        "title.episode",    # TV episode information
        "title.principals", # Principal cast/crew for each title
        "title.ratings",    # User ratings and vote counts
    ]

    # =========================================================================
    # Logging Configuration
    # =========================================================================

    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: Path = Path(os.getenv("LOG_FILE", "logs/ingestion.log"))

    # =========================================================================
    # Helper Methods
    # =========================================================================

    @classmethod
    def validate(cls) -> None:
        """
        Validate that all required configuration is present.

        Raises:
            ValueError: If required environment variables are missing
            FileNotFoundError: If credentials file doesn't exist
        """
        # Check required environment variables
        required_vars = {
            "GCP_PROJECT_ID": cls.project_id,
            "GCS_BUCKET_NAME": cls.bucket_name,
        }

        missing_vars = [
            var for var, value in required_vars.items()
            if not value or value.startswith("your-")
        ]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please copy .env.template to .env and fill in your values."
            )

        # Check credentials file exists
        if not Path(cls.credentials_path).exists():
            raise FileNotFoundError(
                f"Credentials file not found: {cls.credentials_path}\n"
                f"Please download your service account key from GCP Console and "
                f"place it at this location."
            )

    @classmethod
    def create_directories(cls) -> None:
        """
        Create necessary local directories if they don't exist.

        This ensures that the pipeline can write downloaded and converted
        files without errors.
        """
        cls.raw_data_folder.mkdir(parents=True, exist_ok=True)
        cls.parquet_data_folder.mkdir(parents=True, exist_ok=True)
        cls.log_file.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_table_filename(cls, table: str) -> str:
        """
        Get the filename for a given IMDB table.

        Args:
            table: Table name (e.g., "title.basics")

        Returns:
            Filename with .tsv.gz extension
        """
        return f"{table}.tsv.gz"

    @classmethod
    def get_table_id(cls, table: str) -> str:
        """
        Convert table name to BigQuery table ID.

        BigQuery table names cannot contain dots, so we replace them
        with underscores.

        Args:
            table: Table name (e.g., "title.basics")

        Returns:
            BigQuery-compatible table ID
        """
        return table.replace(".", "_")

    @classmethod
    def get_gcs_path(cls, table: str) -> str:
        """
        Get the GCS path for a table's Parquet file.

        Args:
            table: Table name (e.g., "title.basics")

        Returns:
            GCS path (without gs:// prefix)
        """
        table_id = cls.get_table_id(table)
        return f"imdb/{table_id}/{table_id}.parquet"

    @classmethod
    def get_gcs_uri(cls, table: str) -> str:
        """
        Get the full GCS URI for a table's Parquet file.

        Args:
            table: Table name (e.g., "title.basics")

        Returns:
            Full GCS URI
        """
        return f"gs://{cls.bucket_name}/{cls.get_gcs_path(table)}"

    def __repr__(self) -> str:
        """String representation of the configuration."""
        return (
            f"Config("
            f"project_id='{self.project_id}', "
            f"bucket='{self.bucket_name}', "
            f"bronze='{self.bronze_dataset}', "
            f"silver='{self.silver_dataset}'"
            f")"
        )


# Validate configuration on module import (fail fast if misconfigured)
try:
    Config.validate()
    Config.create_directories()
except (ValueError, FileNotFoundError) as e:
    # Don't fail hard during import, just warn
    # This allows the module to be imported for documentation/testing
    import warnings
    warnings.warn(f"Configuration validation failed: {e}", UserWarning)

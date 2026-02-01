"""
BigQuery Tool for LLM Agent

This module provides functions to execute SQL queries on BigQuery
and retrieve schema information for the LLM to understand the data structure.
"""

import os
from typing import Dict, List, Any

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "imdb-analytics")
DATASET_ID = os.getenv("BQ_SILVER_DATASET", "silver")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./credentials.json")


def get_bigquery_client() -> bigquery.Client:
    """
    Get authenticated BigQuery client.
    
    Returns:
        Authenticated BigQuery client
    """
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def execute_sql(sql_query: str) -> Dict[str, Any]:
    """
    Execute a SQL query on BigQuery and return results.
    
    Args:
        sql_query: SQL query to execute
    
    Returns:
        Dictionary with:
        - success: bool
        - data: DataFrame or None
        - error: str or None
        - rows_returned: int
    """
    try:
        client = get_bigquery_client()
        
        # Execute query
        query_job = client.query(sql_query)
        results = query_job.result()
        
        # Convert to DataFrame
        df = results.to_dataframe()
        
        return {
            "success": True,
            "data": df,
            "error": None,
            "rows_returned": len(df),
            "bytes_processed": query_job.total_bytes_processed,
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": str(e),
            "rows_returned": 0,
            "bytes_processed": 0,
        }


def get_schema() -> Dict[str, List[Dict[str, str]]]:
    """
    Get schema information for all tables in the silver dataset.
    
    Returns:
        Dictionary mapping table names to their column schemas
    """
    client = get_bigquery_client()
    
    # Tables to document
    tables = ["movies", "dim_actors", "dim_directors"]
    
    schema_info = {}
    
    for table_name in tables:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        table = client.get_table(table_ref)
        
        columns = []
        for field in table.schema:
            column_info = {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or "",
            }
            
            # Handle nested fields (STRUCT/ARRAY)
            if field.field_type == "RECORD" and field.fields:
                nested_fields = []
                for nested_field in field.fields:
                    nested_fields.append({
                        "name": nested_field.name,
                        "type": nested_field.field_type,
                    })
                column_info["fields"] = nested_fields
            
            columns.append(column_info)
        
        schema_info[table_name] = columns
    
    return schema_info


def format_schema_for_llm() -> str:
    """
    Format schema information in a way that's easy for LLM to understand.
    
    Returns:
        Formatted string describing the database schema
    """
    schema = get_schema()
    
    prompt = f"""
        You have access to a BigQuery database with IMDB movie data in the `{PROJECT_ID}.{DATASET_ID}` dataset.

        Here are the available tables and their schemas:

        """
    
    for table_name, columns in schema.items():
        prompt += f"\n## Table: {DATASET_ID}.{table_name}\n\n"
        
        for col in columns:
            prompt += f"- **{col['name']}** ({col['type']}): "
            
            # Add nested fields info
            if "fields" in col:
                nested = ", ".join([f"{f['name']} ({f['type']})" for f in col["fields"]])
                prompt += f"STRUCT with fields: {nested}"
            
            prompt += "\n"
    
    prompt += """

        **Important Notes:**
        - The `movies` table contains nested arrays:
        - `directors`: ARRAY of STRUCT with director information
        - `actors`: ARRAY of STRUCT with actor information
        - Use UNNEST() to query nested arrays: `FROM movies, UNNEST(directors) AS director`
        - Use EXISTS() for filtering on nested fields
        - All queries must be fully qualified: `imdb-analytics.silver.table_name`

        **Example Queries:**

        Find movies by director:
        ```sql
        SELECT title, release_year, average_rating
        FROM `imdb-analytics.silver.movies`
        WHERE EXISTS (
        SELECT 1 FROM UNNEST(directors) AS d
        WHERE d.name LIKE '%Nolan%'
        )
        ORDER BY average_rating DESC
        LIMIT 10
        ```

        Find movies with specific actor:
        ```sql
        SELECT title, release_year, average_rating
        FROM `imdb-analytics.silver.movies`
        WHERE EXISTS (
        SELECT 1 FROM UNNEST(actors) AS a
        WHERE a.name = 'Tom Hanks'
        )
        ORDER BY average_rating DESC
        ```
        """
    
    return prompt


# Test function
if __name__ == "__main__":
    # Test schema retrieval
    print("=== Schema Information ===")
    schema_prompt = format_schema_for_llm()
    print(schema_prompt)
    
    # Test query execution
    print("\n=== Test Query ===")
    test_query = """
    SELECT title, release_year, average_rating
    FROM `imdb-analytics.silver.movies`
    ORDER BY average_rating DESC
    LIMIT 5
    """
    
    result = execute_sql(test_query)
    if result["success"]:
        print(f"Query successful! {result['rows_returned']} rows returned")
        print(result["data"])
    else:
        print(f"Query failed: {result['error']}")

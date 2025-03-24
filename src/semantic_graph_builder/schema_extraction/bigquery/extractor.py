"""
BigQuery Schema Extractor

Extracts table schemas and metadata from BigQuery using INFORMATION_SCHEMA views.
"""
import logging
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class BigQuerySchemaExtractor:
    """Extracts schema information from BigQuery tables."""
    
    def __init__(self, project_id: str):
        """
        Initialize the BigQuery schema extractor.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    async def extract_datasets(self) -> List[Dict]:
        """
        Extract all datasets in the project.
        
        Returns:
            List of dataset metadata dictionaries
        """
        try:
            datasets = list(self.client.list_datasets())
            return [
                {
                    "dataset_id": dataset.dataset_id,
                    "friendly_name": dataset.friendly_name,
                    "description": dataset.description,
                    "labels": dataset.labels,
                    "created": dataset.created.isoformat() if dataset.created else None,
                }
                for dataset in datasets
            ]
        except GoogleCloudError as e:
            logger.error(f"Error extracting datasets: {e}")
            raise
    
    async def extract_tables(self, dataset_id: str) -> List[Dict]:
        """
        Extract all tables in a dataset.
        
        Args:
            dataset_id: BigQuery dataset ID
            
        Returns:
            List of table metadata dictionaries
        """
        query = f"""
        SELECT
          t.table_catalog,
          t.table_schema,
          t.table_name,
          t.table_type,
          t.creation_time,
          t.is_insertable_into,
          t.is_typed,
          t.ddl
        FROM
          `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES` t
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [
                {
                    "table_catalog": row.table_catalog,
                    "table_schema": row.table_schema,
                    "table_name": row.table_name,
                    "table_type": row.table_type,
                    "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                    "is_insertable_into": row.is_insertable_into,
                    "is_typed": row.is_typed,
                    "ddl": row.ddl,
                }
                for row in results
            ]
        except GoogleCloudError as e:
            logger.error(f"Error extracting tables from {dataset_id}: {e}")
            raise
    
    async def extract_columns(self, dataset_id: str, table_name: Optional[str] = None) -> List[Dict]:
        """
        Extract column information for tables in a dataset.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_name: Optional table name to filter results
            
        Returns:
            List of column metadata dictionaries
        """
        table_filter = f"AND table_name = '{table_name}'" if table_name else ""
        
        query = f"""
        SELECT
          c.table_catalog,
          c.table_schema,
          c.table_name,
          c.column_name,
          c.ordinal_position,
          c.data_type,
          c.is_nullable,
          c.is_hidden,
          c.is_system_defined,
          c.is_partitioning_column,
          c.clustering_ordinal_position,
          c.description
        FROM
          `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` c
        WHERE
          1=1
          {table_filter}
        ORDER BY
          c.table_name,
          c.ordinal_position
        """
        
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [
                {
                    "table_catalog": row.table_catalog,
                    "table_schema": row.table_schema,
                    "table_name": row.table_name,
                    "column_name": row.column_name,
                    "ordinal_position": row.ordinal_position,
                    "data_type": row.data_type,
                    "is_nullable": row.is_nullable,
                    "is_hidden": row.is_hidden,
                    "is_system_defined": row.is_system_defined,
                    "is_partitioning_column": row.is_partitioning_column,
                    "clustering_ordinal_position": row.clustering_ordinal_position,
                    "description": row.description,
                }
                for row in results
            ]
        except GoogleCloudError as e:
            logger.error(f"Error extracting columns from {dataset_id}.{table_name or '*'}: {e}")
            raise
    
    async def extract_table_stats(self, dataset_id: str, table_name: str) -> Dict:
        """
        Extract basic statistics about a table.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_name: BigQuery table name
            
        Returns:
            Dictionary with table statistics
        """
        query = f"""
        SELECT
          COUNT(*) as row_count
        FROM
          `{self.project_id}.{dataset_id}.{table_name}`
        """
        
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            if not results:
                return {"row_count": 0}
                
            return {"row_count": results[0].row_count}
        except GoogleCloudError as e:
            logger.error(f"Error extracting stats from {dataset_id}.{table_name}: {e}")
            return {"row_count": None, "error": str(e)}
    
    async def extract_full_schema(self, dataset_id: str) -> Dict:
        """
        Extract complete schema information for a dataset.
        
        Args:
            dataset_id: BigQuery dataset ID
            
        Returns:
            Dictionary with complete schema information
        """
        tables = await self.extract_tables(dataset_id)
        all_columns = await self.extract_columns(dataset_id)
        
        # Group columns by table
        columns_by_table = {}
        for column in all_columns:
            table_name = column["table_name"]
            if table_name not in columns_by_table:
                columns_by_table[table_name] = []
            columns_by_table[table_name].append(column)
        
        # Add columns to tables
        for table in tables:
            table_name = table["table_name"]
            table["columns"] = columns_by_table.get(table_name, [])
            
            # Get table stats if not a view
            if table["table_type"] != "VIEW":
                stats = await self.extract_table_stats(dataset_id, table_name)
                table["statistics"] = stats
        
        return {
            "dataset_id": dataset_id,
            "tables": tables
        }
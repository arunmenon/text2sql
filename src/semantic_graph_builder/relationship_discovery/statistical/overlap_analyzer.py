"""
Statistical Overlap Analyzer

Analyzes column value overlaps to detect potential relationships between tables.
"""
import logging
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class OverlapAnalyzer:
    """Analyzes column value overlaps for relationship inference."""
    
    def __init__(self, project_id: str):
        """
        Initialize the overlap analyzer.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    async def analyze_column_overlap(
        self,
        dataset_id: str,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        sample_size: Optional[int] = None
    ) -> Dict:
        """
        Analyze the overlap between two columns.
        
        Args:
            dataset_id: BigQuery dataset ID
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            sample_size: Optional sample size for large tables
            
        Returns:
            Dictionary with overlap statistics
        """
        # Generate query with optional sampling
        sample_clause = ""
        if sample_size:
            sample_clause = f"TABLESAMPLE SYSTEM ({sample_size} PERCENT)"
        
        query = f"""
        WITH 
        source_values AS (
            SELECT DISTINCT {source_column} as value
            FROM `{self.project_id}.{dataset_id}.{source_table}` {sample_clause}
            WHERE {source_column} IS NOT NULL
        ),
        target_values AS (
            SELECT DISTINCT {target_column} as value
            FROM `{self.project_id}.{dataset_id}.{target_table}` {sample_clause}
            WHERE {target_column} IS NOT NULL
        ),
        overlap AS (
            SELECT COUNT(*) as overlap_count
            FROM source_values s
            INNER JOIN target_values t
            ON CAST(s.value AS STRING) = CAST(t.value AS STRING)
        ),
        source_count AS (
            SELECT COUNT(*) as total
            FROM source_values
        ),
        target_count AS (
            SELECT COUNT(*) as total
            FROM target_values
        )
        
        SELECT
            (SELECT total FROM source_count) as source_distinct,
            (SELECT total FROM target_count) as target_distinct,
            (SELECT overlap_count FROM overlap) as overlap_count,
            SAFE_DIVIDE(
                (SELECT overlap_count FROM overlap),
                (SELECT total FROM source_count)
            ) as source_overlap_pct,
            SAFE_DIVIDE(
                (SELECT overlap_count FROM overlap),
                (SELECT total FROM target_count)
            ) as target_overlap_pct
        """
        
        try:
            query_job = self.client.query(query)
            result = list(query_job.result())
            
            if not result:
                return {
                    "source_distinct": 0,
                    "target_distinct": 0,
                    "overlap_count": 0,
                    "source_overlap_pct": 0.0,
                    "target_overlap_pct": 0.0,
                    "confidence": 0.0
                }
            
            row = result[0]
            
            # Calculate a confidence score
            source_overlap = row.source_overlap_pct or 0
            target_overlap = row.target_overlap_pct or 0
            
            # Higher confidence when one side has high overlap
            confidence = max(source_overlap, target_overlap)
            
            return {
                "source_distinct": row.source_distinct,
                "target_distinct": row.target_distinct,
                "overlap_count": row.overlap_count,
                "source_overlap_pct": source_overlap,
                "target_overlap_pct": target_overlap,
                "confidence": confidence
            }
            
        except GoogleCloudError as e:
            logger.error(f"Error in column overlap analysis: {e}")
            return {
                "error": str(e),
                "confidence": 0.0
            }
    
    async def find_candidate_relationships(
        self,
        dataset_id: str,
        tables: List[Dict],
        min_confidence: float = 0.8,
        sample_size: Optional[int] = None
    ) -> List[Dict]:
        """
        Find candidate relationships across tables by analyzing column overlaps.
        
        Args:
            dataset_id: BigQuery dataset ID
            tables: List of table metadata dictionaries
            min_confidence: Minimum confidence threshold (0.0-1.0)
            sample_size: Optional sample size percentage for large tables
            
        Returns:
            List of candidate relationships
        """
        relationships = []
        
        # Get candidate column pairs
        candidate_pairs = self._get_candidate_column_pairs(tables)
        
        for source_table, source_column, target_table, target_column in candidate_pairs:
            logger.info(f"Analyzing overlap: {source_table}.{source_column} -> {target_table}.{target_column}")
            
            result = await self.analyze_column_overlap(
                dataset_id, 
                source_table, 
                source_column, 
                target_table, 
                target_column,
                sample_size
            )
            
            # Check if we met the confidence threshold
            if result.get("confidence", 0) >= min_confidence:
                relationships.append({
                    "source_table": source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "confidence": result["confidence"],
                    "statistics": result,
                    "detection_method": "statistical_overlap"
                })
        
        return relationships
    
    def _get_candidate_column_pairs(self, tables: List[Dict]) -> List[Tuple[str, str, str, str]]:
        """
        Generate candidate column pairs for overlap analysis.
        
        This method uses heuristics to choose which column pairs to analyze
        to avoid a full cartesian product of all columns.
        
        Args:
            tables: List of table metadata dictionaries
            
        Returns:
            List of (source_table, source_column, target_table, target_column) tuples
        """
        pairs = []
        
        # Generate pairs from likely column matches
        for i, source_table in enumerate(tables):
            source_table_name = source_table["table_name"]
            source_columns = source_table.get("columns", [])
            
            for j, target_table in enumerate(tables):
                # Skip self-comparisons
                if i == j:
                    continue
                    
                target_table_name = target_table["table_name"]
                target_columns = target_table.get("columns", [])
                
                for source_col in source_columns:
                    source_col_name = source_col.get("column_name") or source_col.get("name")
                    
                    # Skip system columns or those unlikely to be foreign keys
                    if self._should_skip_column(source_col_name, source_col.get("data_type")):
                        continue
                    
                    for target_col in target_columns:
                        target_col_name = target_col.get("column_name") or target_col.get("name")
                        
                        # Skip system columns or those unlikely to be primary/unique keys
                        if self._should_skip_column(target_col_name, target_col.get("data_type")):
                            continue
                        
                        # Check if this is a likely FK-PK pair using naming conventions
                        if self._is_candidate_pair(source_col_name, target_col_name, target_table_name):
                            pairs.append((
                                source_table_name,
                                source_col_name,
                                target_table_name,
                                target_col_name
                            ))
        
        return pairs
    
    def _should_skip_column(self, column_name: str, data_type: Optional[str] = None) -> bool:
        """
        Determine if a column should be skipped in relationship inference.
        
        Args:
            column_name: Column name
            data_type: Column data type
            
        Returns:
            True if column should be skipped, False otherwise
        """
        if not column_name:
            return True
            
        # Skip system columns
        if column_name.startswith('_'):
            return True
            
        # Skip large text columns or complex types
        if data_type and any(t in data_type.upper() for t in ['ARRAY', 'STRUCT', 'TEXT']):
            return True
            
        return False
    
    def _is_candidate_pair(self, source_col: str, target_col: str, target_table: str) -> bool:
        """
        Determine if a column pair is a candidate for relationship inference.
        
        This uses naming conventions to identify likely foreign key relationships.
        
        Args:
            source_col: Source column name
            target_col: Target column name
            target_table: Target table name
            
        Returns:
            True if candidate pair, False otherwise
        """
        # Convert to lowercase for comparison
        source_col = source_col.lower()
        target_col = target_col.lower()
        target_table = target_table.lower()
        
        # Exact match (e.g., "id" to "id")
        if source_col == target_col:
            return True
            
        # FK to PK pattern (e.g., "customer_id" to "id" in "customers")
        if source_col.endswith('_id') and target_col == 'id':
            prefix = source_col[:-3]  # Remove _id
            # Check if prefix matches table name (handling plurals)
            if prefix == target_table or prefix + 's' == target_table or prefix == target_table + 's':
                return True
                
        # Check for other common patterns
        common_relations = [
            ('_fk', ''),
            ('_key', ''),
            ('_ref', ''),
            ('_id', '')
        ]
        
        for suffix, replacement in common_relations:
            if source_col.endswith(suffix):
                base = source_col[:-len(suffix)] + replacement
                if base == target_col or base == target_table:
                    return True
                    
        return False
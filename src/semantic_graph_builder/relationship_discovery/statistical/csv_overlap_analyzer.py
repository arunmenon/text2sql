"""
CSV-based Statistical Overlap Analyzer

Analyzes column value overlaps from CSV files to detect potential relationships between tables.
"""
import logging
import os
import json
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

logger = logging.getLogger(__name__)

class CSVOverlapAnalyzer:
    """Analyzes column value overlaps from CSV files for relationship inference."""
    
    def __init__(self, csv_dir_path: str):
        """
        Initialize the CSV overlap analyzer.
        
        Args:
            csv_dir_path: Path to directory containing CSV files
        """
        self.csv_dir_path = csv_dir_path
        self.dataframes = {}  # Cache for loaded dataframes
        self.table_to_csv_mapping = self._load_table_mapping()
    
    def _load_table_mapping(self) -> Dict[str, str]:
        """
        Load the table-to-CSV mapping from the configuration file.
        
        Returns:
            Dictionary mapping table names to CSV filenames
        """
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))))), "config", "table_to_csv_mapping.json")
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    mapping = json.load(f)
                logger.info(f"Loaded table-to-CSV mapping from {config_path}")
                return mapping
            else:
                logger.warning(f"Config file not found at {config_path}, using default mapping")
                return {}
        except Exception as e:
            logger.error(f"Error loading table-to-CSV mapping: {e}")
            return {}
    
    def _get_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Get or load a dataframe for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame for the specified table
        """
        if table_name in self.dataframes:
            return self.dataframes[table_name]
        
        # Check if the table name is in our configuration mapping
        if table_name in self.table_to_csv_mapping:
            csv_filename = self.table_to_csv_mapping[table_name]
            full_path = os.path.join(self.csv_dir_path, csv_filename)
            
            if os.path.exists(full_path):
                df = pd.read_csv(full_path)
                self.dataframes[table_name] = df
                logger.info(f"Loaded dataframe for {table_name} from {full_path} (using config mapping)")
                return df
        
        # Fallback to standard patterns if mapping doesn't exist or file isn't found
        possible_filenames = [
            os.path.join(self.csv_dir_path, f"{table_name}.csv"),
            os.path.join(self.csv_dir_path, f"{table_name.lower()}.csv"),
            os.path.join(self.csv_dir_path, f"sc_walmart_{table_name.lower()}.csv"),
        ]
        
        for filename in possible_filenames:
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                self.dataframes[table_name] = df
                logger.info(f"Loaded dataframe for {table_name} from {filename}")
                return df
        
        # If no matching file is found, try to find any file containing the table name
        for filename in os.listdir(self.csv_dir_path):
            if table_name.lower() in filename.lower() and filename.endswith('.csv'):
                full_path = os.path.join(self.csv_dir_path, filename)
                df = pd.read_csv(full_path)
                self.dataframes[table_name] = df
                logger.info(f"Loaded dataframe for {table_name} from {full_path} (partial match)")
                return df
        
        # Instead of raising an error, return an empty DataFrame
        logger.warning(f"No CSV file found for table {table_name}. Using empty DataFrame.")
        self.dataframes[table_name] = pd.DataFrame()
        return self.dataframes[table_name]
    
    async def analyze_column_overlap(
        self,
        source_table: str,
        source_column: str,
        target_table: str,
        target_column: str,
        sample_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Analyze the overlap between two columns in CSV files.
        
        Args:
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            sample_size: Optional sample size for large tables (as percentage)
            
        Returns:
            Dictionary with overlap statistics
        """
        try:
            # Get dataframes
            source_df = self._get_dataframe(source_table)
            target_df = self._get_dataframe(target_table)
            
            # Apply sampling if requested
            if sample_size and sample_size < 100:
                source_df = source_df.sample(frac=sample_size/100)
                target_df = target_df.sample(frac=sample_size/100)
            
            # Check if columns exist
            if source_column not in source_df.columns:
                logger.warning(f"Column {source_column} not found in table {source_table}")
                return {
                    "source_distinct": 0,
                    "target_distinct": 0,
                    "overlap_count": 0,
                    "source_overlap_pct": 0.0,
                    "target_overlap_pct": 0.0,
                    "confidence": 0.0
                }
                
            if target_column not in target_df.columns:
                logger.warning(f"Column {target_column} not found in table {target_table}")
                return {
                    "source_distinct": 0,
                    "target_distinct": 0,
                    "overlap_count": 0,
                    "source_overlap_pct": 0.0,
                    "target_overlap_pct": 0.0,
                    "confidence": 0.0
                }
            
            # Convert to string for consistent comparison
            source_df[source_column] = source_df[source_column].astype(str)
            target_df[target_column] = target_df[target_column].astype(str)
            
            # Drop nulls and empty strings
            source_df = source_df[source_df[source_column].str.strip() != ""]
            source_df = source_df.dropna(subset=[source_column])
            target_df = target_df[target_df[target_column].str.strip() != ""]
            target_df = target_df.dropna(subset=[target_column])
            
            # Get distinct values
            source_values = set(source_df[source_column].unique())
            target_values = set(target_df[target_column].unique())
            
            # Calculate overlap
            overlap_values = source_values.intersection(target_values)
            overlap_count = len(overlap_values)
            
            # Calculate stats
            source_distinct = len(source_values)
            target_distinct = len(target_values)
            
            source_overlap_pct = overlap_count / source_distinct if source_distinct > 0 else 0
            target_overlap_pct = overlap_count / target_distinct if target_distinct > 0 else 0
            
            # Calculate confidence (same logic as original)
            confidence = max(source_overlap_pct, target_overlap_pct)
            
            return {
                "source_distinct": source_distinct,
                "target_distinct": target_distinct,
                "overlap_count": overlap_count,
                "source_overlap_pct": source_overlap_pct,
                "target_overlap_pct": target_overlap_pct,
                "confidence": confidence
            }
            
        except Exception as e:
            logger.error(f"Error in CSV column overlap analysis: {e}")
            return {
                "error": str(e),
                "confidence": 0.0
            }
    
    async def find_candidate_relationships(
        self,
        tables: List[Dict],
        min_confidence: float = 0.8,
        sample_size: Optional[int] = None
    ) -> List[Dict]:
        """
        Find candidate relationships across tables by analyzing column overlaps in CSV files.
        
        Args:
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
                    "detection_method": "statistical_overlap_csv"
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
            
        # Skip columns that typically aren't used in relationships
        skip_suffixes = ['desc', 'description', 'comment', 'text', 'name']
        if any(column_name.lower().endswith(suffix) for suffix in skip_suffixes):
            return True
            
        # Skip large text columns or complex types if data_type is provided
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
"""
Prompt Data Context Provider

Provides real data context from CSV files to ground LLM prompts
with actual data examples for more accurate metadata enhancement.
"""
import os
import json
import logging
import pandas as pd
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class PromptDataContextProvider:
    """Provides data samples from CSV files to ground LLM prompts with real examples."""
    
    def __init__(self, csv_dir_path: Optional[str] = None):
        """
        Initialize prompt data context provider.
        
        Args:
            csv_dir_path: Path to directory containing CSV files (optional)
        """
        self.csv_dir_path = csv_dir_path
        self.dataframes = {}  # Cache for loaded dataframes
        
    def get_table_data_context(self, table_name: str, columns: List[Dict], max_rows: int = 5) -> str:
        """
        Get sample data context for a table from CSV files.
        
        Args:
            table_name: Table name
            columns: List of column metadata
            max_rows: Maximum number of sample rows to include
            
        Returns:
            Formatted sample data string for inclusion in prompts
        """
        if not self.csv_dir_path:
            return ""
            
        df = self._get_dataframe(table_name)
        if df is None or df.empty:
            return ""
            
        # Get column names from metadata
        column_names = [col.get("name") or col.get("column_name") for col in columns 
                       if col.get("name") or col.get("column_name")]
        
        # Filter dataframe to only include columns that exist in the table metadata
        existing_cols = [col for col in column_names if col in df.columns]
        
        if not existing_cols:
            return ""
            
        # Select a subset of columns to avoid overwhelming the context
        selected_cols = existing_cols[:min(5, len(existing_cols))]
        sample_df = df[selected_cols].head(max_rows)
        
        # Format as a simple string table
        sample_data = f"Sample Data for {table_name} (first {max_rows} rows):\n"
        
        # Add column headers
        sample_data += " | ".join(selected_cols) + "\n"
        sample_data += "-" * (sum(len(col) + 3 for col in selected_cols)) + "\n"
        
        # Add rows
        for _, row in sample_df.iterrows():
            row_values = []
            for col in selected_cols:
                val = str(row[col])
                # Truncate long values
                if len(val) > 20:
                    val = val[:17] + "..."
                row_values.append(val)
            sample_data += " | ".join(row_values) + "\n"
            
        return sample_data
    
    def get_column_data_examples(self, table_name: str, column_name: str, max_examples: int = 10) -> str:
        """
        Get sample values for a specific column.
        
        Args:
            table_name: Table name
            column_name: Column name
            max_examples: Maximum number of distinct examples to include
            
        Returns:
            Formatted column example values for inclusion in prompts
        """
        if not self.csv_dir_path:
            return ""
            
        df = self._get_dataframe(table_name)
        if df is None or df.empty or column_name not in df.columns:
            return ""
        
        # Get distinct values
        try:
            distinct_values = df[column_name].dropna().unique()
            examples = distinct_values[:min(max_examples, len(distinct_values))]
            
            # Format examples
            examples_str = ", ".join([str(val)[:30] if len(str(val)) <= 30 else f"{str(val)[:27]}..." 
                                     for val in examples])
            
            if examples_str:
                return f"Example values for {column_name}: {examples_str}"
            return ""
            
        except Exception as e:
            logger.error(f"Error getting examples for {table_name}.{column_name}: {e}")
            return ""
    
    def get_column_examples_for_batch(self, table_name: str, columns: List[Dict], max_examples: int = 5) -> str:
        """
        Get sample values for multiple columns.
        
        Args:
            table_name: Table name
            columns: List of column metadata
            max_examples: Maximum number of distinct examples per column
            
        Returns:
            Formatted string with examples for each column
        """
        if not columns:
            return ""
            
        examples_list = []
        for col in columns:
            col_name = col.get("name") or col.get("column_name")
            if col_name:
                examples = self.get_column_data_examples(table_name, col_name, max_examples)
                if examples:
                    examples_list.append(examples)
                    
        if examples_list:
            return "Column Examples:\n" + "\n".join(examples_list)
        return ""
    
    def _get_dataframe(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Get or load a dataframe for a table from CSV files.
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame for the specified table or None if not found
        """
        if not self.csv_dir_path:
            return None
            
        if table_name in self.dataframes:
            return self.dataframes[table_name]
        
        try:
            # First try to load from config/table_to_csv_mapping.json
            mapping_file = os.path.join("config", "table_to_csv_mapping.json")
            if os.path.exists(mapping_file):
                try:
                    with open(mapping_file, 'r') as f:
                        table_mapping = json.load(f)
                    
                    if table_name in table_mapping:
                        csv_filename = table_mapping[table_name]
                        full_path = os.path.join(self.csv_dir_path, csv_filename)
                        if os.path.exists(full_path):
                            df = pd.read_csv(full_path)
                            self.dataframes[table_name] = df
                            logger.info(f"Loaded dataframe for {table_name} from {full_path} (using mapping)")
                            return df
                except Exception as e:
                    logger.warning(f"Error reading table mapping: {e}")
            
            # Try different possible file patterns
            possible_filenames = [
                os.path.join(self.csv_dir_path, f"{table_name}.csv"),
                os.path.join(self.csv_dir_path, f"{table_name.lower()}.csv"),
                os.path.join(self.csv_dir_path, f"sc_walmart_{table_name.lower()}.csv"),
                # Add more patterns if needed
            ]
            
            for filename in possible_filenames:
                if os.path.exists(filename):
                    df = pd.read_csv(filename)
                    self.dataframes[table_name] = df
                    logger.info(f"Loaded dataframe for {table_name} from {filename}")
                    return df
            
            # If no matching file is found, search for any file containing the table name
            for filename in os.listdir(self.csv_dir_path):
                if table_name.lower() in filename.lower() and filename.endswith('.csv'):
                    full_path = os.path.join(self.csv_dir_path, filename)
                    df = pd.read_csv(full_path)
                    self.dataframes[table_name] = df
                    logger.info(f"Loaded dataframe for {table_name} from {full_path}")
                    return df
            
            logger.warning(f"No CSV file found for table {table_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error loading dataframe for {table_name}: {e}")
            return None
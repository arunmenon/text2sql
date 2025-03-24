"""
LLM-based Relationship Analyzer

Uses LLMs to infer relationships between tables based on column names,
table descriptions, sample data, and semantic understanding.
"""
import logging
import asyncio
import os
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Set

from src.llm.client import LLMClient
from src.semantic_graph_builder.utils.prompt_loader import PromptLoader

logger = logging.getLogger(__name__)

class LLMRelationshipAnalyzer:
    """Analyzes schema semantics using LLMs to infer relationships."""
    
    def __init__(self, llm_client: LLMClient, csv_dir_path: Optional[str] = None):
        """
        Initialize LLM relationship analyzer.
        
        Args:
            llm_client: LLM client for inference
            csv_dir_path: Path to directory containing CSV files (optional)
        """
        self.llm_client = llm_client
        self.csv_dir_path = csv_dir_path
        self.dataframes = {}  # Cache for loaded dataframes
        
        # Use the centralized PromptLoader
        self.prompt_loader = PromptLoader()
            
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
            # Map table names to CSV file names (assumes a naming convention)
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
    
    def _get_sample_data(self, table_name: str, columns: List[Dict], max_rows: int = 5) -> str:
        """
        Get sample data for a table from CSV files.
        
        Args:
            table_name: Table name
            columns: List of column metadata
            max_rows: Maximum number of sample rows to include
            
        Returns:
            Formatted sample data string
        """
        if not self.csv_dir_path:
            return ""
            
        df = self._get_dataframe(table_name)
        if df is None or df.empty:
            return ""
            
        # Get column names from metadata
        column_names = [col.get("name") for col in columns]
        
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
        
    async def _analyze_column_overlap(self, source_table: Dict, target_table: Dict) -> str:
        """
        Analyze column value overlaps between two tables.
        
        Args:
            source_table: Source table metadata
            target_table: Target table metadata
            
        Returns:
            Formatted data overlap analysis string
        """
        if not self.csv_dir_path:
            return ""
            
        source_name = source_table["table_name"]
        target_name = target_table["table_name"]
        
        source_df = self._get_dataframe(source_name)
        target_df = self._get_dataframe(target_name)
        
        if source_df is None or target_df is None:
            return ""
            
        overlap_results = []
        
        # Get candidate column pairs for analysis
        for source_col in source_table["columns"]:
            source_col_name = source_col["name"]
            
            if source_col_name not in source_df.columns:
                continue
                
            for target_col in target_table["columns"]:
                target_col_name = target_col["name"]
                
                if target_col_name not in target_df.columns:
                    continue
                    
                # Skip system columns or those unlikely to be in relationships
                if any(pattern in source_col_name.lower() for pattern in ["created", "updated", "timestamp"]):
                    continue
                    
                if any(pattern in target_col_name.lower() for pattern in ["created", "updated", "timestamp"]):
                    continue
                
                try:
                    # Convert to string for consistent comparison
                    source_values = set(source_df[source_col_name].astype(str).dropna().unique())
                    target_values = set(target_df[target_col_name].astype(str).dropna().unique())
                    
                    # Calculate overlap
                    overlap_values = source_values.intersection(target_values)
                    overlap_count = len(overlap_values)
                    
                    # Only include if there's meaningful overlap
                    if overlap_count > 0:
                        source_distinct = len(source_values)
                        target_distinct = len(target_values)
                        
                        source_overlap_pct = overlap_count / source_distinct if source_distinct > 0 else 0
                        target_overlap_pct = overlap_count / target_distinct if target_distinct > 0 else 0
                        
                        # Calculate confidence
                        confidence = max(source_overlap_pct, target_overlap_pct)
                        
                        # Only include if confidence is reasonable
                        if confidence > 0.05:  # At least 5% overlap
                            overlap_results.append({
                                "source_column": source_col_name,
                                "target_column": target_col_name,
                                "overlap_count": overlap_count,
                                "source_distinct": source_distinct,
                                "target_distinct": target_distinct,
                                "source_overlap_pct": source_overlap_pct,
                                "target_overlap_pct": target_overlap_pct,
                                "confidence": confidence,
                                "sample_values": list(overlap_values)[:3]  # Include a few sample values
                            })
                except Exception as e:
                    logger.error(f"Error analyzing column overlap: {e}")
        
        if not overlap_results:
            return ""
            
        # Sort by confidence in descending order
        overlap_results.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Format the overlap analysis as text
        overlap_text = "Data Overlap Analysis:\n"
        for result in overlap_results[:3]:  # Only include top 3 results
            overlap_text += (
                f"- {source_name}.{result['source_column']} and {target_name}.{result['target_column']}:\n"
                f"  - Overlap: {result['overlap_count']} values\n"
                f"  - Source Coverage: {result['source_overlap_pct']:.2%} of distinct values\n"
                f"  - Target Coverage: {result['target_overlap_pct']:.2%} of distinct values\n"
                f"  - Sample overlapping values: {', '.join(str(v) for v in result['sample_values'])}\n"
            )
            
        return overlap_text
        
    async def infer_relationships(self, tables: List[Dict], min_confidence: float = 0.7) -> List[Dict]:
        """
        Infer relationships using LLM semantic analysis.
        
        Args:
            tables: List of table metadata dictionaries
            min_confidence: Minimum confidence threshold for relationships
            
        Returns:
            List of inferred relationships
        """
        relationships = []
        
        # Preprocess tables for analysis
        processed_tables = self._preprocess_tables(tables)
        
        # For each potential table pair, analyze relationship
        for i, source_table in enumerate(processed_tables):
            for j, target_table in enumerate(processed_tables):
                # Skip self-relationships 
                if i == j:
                    continue
                    
                # Only consider certain pairs to reduce API calls
                if not self._should_analyze_pair(source_table, target_table):
                    continue
                
                # Analyze the table pair with LLM
                pair_relationships = await self._analyze_table_pair(
                    source_table, target_table, min_confidence
                )
                
                if pair_relationships:
                    relationships.extend(pair_relationships)
                    
        return relationships
    
    def _preprocess_tables(self, tables: List[Dict]) -> List[Dict]:
        """
        Preprocess tables for analysis.
        
        Args:
            tables: List of table metadata
            
        Returns:
            Processed tables with normalized structure
        """
        processed = []
        
        for table in tables:
            table_name = table.get("table_name")
            if not table_name:
                continue
                
            columns = table.get("columns", [])
            description = table.get("description", "")
            
            # Extract key column information for context
            column_info = []
            for col in columns:
                col_name = col.get("column_name") or col.get("name")
                if col_name:
                    data_type = col.get("data_type", "unknown")
                    col_desc = col.get("description", "")
                    column_info.append({
                        "name": col_name,
                        "data_type": data_type,
                        "description": col_desc
                    })
            
            processed.append({
                "table_name": table_name,
                "description": description,
                "columns": column_info
            })
            
        return processed
    
    def _should_analyze_pair(self, source_table: Dict, target_table: Dict) -> bool:
        """
        Determine if a table pair should be analyzed based on heuristics.
        
        Args:
            source_table: Source table metadata
            target_table: Target table metadata
            
        Returns:
            True if pair should be analyzed, False otherwise
        """
        source_name = source_table["table_name"].lower()
        target_name = target_table["table_name"].lower()
        
        # Check for potential entity-child relationship in names
        if source_name in target_name or target_name in source_name:
            return True
            
        # Check for ID columns that might suggest relationships
        source_has_id = any("id" in col["name"].lower() for col in source_table["columns"])
        target_has_id = any("id" in col["name"].lower() for col in target_table["columns"])
        
        if source_has_id and target_has_id:
            return True
            
        # Check for columns with similar names
        source_cols = [col["name"].lower() for col in source_table["columns"]]
        target_cols = [col["name"].lower() for col in target_table["columns"]]
        
        for s_col in source_cols:
            for t_col in target_cols:
                if (s_col in t_col or t_col in s_col) and any(k in t_col for k in ["id", "key", "code", "ref"]):
                    return True
                    
        return False
        
    async def _analyze_table_pair(
        self, source_table: Dict, target_table: Dict, min_confidence: float
    ) -> List[Dict]:
        """
        Analyze a pair of tables for potential relationships using LLM.
        
        Args:
            source_table: Source table metadata
            target_table: Target table metadata
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of inferred relationships
        """
        # Build LLM prompt with sample data and column overlap analysis
        prompt = await self._build_relationship_prompt(source_table, target_table)
        
        # Define expected schema for structured output
        schema = {
            "relationships": [
                {
                    "source_table": "string",
                    "source_column": "string",
                    "target_table": "string",
                    "target_column": "string",
                    "relationship_type": "string",  # one-to-many, many-to-many, etc.
                    "confidence": "number",
                    "explanation": "string"
                }
            ]
        }
        
        try:
            # Get structured response from LLM
            response = await self.llm_client.generate_structured_output(prompt, schema)
            
            # Filter relationships by confidence
            relationships = []
            
            for rel in response.get("relationships", []):
                if rel.get("confidence", 0) >= min_confidence:
                    relationships.append({
                        "source_table": rel["source_table"],
                        "source_column": rel["source_column"],
                        "target_table": rel["target_table"],
                        "target_column": rel["target_column"],
                        "confidence": rel["confidence"],
                        "relationship_type": rel["relationship_type"],
                        "detection_method": "llm_semantic",
                        "explanation": rel.get("explanation", "")
                    })
                    
            return relationships
            
        except Exception as e:
            logger.error(f"Error analyzing table pair {source_table['table_name']} and {target_table['table_name']}: {e}")
            return []
    
    async def _build_relationship_prompt(self, source_table: Dict, target_table: Dict) -> str:
        """
        Build prompt for relationship analysis.
        
        Args:
            source_table: Source table metadata
            target_table: Target table metadata
            
        Returns:
            Prompt for LLM
        """
        # Format source table columns
        source_columns = "\n".join([
            f"- {col['name']} ({col['data_type']}): {col['description']}"
            for col in source_table["columns"]
        ])
        
        # Format target table columns
        target_columns = "\n".join([
            f"- {col['name']} ({col['data_type']}): {col['description']}"
            for col in target_table["columns"]
        ])
        
        # Get sample data if available
        source_sample_data = ""
        target_sample_data = ""
        data_overlap_analysis = ""
        
        if self.csv_dir_path:
            source_sample_data = self._get_sample_data(source_table["table_name"], source_table["columns"])
            target_sample_data = self._get_sample_data(target_table["table_name"], target_table["columns"])
            
            # Get data overlap analysis
            data_overlap_analysis = await self._analyze_column_overlap(source_table, target_table)
        
        # Use the PromptLoader to load and format the prompt
        try:
            # Format variables to pass to the prompt template
            prompt_vars = {
                "source_table_name": source_table["table_name"],
                "source_table_description": source_table.get("description", ""),
                "source_columns": source_columns,
                "source_sample_data": source_sample_data,
                
                "target_table_name": target_table["table_name"],
                "target_table_description": target_table.get("description", ""),
                "target_columns": target_columns,
                "target_sample_data": target_sample_data,
                
                "data_overlap_analysis": data_overlap_analysis
            }
            
            # Use the standard prompt loader to format the prompt
            prompt = self.prompt_loader.format_prompt("relationship_analyzer", **prompt_vars)
            return prompt
            
        except Exception as e:
            logger.error(f"Error formatting relationship prompt: {e}")
            
            # Fall back to a default prompt if the loader fails
            logger.warning("Using fallback relationship prompt")
            prompt = f"""
            Analyze the following two database tables and identify potential relationships between them.
            
            Source Table: {source_table['table_name']}
            Description: {source_table['description']}
            Columns:
            {source_columns}
            """
            
            if source_sample_data:
                prompt += f"\n{source_sample_data}\n"
            
            prompt += f"""
            Target Table: {target_table['table_name']}
            Description: {target_table['description']}
            Columns:
            {target_columns}
            """
            
            if target_sample_data:
                prompt += f"\n{target_sample_data}\n"
                
            if data_overlap_analysis:
                prompt += f"\n{data_overlap_analysis}\n"
            
            prompt += """
            Based on the table and column names, data types, descriptions, and sample data, identify potential foreign key relationships.
            Consider standard database naming conventions, semantic relationships, and business logic.
            
            Focus on these types of relationships:
            1. Primary key to foreign key (e.g., customers.id → orders.customer_id)
            2. Lookup relationships (e.g., products.category_code → categories.code)
            3. Association/junction table relationships (for many-to-many)
            
            For each potential relationship:
            1. Specify the source and target tables and columns
            2. Indicate relationship type (one-to-one, one-to-many, many-to-many)
            3. Assign a confidence score (0.0-1.0)
            4. Provide a detailed explanation for the relationship
            
            Only return relationships with meaningful business value. Ignore implementation details like created_at/updated_at columns.
            """
            
            return prompt
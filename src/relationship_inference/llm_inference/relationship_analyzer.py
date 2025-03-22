"""
LLM-based Relationship Analyzer

Uses LLMs to infer relationships between tables based on column names,
table descriptions, and semantic understanding.
"""
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple

from src.llm.client import LLMClient

logger = logging.getLogger(__name__)

class LLMRelationshipAnalyzer:
    """Analyzes schema semantics using LLMs to infer relationships."""
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize LLM relationship analyzer.
        
        Args:
            llm_client: LLM client for inference
        """
        self.llm_client = llm_client
        
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
        # Build LLM prompt
        prompt = self._build_relationship_prompt(source_table, target_table)
        
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
    
    def _build_relationship_prompt(self, source_table: Dict, target_table: Dict) -> str:
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
        
        return f"""
        Analyze the following two database tables and identify potential relationships between them.
        
        Source Table: {source_table['table_name']}
        Description: {source_table['description']}
        Columns:
        {source_columns}
        
        Target Table: {target_table['table_name']}
        Description: {target_table['description']}
        Columns:
        {target_columns}
        
        Based on the table and column names, data types, and descriptions, identify potential foreign key relationships.
        Consider standard database naming conventions, semantic relationships, and business logic.
        
        Focus on these types of relationships:
        1. Primary key to foreign key (e.g., customers.id → orders.customer_id)
        2. Lookup relationships (e.g., products.category_code → categories.code)
        3. Association/junction table relationships (for many-to-many)
        
        For each potential relationship:
        1. Specify the source and target tables and columns
        2. Indicate relationship type (one-to-one, one-to-many, many-to-many)
        3. Assign a confidence score (0.0-1.0)
        4. Provide a brief explanation for the relationship
        
        Only return relationships with meaningful business value. Ignore implementation details like created_at/updated_at columns.
        """
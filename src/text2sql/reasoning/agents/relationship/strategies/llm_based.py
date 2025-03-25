"""
LLM-based strategy for join path discovery.

This module implements a strategy for discovering join paths using
a language model when other strategies fail.
"""

from typing import Dict, Any
import logging

from src.text2sql.reasoning.agents.relationship.base import JoinPathStrategy
from src.text2sql.reasoning.registry import StrategyRegistry
from src.llm.client import LLMClient
from src.text2sql.utils.prompt_loader import PromptLoader


@StrategyRegistry.register("llm_based_join")
class LLMBasedJoinStrategy(JoinPathStrategy):
    """Strategy for finding relationships using LLM."""
    
    def __init__(self, graph_context_provider, llm_client, prompt_loader, **kwargs):
        """
        Initialize LLM-based join strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
            llm_client: LLM client for text generation
            prompt_loader: Loader for prompt templates
            **kwargs: Additional configuration parameters
        """
        super().__init__(graph_context_provider)
        self.llm_client = llm_client
        self.prompt_loader = prompt_loader
        self.prompt_template = kwargs.get("prompt_template", "relationship_discovery")
        self.confidence_threshold = kwargs.get("confidence_threshold", 0.4)
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Uses language model to discover potential join paths based on schema semantics"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships using LLM.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        query = context.get("query", "")
        
        # Get schema context for tables
        schema_context = self.graph_context.get_schema_context(tenant_id)
        
        # Format tables info for the prompt
        tables_info = self._format_tables_info(schema_context, source_table, target_table)
        
        # Create prompt for LLM resolution
        prompt = self.prompt_loader.format_prompt(
            self.prompt_template,
            query=query,
            source_table=source_table,
            target_table=target_table,
            tables_info=tables_info
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "join_path": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "from_table": {"type": "string"},
                            "from_column": {"type": "string"},
                            "to_table": {"type": "string"},
                            "to_column": {"type": "string"},
                            "relationship_type": {"type": "string"}
                        }
                    }
                },
                "confidence": {"type": "number"},
                "reasoning": {"type": "string"}
            }
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "join_path" in response and response["join_path"]:
                join_path = {
                    "path": response["join_path"],
                    "confidence": response.get("confidence", 0.6),
                    "reasoning": response.get("reasoning", "")
                }
                
                # Only return if confidence meets threshold
                if join_path["confidence"] >= self.confidence_threshold:
                    return {
                        "source_table": source_table,
                        "target_table": target_table,
                        "join_path": join_path,
                        "confidence": join_path["confidence"],
                        "strategy": self.strategy_name,
                        "alternative_paths": [],
                        "reasoning": join_path["reasoning"]
                    }
        except Exception as e:
            self.logger.error(f"Error in LLM join resolution: {e}")
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }
    
    def _format_tables_info(self, schema_context: Dict[str, Any], source_table: str, target_table: str) -> str:
        """
        Format tables information for LLM prompts.
        
        Args:
            schema_context: Schema context information
            source_table: Source table name
            target_table: Target table name
            
        Returns:
            Formatted string with tables information
        """
        tables_info = []
        tables = schema_context.get("tables", {})
        
        # Add source table info
        if source_table in tables:
            source_info = tables[source_table]
            columns = source_info.get("columns", [])
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in columns
            ])
            
            tables_info.append(f"SOURCE TABLE: {source_table}\n  Description: {source_info.get('description', 'No description')}\n  Columns:\n    {column_str}")
        
        # Add target table info
        if target_table in tables:
            target_info = tables[target_table]
            columns = target_info.get("columns", [])
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in columns
            ])
            
            tables_info.append(f"TARGET TABLE: {target_table}\n  Description: {target_info.get('description', 'No description')}\n  Columns:\n    {column_str}")
        
        # Add intermediate tables that might be relevant
        # Look for tables with foreign keys to both source and target
        intermediate_tables = []
        for table_name, table_info in tables.items():
            if table_name not in [source_table, target_table]:
                fks = table_info.get("foreign_keys", [])
                refs_source = any(fk.get("referenced_table") == source_table for fk in fks)
                refs_target = any(fk.get("referenced_table") == target_table for fk in fks)
                
                if refs_source or refs_target:
                    intermediate_tables.append(table_name)
        
        # Add up to 3 intermediate tables
        for table_name in intermediate_tables[:3]:
            table_info = tables[table_name]
            columns = table_info.get("columns", [])
            
            key_columns = [col for col in columns if "key" in col.get("name", "").lower() or "id" in col.get("name", "").lower()]
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in key_columns[:5]  # Only include key columns
            ])
            
            tables_info.append(f"INTERMEDIATE TABLE: {table_name}\n  Description: {table_info.get('description', 'No description')}\n  Key Columns:\n    {column_str}")
        
        return "\n\n".join(tables_info)
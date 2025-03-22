import json
import logging
import re
from typing import Dict, Any, Optional, List, Union

from src.llm.client import LLMClient
from src.text2sql.models import (
    StructuredQuery, ResolvedQuery, QueryInterpretation, SQLResult
)

logger = logging.getLogger(__name__)

class SQLGenerationComponent:
    """Component for generating SQL from resolved queries"""
    
    def __init__(self, llm_client: LLMClient):
        """
        Initialize SQL generation component.
        
        Args:
            llm_client: LLM client for text generation
        """
        self.llm_client = llm_client
        
    async def generate_sql(
        self, resolved_query: ResolvedQuery, structured_query: StructuredQuery
    ) -> List[SQLResult]:
        """
        Generate SQL from the resolved query.
        
        Args:
            resolved_query: Query with components resolved against schema
            structured_query: Original structured query
            
        Returns:
            List of SQL results
        """
        # Handle differently based on ambiguity
        if resolved_query.interpretations and len(resolved_query.interpretations) > 1:
            # Generate SQL for each interpretation
            sql_variants = await self._generate_multiple_variants(
                resolved_query.interpretations,
                structured_query,
                resolved_query.schema_context
            )
            return sql_variants
        else:
            # Generate SQL for single interpretation
            interpretation = resolved_query.interpretations[0] if resolved_query.interpretations else None
            sql = await self._generate_single_sql(
                interpretation,
                structured_query,
                resolved_query.join_paths,
                resolved_query.schema_context
            )
            return [sql]
    
    async def _generate_single_sql(
        self, 
        interpretation: Optional[QueryInterpretation],
        structured_query: StructuredQuery,
        join_paths: Dict[str, Any],
        schema_context: Dict[str, Any]
    ) -> SQLResult:
        """Generate a single SQL query based on interpretation"""
        # Build the prompt for SQL generation
        sql_prompt = self._build_sql_generation_prompt(
            interpretation,
            structured_query,
            join_paths,
            schema_context
        )
        
        # Generate SQL with LLM
        sql_result_text = await self.llm_client.generate(sql_prompt)
        
        # Parse and extract the SQL
        sql_query = self._extract_sql_from_response(sql_result_text)
        
        # Generate explanation
        explanation_prompt = self._build_explanation_prompt(
            sql_query, interpretation, structured_query
        )
        explanation_result = await self.llm_client.generate(explanation_prompt)
        
        # Extract assumptions from the explanation
        assumptions = self._extract_assumptions(explanation_result)
        
        # Create SQL result
        result = SQLResult(
            sql=sql_query,
            explanation=explanation_result,
            assumptions=assumptions,
            approach=self._determine_approach(sql_query, interpretation),
            interpretation=interpretation,
            is_primary=interpretation.is_primary if interpretation else True
        )
        
        return result
    
    async def _generate_multiple_variants(
        self,
        interpretations: List[QueryInterpretation],
        structured_query: StructuredQuery,
        schema_context: Dict[str, Any]
    ) -> List[SQLResult]:
        """Generate multiple SQL variants for different interpretations"""
        variants = []
        
        for interp in interpretations:
            # Create custom join paths dictionary for this interpretation
            # that only includes the tables referenced in this interpretation
            entity_table_names = [entity.table_name for entity in interp.entities.values()]
            
            # Filter join paths to only include relevant tables
            interp_join_paths = {}
            for path_key, path in interp.join_paths.items():
                # Check if path connects tables in this interpretation
                if any(table in path_key for table in entity_table_names):
                    interp_join_paths[path_key] = path
            
            sql = await self._generate_single_sql(
                interp,
                structured_query,
                interp_join_paths,
                schema_context
            )
            
            # Set interpretation and primary flag
            sql.interpretation = interp
            sql.is_primary = interp.is_primary
            
            variants.append(sql)
            
        return variants
    
    def _build_sql_generation_prompt(
        self,
        interpretation: Optional[QueryInterpretation],
        structured_query: StructuredQuery,
        join_paths: Dict[str, Any],
        schema_context: Dict[str, Any]
    ) -> str:
        """Build the prompt for SQL generation"""
        # Get original query
        raw_query = structured_query._meta.raw_query
        
        # Extract tables
        table_names = []
        if interpretation and interpretation.entities:
            table_names = [entity.table_name for entity in interpretation.entities.values()]
        
        # Format tables information
        tables_info = self._format_tables_info(table_names, schema_context)
        
        # Format joins information
        joins_info = self._format_joins_info(join_paths, table_names)
        
        # Format filters
        filters_info = self._format_filters(structured_query.filters)
        
        # Format concepts if available
        concepts_info = ""
        if interpretation and interpretation.concepts:
            concepts = []
            for concept_name, concept in interpretation.concepts.items():
                concepts.append(f"- {concept_name}: {concept.interpretation}")
                if "sql_fragment" in concept.implementation:
                    concepts.append(f"  SQL Implementation: {concept.implementation['sql_fragment']}")
            
            concepts_info = "SEMANTIC CONCEPTS:\n" + "\n".join(concepts)
        
        # Format aggregations
        aggregations_info = self._format_aggregations(structured_query.aggregation_functions)
        
        # Format grouping
        grouping_info = self._format_grouping(structured_query.grouping_dimensions)
        
        # Format sorting
        sorting_info = self._format_sorting(structured_query.sorting_criteria)
        
        # Build the final prompt
        return f"""
        Generate a SQL query based on the following natural language request:
        
        USER QUERY: {raw_query}
        
        INTENT: {structured_query.primary_intent}
        
        {tables_info}
        
        {joins_info}
        
        {filters_info}
        
        {concepts_info}
        
        {aggregations_info}
        
        {grouping_info}
        
        {sorting_info}
        
        LIMIT: {structured_query.limit if structured_query.limit is not None else "None specified"}
        
        Generate a syntactically correct SQL query that addresses the user's intent.
        The SQL should be executable against the described database schema.
        Include comments to explain complex parts of the query.
        Use table aliases where appropriate for readability.
        
        Provide only the SQL query itself, formatted with proper indentation.
        """
    
    def _format_tables_info(self, table_names: List[str], schema_context: Dict[str, Any]) -> str:
        """Format tables information for prompt"""
        tables_info = []
        tables_info.append("TABLES:")
        
        for table_name in table_names:
            if table_name in schema_context["tables"]:
                table_info = schema_context["tables"][table_name]
                description = table_info.get("description", "No description")
                
                # Get columns
                columns = table_info.get("columns", [])
                columns_text = []
                
                for column in columns:
                    col_name = column.get("name") or column.get("column_name")
                    data_type = column.get("data_type", "Unknown")
                    col_desc = column.get("description", "")
                    nullable = "NULL" if column.get("is_nullable", True) else "NOT NULL"
                    
                    if col_name:
                        columns_text.append(f"    - {col_name} ({data_type}, {nullable}): {col_desc}")
                
                tables_info.append(f"- {table_name}: {description}")
                tables_info.append("  Columns:")
                tables_info.extend(columns_text)
        
        return "\n".join(tables_info)
    
    def _format_joins_info(self, join_paths: Dict[str, Any], table_names: List[str]) -> str:
        """Format joins information for prompt"""
        if not join_paths:
            return "JOINS: No explicit join paths identified"
            
        joins_info = ["JOINS:"]
        
        for path_key, path_data in join_paths.items():
            # Skip if not relevant to the selected tables
            if not any(table in path_key for table in table_names):
                continue
                
            if "paths" in path_data and path_data["paths"]:
                path = path_data["paths"][0]  # Take first path
                
                # Extract path components
                if "columns" in path:
                    columns = path["columns"]
                    tables = path_key.split("_to_")
                    
                    if len(tables) == 2 and len(columns) >= 2:
                        source_table, target_table = tables
                        
                        # Format the join condition
                        if len(columns) == 2:
                            # Direct join
                            joins_info.append(f"- {source_table} JOIN {target_table} ON {source_table}.{columns[0]} = {target_table}.{columns[1]}")
                        else:
                            # Indirect join through intermediary tables
                            intermediaries = []
                            for i in range(0, len(columns) - 1, 2):
                                if i + 1 < len(columns):
                                    if i == 0:
                                        # First join
                                        table1 = source_table
                                    else:
                                        # Intermediary table (inferred from column name)
                                        column_parts = columns[i].split(".")
                                        table1 = column_parts[0] if len(column_parts) > 1 else columns[i].split("_")[0]
                                    
                                    if i + 2 >= len(columns):
                                        # Last join
                                        table2 = target_table
                                    else:
                                        # Intermediary table (inferred from column name)
                                        column_parts = columns[i+1].split(".")
                                        table2 = column_parts[0] if len(column_parts) > 1 else columns[i+1].split("_")[0]
                                    
                                    intermediaries.append(f"{table1} JOIN {table2} ON {table1}.{columns[i]} = {table2}.{columns[i+1]}")
                            
                            joins_info.append(f"- Path from {source_table} to {target_table}:")
                            for interm in intermediaries:
                                joins_info.append(f"  * {interm}")
        
        return "\n".join(joins_info)
    
    def _format_filters(self, filters: List[Dict[str, Any]]) -> str:
        """Format filters for prompt"""
        if not filters:
            return "FILTERS: None specified"
            
        filters_info = ["FILTERS:"]
        
        for filter_item in filters:
            field = filter_item.get("field", "")
            operator = filter_item.get("operator", "=")
            value = filter_item.get("value", "")
            
            filters_info.append(f"- {field} {operator} {value}")
        
        return "\n".join(filters_info)
    
    def _format_aggregations(self, aggregations: List[Dict[str, Any]]) -> str:
        """Format aggregations for prompt"""
        if not aggregations:
            return "AGGREGATIONS: None specified"
            
        agg_info = ["AGGREGATIONS:"]
        
        for agg in aggregations:
            func = agg.get("function", "")
            field = agg.get("field", "")
            
            agg_info.append(f"- {func}({field})")
        
        return "\n".join(agg_info)
    
    def _format_grouping(self, grouping: List[str]) -> str:
        """Format grouping for prompt"""
        if not grouping:
            return "GROUPING: None specified"
            
        grouping_info = ["GROUPING:"]
        
        for group in grouping:
            grouping_info.append(f"- {group}")
        
        return "\n".join(grouping_info)
    
    def _format_sorting(self, sorting: List[Dict[str, Any]]) -> str:
        """Format sorting for prompt"""
        if not sorting:
            return "SORTING: None specified"
            
        sorting_info = ["SORTING:"]
        
        for sort in sorting:
            field = sort.get("field", "")
            direction = sort.get("direction", "asc")
            
            sorting_info.append(f"- {field} {direction.upper()}")
        
        return "\n".join(sorting_info)
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL query from LLM response"""
        # Try to find SQL between triple backticks
        sql_pattern = r"```sql\s*([\s\S]*?)\s*```"
        sql_matches = re.findall(sql_pattern, response)
        
        if sql_matches:
            return sql_matches[0].strip()
            
        # If not found with sql tag, try generic code blocks
        generic_pattern = r"```\s*([\s\S]*?)\s*```"
        generic_matches = re.findall(generic_pattern, response)
        
        if generic_matches:
            return generic_matches[0].strip()
            
        # If no code blocks, return the whole response
        return response.strip()
    
    def _build_explanation_prompt(
        self, 
        sql_query: str,
        interpretation: Optional[QueryInterpretation],
        structured_query: StructuredQuery
    ) -> str:
        """Build prompt for generating SQL explanation"""
        raw_query = structured_query._meta.raw_query
        
        return f"""
        Original query: "{raw_query}"
        
        Generated SQL:
        ```sql
        {sql_query}
        ```
        
        Please provide a concise explanation of this SQL query for a business user:
        1. What the query does in simple terms
        2. Key assumptions made during interpretation
        3. How the SQL implements the user's request
        
        Keep the explanation non-technical and focus on the business meaning.
        """
    
    def _extract_assumptions(self, explanation: str) -> List[str]:
        """Extract assumptions from explanation text"""
        assumptions = []
        
        # Look for assumptions section
        assumptions_pattern = r"(?:Assumptions|Key assumptions|Assumptions made):(.*?)(?:\n\n|\n[A-Z]|$)"
        assumptions_match = re.search(assumptions_pattern, explanation, re.IGNORECASE | re.DOTALL)
        
        if assumptions_match:
            # Extract assumptions text
            assumptions_text = assumptions_match.group(1).strip()
            
            # Split by bullet points or numbered items
            items = re.split(r"\n\s*[-*•]|\n\s*\d+\.", assumptions_text)
            
            # Clean up and add to list
            for item in items:
                cleaned = item.strip()
                if cleaned:
                    assumptions.append(cleaned)
        
        # If no structured assumptions found, look for sentences with assumption keywords
        if not assumptions:
            assumption_keywords = ["assume", "assuming", "assumption", "interpret", "considered"]
            sentences = re.split(r'(?<=[.!?])\s+', explanation)
            
            for sentence in sentences:
                if any(keyword in sentence.lower() for keyword in assumption_keywords):
                    assumptions.append(sentence.strip())
        
        return assumptions
    
    def _determine_approach(self, sql_query: str, interpretation: Optional[QueryInterpretation]) -> str:
        """Determine the approach used in SQL generation"""
        # Default approach
        approach = "direct_query"
        
        # Check for common patterns
        if "GROUP BY" in sql_query:
            approach = "aggregation"
        elif "WITH" in sql_query:
            approach = "common_table_expression"
        elif sql_query.count("SELECT") > 1:
            approach = "subquery"
        elif "UNION" in sql_query:
            approach = "union"
        elif "JOIN" in sql_query and sql_query.count("JOIN") > 2:
            approach = "multi_join"
        
        # Add interpretation info if available
        if interpretation and interpretation.rationale:
            approach += f"_{interpretation.rationale.split()[0].lower()}"
        
        return approach
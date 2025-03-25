"""
Column-based attribute resolution strategy.

This module contains a strategy for resolving attributes based on
column name matching.
"""

import re
from typing import Dict, Any, List, Optional

from src.text2sql.reasoning.agents.attribute.base import AttributeResolutionStrategy, AttributeType


class ColumnBasedResolutionStrategy(AttributeResolutionStrategy):
    """Resolve attributes by matching to table columns."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Resolves attributes by matching them to table columns"
    
    async def resolve(self, attribute_type: str, attribute_value: str, 
                   context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve attribute using column mapping.
        
        Args:
            attribute_type: Type of attribute (filter, aggregation, etc.)
            attribute_value: Attribute value to resolve
            context: Resolution context with entities, schema, etc.
            
        Returns:
            Resolution result
        """
        # Get entity schema context from the graph
        entities = context.get("entities", {})
        schema_context = self._get_schema_context(context)
        
        # Initialize the result
        result = {
            "attribute_type": attribute_type,
            "attribute_value": attribute_value,
            "resolved_to": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
        
        # Handle different attribute types
        if attribute_type == AttributeType.FILTER:
            filter_result = self._resolve_filter(attribute_value, entities, schema_context, context)
            result.update(filter_result)
        
        elif attribute_type == AttributeType.AGGREGATION:
            aggregation_result = self._resolve_aggregation(attribute_value, entities, schema_context, context)
            result.update(aggregation_result)
        
        elif attribute_type == AttributeType.GROUPING:
            grouping_result = self._resolve_grouping(attribute_value, entities, schema_context, context)
            result.update(grouping_result)
        
        elif attribute_type == AttributeType.SORTING:
            sorting_result = self._resolve_sorting(attribute_value, entities, schema_context, context)
            result.update(sorting_result)
        
        elif attribute_type == AttributeType.LIMIT:
            limit_result = self._resolve_limit(attribute_value, context)
            result.update(limit_result)
        
        return result
    
    def _get_schema_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema context for entity tables."""
        schema_context = {}
        
        # Get all entities and their table schemas
        entities = context.get("entities", {})
        
        for entity_name, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                table_name = entity_info["resolved_to"]
                
                # Get table schema using the graph context provider
                table_schema = self.graph_context.get_table_schema(table_name)
                
                if table_schema:
                    schema_context[table_name] = table_schema
        
        return schema_context
    
    def _resolve_filter(self, attribute_value: str, entities: Dict[str, Any], 
                     schema_context: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve filter attribute to SQL WHERE clause."""
        # Check if this is an attribute extracted by the extractors
        if isinstance(attribute_value, dict):
            # Already pre-processed by extractor
            operator = attribute_value.get("operator")
            value = attribute_value.get("value")
            column_hint = attribute_value.get("column_hint")
            entity_name = attribute_value.get("entity_name")
            
            if not operator or not value:
                return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
            
            # Get entity's table name
            table_name = None
            if entity_name and entity_name in entities:
                table_name = entities[entity_name].get("resolved_to")
            
            # If we have a column hint, try to match it to a column
            column_name = None
            matched_table = None
            confidence = 0.0
            
            if column_hint:
                for t_name, schema in schema_context.items():
                    for column in schema.get("columns", []):
                        col_name = column.get("name", "")
                        col_desc = column.get("description", "")
                        
                        # Calculate column match score
                        match_score = self._calculate_column_match(column_hint, col_name, col_desc)
                        
                        if match_score > confidence:
                            confidence = match_score
                            column_name = col_name
                            matched_table = t_name
            
            # If no match found by hint, try to determine the most likely column based on data type
            if not column_name:
                # Determine likely data type from operator and value
                likely_type = None
                if operator in ["contains", "starts_with", "ends_with"]:
                    likely_type = "string"
                elif operator in ["before_date", "after_date", "on_date"]:
                    likely_type = "date"
                elif operator in ["greater_than", "less_than", "equal_to", "between"]:
                    if isinstance(value, dict) and "value" in value:
                        if value["value"].isdigit():
                            likely_type = "number"
                
                # Find columns of the right type
                if likely_type:
                    for t_name, schema in schema_context.items():
                        # Prioritize the entity's table if specified
                        if table_name and t_name != table_name:
                            continue
                            
                        for column in schema.get("columns", []):
                            col_name = column.get("name", "")
                            data_type = column.get("data_type", "").lower()
                            
                            # Simple type matching
                            if ((likely_type == "string" and any(t in data_type for t in ["char", "text", "varchar"])) or
                                (likely_type == "date" and any(t in data_type for t in ["date", "time", "timestamp"])) or
                                (likely_type == "number" and any(t in data_type for t in ["int", "float", "double", "decimal", "numeric"]))):
                                
                                # Check if column name contains common keywords relevant to the type
                                relevance_score = 0.7
                                
                                if likely_type == "date" and any(kw in col_name.lower() for kw in ["date", "time", "day", "month", "year"]):
                                    relevance_score = 0.9
                                elif likely_type == "string" and any(kw in col_name.lower() for kw in ["name", "title", "description", "text"]):
                                    relevance_score = 0.8
                                elif likely_type == "number" and any(kw in col_name.lower() for kw in ["count", "amount", "total", "num", "quantity"]):
                                    relevance_score = 0.8
                                
                                if relevance_score > confidence:
                                    confidence = relevance_score
                                    column_name = col_name
                                    matched_table = t_name
            
            # Construct SQL WHERE clause
            if column_name and matched_table:
                # Map operators to SQL
                sql_operators = {
                    "equal_to": "=",
                    "not_equal_to": "!=",
                    "greater_than": ">",
                    "less_than": "<",
                    "greater_equal": ">=",
                    "less_equal": "<=",
                    "contains": "LIKE",
                    "starts_with": "LIKE",
                    "ends_with": "LIKE",
                    "between": "BETWEEN",
                    "before_date": "<",
                    "after_date": ">",
                    "on_date": "="
                }
                
                # Process value based on operator
                sql_value = None
                
                if operator in ["contains", "starts_with", "ends_with"]:
                    text_val = value.get("text", "")
                    if operator == "contains":
                        sql_value = f"'%{text_val}%'"
                    elif operator == "starts_with":
                        sql_value = f"'{text_val}%'"
                    elif operator == "ends_with":
                        sql_value = f"'%{text_val}'"
                
                elif operator == "between":
                    start_val = value.get("start")
                    end_val = value.get("end")
                    if start_val and end_val:
                        sql_value = f"{start_val} AND {end_val}"
                
                elif operator in ["before_date", "after_date", "on_date"]:
                    date_val = value.get("date", "")
                    sql_value = f"'{date_val}'"
                
                elif "value" in value:
                    val = value["value"]
                    # Check if it's numeric
                    if val.isdigit():
                        sql_value = val
                    else:
                        sql_value = f"'{val}'"
                
                if sql_value and operator in sql_operators:
                    sql_op = sql_operators[operator]
                    where_clause = f"{matched_table}.{column_name} {sql_op} {sql_value}"
                    
                    return {
                        "resolved_to": where_clause,
                        "confidence": confidence,
                        "metadata": {
                            "table": matched_table,
                            "column": column_name,
                            "operator": sql_op,
                            "value": sql_value
                        }
                    }
        
        # If we reach here, we couldn't resolve the filter
        return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
    
    def _resolve_aggregation(self, attribute_value: str, entities: Dict[str, Any], 
                         schema_context: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve aggregation attribute to SQL aggregation function."""
        # Check if this is an attribute extracted by the extractors
        if isinstance(attribute_value, dict):
            # Already pre-processed by extractor
            agg_func = attribute_value.get("function")
            target = attribute_value.get("target")
            
            if not agg_func or not target:
                return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
            
            # Find matching column
            column_name = None
            matched_table = None
            confidence = 0.0
            
            for t_name, schema in schema_context.items():
                for column in schema.get("columns", []):
                    col_name = column.get("name", "")
                    col_desc = column.get("description", "")
                    
                    # Calculate target match score
                    match_score = self._calculate_column_match(target, col_name, col_desc)
                    
                    if match_score > confidence:
                        confidence = match_score
                        column_name = col_name
                        matched_table = t_name
            
            # If we can't find a good column match but the target mentions an entity
            if confidence < 0.5:
                for entity_name, entity_info in entities.items():
                    if entity_name.lower() in target.lower():
                        table_name = entity_info.get("resolved_to")
                        
                        if table_name:
                            # For COUNT(*), we just need the table
                            if agg_func == "count":
                                confidence = 0.7
                                matched_table = table_name
                                # For COUNT, we'll use either primary key or *
                                schema = schema_context.get(table_name, {})
                                for column in schema.get("columns", []):
                                    if column.get("is_primary_key"):
                                        column_name = column.get("name")
                                        break
                                
                                if not column_name:
                                    column_name = "*"
            
            # Construct SQL aggregation
            if agg_func and (column_name or (agg_func == "count" and matched_table)):
                # For COUNT(*) case
                if agg_func == "count" and column_name == "*":
                    sql_agg = f"COUNT(*)"
                else:
                    # Normal case with column
                    sql_agg = f"{agg_func.upper()}({matched_table}.{column_name})"
                
                return {
                    "resolved_to": sql_agg,
                    "confidence": confidence,
                    "metadata": {
                        "function": agg_func,
                        "table": matched_table,
                        "column": column_name
                    }
                }
        
        # If we reach here, we couldn't resolve the aggregation
        return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
    
    def _resolve_grouping(self, attribute_value: str, entities: Dict[str, Any], 
                      schema_context: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve grouping attribute to SQL GROUP BY clause."""
        # Check if this is an attribute extracted by the extractors
        if isinstance(attribute_value, dict):
            # Already pre-processed by extractor
            target = attribute_value.get("target")
            entity_name = attribute_value.get("entity_name")
            
            if not target:
                return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
            
            # Get entity's table name if available
            table_name = None
            if entity_name and entity_name in entities:
                table_name = entities[entity_name].get("resolved_to")
            
            # Find matching column
            column_name = None
            matched_table = None
            confidence = 0.0
            
            for t_name, schema in schema_context.items():
                # Prioritize the entity's table if specified
                if table_name and t_name != table_name:
                    continue
                    
                for column in schema.get("columns", []):
                    col_name = column.get("name", "")
                    col_desc = column.get("description", "")
                    
                    # Calculate target match score
                    match_score = self._calculate_column_match(target, col_name, col_desc)
                    
                    if match_score > confidence:
                        confidence = match_score
                        column_name = col_name
                        matched_table = t_name
            
            # If match is poor but entity is specified, try categorical columns
            if confidence < 0.5 and table_name:
                # Look for categorical columns which are good candidates for grouping
                schema = schema_context.get(table_name, {})
                for column in schema.get("columns", []):
                    col_name = column.get("name", "")
                    data_type = column.get("data_type", "").lower()
                    is_categorical = (
                        any(t in data_type for t in ["char", "text", "varchar"]) and
                        any(kw in col_name.lower() for kw in ["type", "category", "status", "state", "level"])
                    )
                    
                    if is_categorical:
                        confidence = 0.6
                        column_name = col_name
                        matched_table = table_name
                        break
            
            # Construct SQL GROUP BY clause
            if column_name and matched_table:
                sql_group_by = f"{matched_table}.{column_name}"
                
                return {
                    "resolved_to": sql_group_by,
                    "confidence": confidence,
                    "metadata": {
                        "table": matched_table,
                        "column": column_name
                    }
                }
        
        # If we reach here, we couldn't resolve the grouping
        return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
    
    def _resolve_sorting(self, attribute_value: str, entities: Dict[str, Any], 
                      schema_context: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve sorting attribute to SQL ORDER BY clause."""
        # Check if this is an attribute extracted by the extractors
        if isinstance(attribute_value, dict):
            # Already pre-processed by extractor
            target = attribute_value.get("target")
            direction = attribute_value.get("direction", "asc")
            
            if not target:
                return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
            
            # Find matching column
            column_name = None
            matched_table = None
            confidence = 0.0
            
            for t_name, schema in schema_context.items():
                for column in schema.get("columns", []):
                    col_name = column.get("name", "")
                    col_desc = column.get("description", "")
                    
                    # Calculate target match score
                    match_score = self._calculate_column_match(target, col_name, col_desc)
                    
                    if match_score > confidence:
                        confidence = match_score
                        column_name = col_name
                        matched_table = t_name
            
            # Construct SQL ORDER BY clause
            if column_name and matched_table:
                sql_order_by = f"{matched_table}.{column_name} {direction.upper()}"
                
                return {
                    "resolved_to": sql_order_by,
                    "confidence": confidence,
                    "metadata": {
                        "table": matched_table,
                        "column": column_name,
                        "direction": direction.upper()
                    }
                }
        
        # If we reach here, we couldn't resolve the sorting
        return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
    
    def _resolve_limit(self, attribute_value: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve limit attribute to SQL LIMIT clause."""
        # Check if this is an attribute extracted by the extractors
        if isinstance(attribute_value, dict):
            # Already pre-processed by extractor
            limit_value = attribute_value.get("value")
            
            if limit_value is None:
                return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
            
            # Construct SQL LIMIT clause
            sql_limit = f"LIMIT {limit_value}"
            
            return {
                "resolved_to": sql_limit,
                "confidence": 0.9,  # High confidence for limit resolution
                "metadata": {
                    "value": limit_value
                }
            }
        
        # If we reach here, we couldn't resolve the limit
        return {"resolved_to": None, "confidence": 0.0, "metadata": {}}
    
    def _calculate_column_match(self, text: str, column_name: str, column_description: str) -> float:
        """
        Calculate match score between text and column.
        
        Args:
            text: Text to match
            column_name: Column name
            column_description: Column description
            
        Returns:
            Match score between 0.0 and 1.0
        """
        text = text.lower()
        col_name = column_name.lower()
        col_desc = column_description.lower() if column_description else ""
        
        # Exact match with column name
        if text == col_name:
            return 0.9
        
        # Partial match with column name
        if text in col_name or col_name in text:
            # Calculate the ratio of overlap
            overlap_ratio = min(len(text), len(col_name)) / max(len(text), len(col_name))
            return 0.7 * overlap_ratio
        
        # Check if any word in text matches column name
        words = re.findall(r'\b\w+\b', text)
        for word in words:
            if word in col_name:
                return 0.6
        
        # Check description match
        if col_desc and text in col_desc:
            return 0.5
        
        # Check if any word in text matches description
        if col_desc:
            for word in words:
                if word in col_desc:
                    return 0.4
        
        return 0.0
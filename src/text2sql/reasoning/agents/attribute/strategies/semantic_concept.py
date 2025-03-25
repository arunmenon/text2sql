"""
Semantic concept attribute resolution strategy.

This module contains a strategy for resolving attributes based on
semantic concepts in the graph.
"""

from typing import Dict, Any, List, Optional

from src.text2sql.reasoning.agents.attribute.base import AttributeResolutionStrategy, AttributeType


class SemanticConceptResolutionStrategy(AttributeResolutionStrategy):
    """Resolve attributes using semantic concepts from the graph."""
    
    @property
    def description(self) -> str:
        """Get strategy description."""
        return "Resolves attributes using semantic concepts from the knowledge graph"
    
    async def resolve(self, attribute_type: str, attribute_value: str, 
                   context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve attribute using semantic concepts.
        
        Args:
            attribute_type: Type of attribute (filter, aggregation, etc.)
            attribute_value: Attribute value to resolve
            context: Resolution context with entities, schema, etc.
            
        Returns:
            Resolution result
        """
        # Initialize the result
        result = {
            "attribute_type": attribute_type,
            "attribute_value": attribute_value,
            "resolved_to": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
        
        # Convert attribute value to string if it's a dict
        attribute_text = attribute_value
        attribute_metadata = {}
        
        if isinstance(attribute_value, dict):
            attribute_metadata = attribute_value.copy()
            attribute_text = attribute_value.get("attribute_value", str(attribute_value))
        
        # Get entity information
        entities = context.get("entities", {})
        
        # Extract the concept name to look up
        concept_name = None
        
        if attribute_type == AttributeType.FILTER:
            # For filters, try to find concept in column hint
            if "column_hint" in attribute_metadata:
                concept_name = attribute_metadata["column_hint"]
            
        elif attribute_type == AttributeType.AGGREGATION:
            # For aggregations, try to find concept in target
            if "target" in attribute_metadata:
                concept_name = attribute_metadata["target"]
            
        elif attribute_type == AttributeType.GROUPING:
            # For groupings, try to find concept in target
            if "target" in attribute_metadata:
                concept_name = attribute_metadata["target"]
            
        elif attribute_type == AttributeType.SORTING:
            # For sortings, try to find concept in target
            if "target" in attribute_metadata:
                concept_name = attribute_metadata["target"]
        
        if not concept_name:
            return result
        
        # Look up the concept in the semantic graph
        concept_info = self._find_semantic_concept(concept_name)
        
        if not concept_info:
            return result
        
        # Map the concept to SQL based on attribute type
        sql_component = None
        confidence = 0.0
        metadata = {}
        
        if attribute_type == AttributeType.FILTER:
            filter_info = self._map_filter_concept(concept_info, attribute_metadata, entities)
            sql_component = filter_info.get("sql")
            confidence = filter_info.get("confidence", 0.0)
            metadata = filter_info.get("metadata", {})
            
        elif attribute_type == AttributeType.AGGREGATION:
            agg_info = self._map_aggregation_concept(concept_info, attribute_metadata, entities)
            sql_component = agg_info.get("sql")
            confidence = agg_info.get("confidence", 0.0)
            metadata = agg_info.get("metadata", {})
            
        elif attribute_type == AttributeType.GROUPING:
            group_info = self._map_grouping_concept(concept_info, attribute_metadata, entities)
            sql_component = group_info.get("sql")
            confidence = group_info.get("confidence", 0.0)
            metadata = group_info.get("metadata", {})
            
        elif attribute_type == AttributeType.SORTING:
            sort_info = self._map_sorting_concept(concept_info, attribute_metadata, entities)
            sql_component = sort_info.get("sql")
            confidence = sort_info.get("confidence", 0.0)
            metadata = sort_info.get("metadata", {})
        
        if sql_component:
            result["resolved_to"] = sql_component
            result["confidence"] = confidence
            result["metadata"] = {
                **metadata,
                "concept": concept_name,
                "concept_type": concept_info.get("type", "unknown")
            }
        
        return result
    
    def _find_semantic_concept(self, concept_name: str) -> Optional[Dict[str, Any]]:
        """
        Find concept in semantic graph.
        
        Args:
            concept_name: Concept to look up
            
        Returns:
            Concept information if found, None otherwise
        """
        # Look for business glossary terms that match the concept
        glossary_terms = self.graph_context.get_business_terms()
        
        for term_name, term_info in glossary_terms.items():
            # Check for exact or partial match
            if concept_name.lower() == term_name.lower() or concept_name.lower() in term_name.lower():
                return {
                    "name": term_name,
                    "type": "business_term",
                    "definition": term_info.get("definition", ""),
                    "mapped_tables": term_info.get("mapped_tables", []),
                    "mapped_columns": term_info.get("mapped_columns", []),
                    "confidence": 0.8 if concept_name.lower() == term_name.lower() else 0.6
                }
        
        # If not found in business glossary, look for domain concepts
        domain_concepts = self.graph_context.get_domain_concepts()
        
        for concept, concept_info in domain_concepts.items():
            if concept_name.lower() == concept.lower() or concept_name.lower() in concept.lower():
                return {
                    "name": concept,
                    "type": "domain_concept",
                    "mapped_tables": concept_info.get("mapped_tables", []),
                    "mapped_columns": concept_info.get("mapped_columns", []),
                    "confidence": 0.7 if concept_name.lower() == concept.lower() else 0.5
                }
        
        return None
    
    def _map_filter_concept(self, concept_info: Dict[str, Any], 
                         attribute_metadata: Dict[str, Any],
                         entities: Dict[str, Any]) -> Dict[str, Any]:
        """Map concept to SQL filter."""
        # Get mapped columns from concept
        mapped_columns = concept_info.get("mapped_columns", [])
        
        if not mapped_columns:
            return {"sql": None, "confidence": 0.0}
        
        # Get operator and value from metadata
        operator = attribute_metadata.get("operator")
        value = attribute_metadata.get("value")
        
        if not operator or not value:
            return {"sql": None, "confidence": 0.0}
        
        # Find the best column match
        best_column = None
        best_table = None
        
        # First try to find column in context of specified entity
        entity_name = attribute_metadata.get("entity_name")
        if entity_name and entity_name in entities:
            table_name = entities[entity_name].get("resolved_to")
            if table_name:
                for col_mapping in mapped_columns:
                    if col_mapping.get("table") == table_name:
                        best_column = col_mapping.get("column")
                        best_table = table_name
                        break
        
        # If no match found, use the first mapped column
        if not best_column and mapped_columns:
            best_column = mapped_columns[0].get("column")
            best_table = mapped_columns[0].get("table")
        
        if not best_column or not best_table:
            return {"sql": None, "confidence": 0.0}
        
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
            if str(val).isdigit():
                sql_value = val
            else:
                sql_value = f"'{val}'"
        
        if sql_value and operator in sql_operators:
            sql_op = sql_operators[operator]
            where_clause = f"{best_table}.{best_column} {sql_op} {sql_value}"
            
            return {
                "sql": where_clause,
                "confidence": concept_info.get("confidence", 0.0) * 0.9,  # Slight penalty for semantic mapping
                "metadata": {
                    "table": best_table,
                    "column": best_column,
                    "operator": sql_op,
                    "value": sql_value
                }
            }
        
        return {"sql": None, "confidence": 0.0}
    
    def _map_aggregation_concept(self, concept_info: Dict[str, Any], 
                             attribute_metadata: Dict[str, Any],
                             entities: Dict[str, Any]) -> Dict[str, Any]:
        """Map concept to SQL aggregation."""
        # Get mapped columns from concept
        mapped_columns = concept_info.get("mapped_columns", [])
        
        if not mapped_columns:
            return {"sql": None, "confidence": 0.0}
        
        # Get aggregation function from metadata
        agg_func = attribute_metadata.get("function")
        
        if not agg_func:
            return {"sql": None, "confidence": 0.0}
        
        # Find the best column match
        best_column = None
        best_table = None
        
        # If we're counting, prefer primary keys or unique identifiers
        if agg_func == "count":
            for col_mapping in mapped_columns:
                col_name = col_mapping.get("column", "").lower()
                if "id" in col_name or "key" in col_name:
                    best_column = col_mapping.get("column")
                    best_table = col_mapping.get("table")
                    break
        
        # If no match found or not count, use numeric columns for other aggregations
        if not best_column and agg_func in ["sum", "avg", "min", "max"]:
            # Try to find numeric columns
            for col_mapping in mapped_columns:
                table = col_mapping.get("table")
                column = col_mapping.get("column")
                
                # Get column data type
                data_type = self.graph_context.get_column_data_type(table, column)
                
                if data_type and any(t in data_type.lower() for t in ["int", "float", "decimal", "double", "numeric"]):
                    best_column = column
                    best_table = table
                    break
        
        # If still no match, use the first mapped column
        if not best_column and mapped_columns:
            best_column = mapped_columns[0].get("column")
            best_table = mapped_columns[0].get("table")
        
        if not best_column or not best_table:
            return {"sql": None, "confidence": 0.0}
        
        # For COUNT(*) case
        if agg_func == "count" and not best_column:
            sql_agg = f"COUNT(*)"
        else:
            # Normal case with column
            sql_agg = f"{agg_func.upper()}({best_table}.{best_column})"
        
        return {
            "sql": sql_agg,
            "confidence": concept_info.get("confidence", 0.0) * 0.9,
            "metadata": {
                "function": agg_func,
                "table": best_table,
                "column": best_column
            }
        }
    
    def _map_grouping_concept(self, concept_info: Dict[str, Any], 
                          attribute_metadata: Dict[str, Any],
                          entities: Dict[str, Any]) -> Dict[str, Any]:
        """Map concept to SQL grouping."""
        # Get mapped columns from concept
        mapped_columns = concept_info.get("mapped_columns", [])
        
        if not mapped_columns:
            return {"sql": None, "confidence": 0.0}
        
        # Find the best column match
        best_column = None
        best_table = None
        
        # First try to find column in context of specified entity
        entity_name = attribute_metadata.get("entity_name")
        if entity_name and entity_name in entities:
            table_name = entities[entity_name].get("resolved_to")
            if table_name:
                for col_mapping in mapped_columns:
                    if col_mapping.get("table") == table_name:
                        best_column = col_mapping.get("column")
                        best_table = table_name
                        break
        
        # If no match found, prefer categorical columns
        if not best_column:
            for col_mapping in mapped_columns:
                table = col_mapping.get("table")
                column = col_mapping.get("column")
                
                # Get column info
                data_type = self.graph_context.get_column_data_type(table, column)
                column_name = column.lower() if column else ""
                
                # Check if it's categorical
                is_categorical = (
                    data_type and any(t in data_type.lower() for t in ["char", "text", "varchar"]) and
                    any(kw in column_name for kw in ["type", "category", "status", "state", "level"])
                )
                
                if is_categorical:
                    best_column = column
                    best_table = table
                    break
        
        # If still no match, use the first mapped column
        if not best_column and mapped_columns:
            best_column = mapped_columns[0].get("column")
            best_table = mapped_columns[0].get("table")
        
        if not best_column or not best_table:
            return {"sql": None, "confidence": 0.0}
        
        sql_group_by = f"{best_table}.{best_column}"
        
        return {
            "sql": sql_group_by,
            "confidence": concept_info.get("confidence", 0.0) * 0.9,
            "metadata": {
                "table": best_table,
                "column": best_column
            }
        }
    
    def _map_sorting_concept(self, concept_info: Dict[str, Any], 
                         attribute_metadata: Dict[str, Any],
                         entities: Dict[str, Any]) -> Dict[str, Any]:
        """Map concept to SQL sorting."""
        # Get mapped columns from concept
        mapped_columns = concept_info.get("mapped_columns", [])
        
        if not mapped_columns:
            return {"sql": None, "confidence": 0.0}
        
        # Get direction from metadata
        direction = attribute_metadata.get("direction", "asc")
        
        # Find the best column match
        best_column = None
        best_table = None
        
        # First try to find column in context of specified entity
        entity_name = attribute_metadata.get("entity_name")
        if entity_name and entity_name in entities:
            table_name = entities[entity_name].get("resolved_to")
            if table_name:
                for col_mapping in mapped_columns:
                    if col_mapping.get("table") == table_name:
                        best_column = col_mapping.get("column")
                        best_table = table_name
                        break
        
        # If no match found, use the first mapped column
        if not best_column and mapped_columns:
            best_column = mapped_columns[0].get("column")
            best_table = mapped_columns[0].get("table")
        
        if not best_column or not best_table:
            return {"sql": None, "confidence": 0.0}
        
        sql_order_by = f"{best_table}.{best_column} {direction.upper()}"
        
        return {
            "sql": sql_order_by,
            "confidence": concept_info.get("confidence", 0.0) * 0.9,
            "metadata": {
                "table": best_table,
                "column": best_column,
                "direction": direction.upper()
            }
        }
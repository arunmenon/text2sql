"""
Formatting utilities for schema data and business terms.
"""

import logging
from typing import Dict, List, Any, Union

logger = logging.getLogger(__name__)

def format_schema(schema_data: Union[Dict[str, Any], str]) -> str:
    """
    Format schema data for inclusion in prompts.
    
    Args:
        schema_data: Database schema information
        
    Returns:
        Formatted schema text
    """
    formatted = []
    
    # Handle if schema_data is already a string
    if isinstance(schema_data, str):
        logger.warning("schema_data is already a string, returning as is")
        return schema_data
    
    try:
        # Handle the case where schema_data has 'tables' key (DirectEnhancementWorkflow format)
        if "tables" in schema_data and isinstance(schema_data["tables"], list):
            for table_info in schema_data["tables"]:
                if not isinstance(table_info, dict):
                    continue
                    
                table_name = table_info.get("table", "") or table_info.get("name", "")
                description = table_info.get("description", "No description available")
                formatted.append(f"Table: {table_name} - {description}")
                
                columns = table_info.get("columns", [])
                if isinstance(columns, list):
                    for column in columns:
                        if not isinstance(column, dict):
                            continue
                            
                        col_name = column.get("column_name", "") or column.get("name", "")
                        col_type = column.get("data_type", "")
                        col_desc = column.get("description", "")
                        
                        formatted.append(f"  Column: {col_name} ({col_type}) - {col_desc}")
        # Handle the case where schema_data is a dictionary with table names as keys
        elif isinstance(schema_data, dict):
            for table_name, table_info in schema_data.items():
                if not isinstance(table_info, dict):
                    continue
                    
                description = table_info.get("description", "No description available")
                formatted.append(f"Table: {table_name} - {description}")
                
                if "columns" in table_info and isinstance(table_info["columns"], list):
                    for column in table_info["columns"]:
                        if not isinstance(column, dict):
                            continue
                            
                        col_name = column.get("name", "")
                        col_type = column.get("data_type", "")
                        col_desc = column.get("description", "")
                        
                        formatted.append(f"  Column: {col_name} ({col_type}) - {col_desc}")
        else:
            logger.warning(f"Unrecognized schema format: {type(schema_data)}")
            return "Schema format unrecognized"
    
    except Exception as e:
        logger.error(f"Error formatting schema: {e}")
        return "Error formatting schema"
    
    return "\n".join(formatted)

def format_terms(terms: Union[Dict[str, Any], str]) -> str:
    """
    Format business terms for inclusion in prompts.
    
    Args:
        terms: Business terms dictionary
        
    Returns:
        Formatted terms text
    """
    # Handle if terms is already a string
    if isinstance(terms, str):
        logger.warning("terms is already a string, returning as is")
        return terms
        
    formatted = []
    
    try:
        business_terms = []
        if isinstance(terms, dict) and "business_terms" in terms:
            business_terms = terms.get("business_terms", [])
            
        if not isinstance(business_terms, list):
            logger.warning("business_terms is not a list")
            return "No business terms found"
            
        for term in business_terms:
            if not isinstance(term, dict):
                continue
                
            name = term.get("name", "")
            definition = term.get("definition", "")
            
            formatted.append(f"Term: {name}")
            formatted.append(f"Definition: {definition}")
            
            # Format technical mappings
            tech_mapping = term.get("technical_mapping", {})
            if not isinstance(tech_mapping, dict):
                continue
                
            tables = tech_mapping.get("tables", [])
            columns = tech_mapping.get("columns", [])
            
            if tables and isinstance(tables, list):
                formatted.append(f"Tables: {', '.join(tables)}")
            
            if columns and isinstance(columns, list):
                col_items = []
                for col in columns:
                    if isinstance(col, dict) and "table" in col and "column" in col:
                        col_items.append(f"{col['table']}.{col['column']}")
                if col_items:
                    formatted.append(f"Columns: {', '.join(col_items)}")
            
            # Format synonyms
            synonyms = term.get("synonyms", [])
            if synonyms and isinstance(synonyms, list):
                formatted.append(f"Synonyms: {', '.join(synonyms)}")
            
            formatted.append("")  # Empty line between terms
    except Exception as e:
        logger.error(f"Error formatting terms: {e}")
        return "Error formatting terms"
    
    return "\n".join(formatted)
"""
DDL Parser

Parses BigQuery DDL statements to extract table structure.
"""
import logging
from typing import Dict, List, Optional, Union

import sqlglot
from sqlglot import exp, parse
from sqlglot.dialects.bigquery import BigQuery

logger = logging.getLogger(__name__)

class DDLParser:
    """Parser for BigQuery DDL statements."""
    
    def __init__(self):
        """Initialize the DDL parser."""
        pass
    
    def parse_ddl(self, ddl: str) -> Dict:
        """
        Parse a DDL statement and extract table structure.
        
        Args:
            ddl: CREATE TABLE DDL statement
            
        Returns:
            Dictionary with parsed table structure
        """
        try:
            parsed = parse(ddl, read=BigQuery)
            
            if not parsed or not parsed[0]:
                logger.error("Failed to parse DDL: empty result")
                return {"error": "Failed to parse DDL"}
            
            stmt = parsed[0]
            
            # Only handle CREATE TABLE statements
            if not isinstance(stmt, exp.Create) or stmt.kind != "TABLE":
                logger.error(f"Not a CREATE TABLE statement: {stmt.kind}")
                return {"error": f"Not a CREATE TABLE statement: {stmt.kind}"}
            
            result = {
                "table_name": self._extract_table_name(stmt),
                "columns": self._extract_columns(stmt),
                "primary_key": self._extract_primary_key(stmt),
                "partitioning": self._extract_partitioning(stmt),
                "clustering": self._extract_clustering(stmt),
                "options": self._extract_options(stmt),
            }
            
            return result
        except Exception as e:
            logger.error(f"Error parsing DDL: {e}")
            return {"error": str(e)}
    
    def _extract_table_name(self, stmt: exp.Create) -> str:
        """Extract table name from CREATE TABLE statement."""
        if isinstance(stmt.this, exp.Table):
            parts = []
            if stmt.this.db:
                parts.append(stmt.this.db)
            if stmt.this.catalog:
                parts.append(stmt.this.catalog)
            if stmt.this.name:
                parts.append(stmt.this.name)
            
            if parts:
                return '.'.join(parts)
        
        # Fallback to raw text
        return str(stmt.this)
    
    def _extract_columns(self, stmt: exp.Create) -> List[Dict]:
        """Extract column definitions from CREATE TABLE statement."""
        columns = []
        
        if stmt.expressions:
            for expr in stmt.expressions:
                if isinstance(expr, exp.ColumnDef):
                    column = {
                        "name": expr.this.name,
                        "data_type": self._format_data_type(expr.kind),
                        "nullable": not expr.exists("NOT NULL"),
                    }
                    
                    # Extract description from comments if available
                    comment = next((o.this for o in expr.find_all(exp.ColumnConstraint) 
                                  if o.kind == "COMMENT"), None)
                    if comment:
                        column["description"] = str(comment).strip("'\"")
                    
                    columns.append(column)
        
        return columns
    
    def _format_data_type(self, data_type: Optional[str]) -> str:
        """Format data type string."""
        if not data_type:
            return "UNKNOWN"
        return data_type.upper()
    
    def _extract_primary_key(self, stmt: exp.Create) -> Optional[List[str]]:
        """Extract primary key columns from CREATE TABLE statement."""
        # Look for PRIMARY KEY constraint
        primary_key = None
        
        for expr in stmt.expressions:
            if isinstance(expr, exp.PrimaryKey):
                primary_key = [col.name for col in expr.expressions if hasattr(col, 'name')]
                break
        
        return primary_key
    
    def _extract_partitioning(self, stmt: exp.Create) -> Optional[Dict]:
        """Extract partitioning information from CREATE TABLE statement."""
        # In BigQuery, partitioning is defined in OPTIONS
        partition_options = self._find_option(stmt, "partition_by")
        if partition_options:
            return {"expression": str(partition_options)}
        return None
    
    def _extract_clustering(self, stmt: exp.Create) -> Optional[List[str]]:
        """Extract clustering information from CREATE TABLE statement."""
        cluster_options = self._find_option(stmt, "cluster_by")
        if cluster_options:
            # Parse the clustering columns
            try:
                clustering_str = str(cluster_options)
                # Remove quotes and split by comma
                columns = [col.strip(' "\'') for col in clustering_str.split(',')]
                return columns
            except Exception:
                return [str(cluster_options)]
        return None
    
    def _extract_options(self, stmt: exp.Create) -> Dict:
        """Extract table options from CREATE TABLE statement."""
        options = {}
        
        # Find OPTIONS expression
        for expr in stmt.find_all(exp.TableOptions):
            for option in expr.expressions:
                if isinstance(option, exp.Option):
                    options[option.this] = str(option.expression).strip("'\"")
        
        return options
    
    def _find_option(self, stmt: exp.Create, option_name: str) -> Optional[Union[str, Dict]]:
        """Find a specific option in the statement."""
        for expr in stmt.find_all(exp.TableOptions):
            for option in expr.expressions:
                if isinstance(option, exp.Option) and option.this == option_name:
                    return option.expression
        return None
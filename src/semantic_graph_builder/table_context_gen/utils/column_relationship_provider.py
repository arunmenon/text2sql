"""
Column Relationship Provider

Provides relationship context for columns to enhance descriptions
with awareness of their join patterns and connections to other tables.
"""
import logging
from typing import Dict, List, Any, Optional, Tuple, Union

logger = logging.getLogger(__name__)

class ColumnRelationshipProvider:
    """Provides relationship context for columns participating in relationships."""
    
    def __init__(self, neo4j_client):
        """
        Initialize column relationship provider.
        
        Args:
            neo4j_client: Neo4j client for graph database access
        """
        self.neo4j_client = neo4j_client
        
    def get_column_relationships(
        self, 
        tenant_id: str, 
        table_name: str, 
        column_name: str,
        include_table_info: bool = True,
        max_relationships: int = 5
    ) -> str:
        """
        Get a summary of relationships in which this column participates.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            column_name: Column name
            include_table_info: Whether to include brief table descriptions
            max_relationships: Maximum number of relationships to include
            
        Returns:
            Formatted string describing the column's relationships
        """
        try:
            # Get all relationships for the table
            relationships = self.neo4j_client.get_relationships_for_table(tenant_id, table_name)
            
            # Extract relationships involving this specific column
            outgoing_references, incoming_references = self._process_relationships(
                relationships, table_name, column_name
            )
            
            # Get table descriptions if needed
            table_descriptions = {}
            if include_table_info and (outgoing_references or incoming_references):
                # Collect tables that this column connects to
                related_tables = set()
                for rel in outgoing_references:
                    related_tables.add(rel["target_table"])
                for rel in incoming_references:
                    related_tables.add(rel["source_table"])
                
                # Get descriptions for these tables
                for related_table in related_tables:
                    try:
                        # Skip the column's own table
                        if related_table == table_name:
                            continue
                            
                        # Get basic table info
                        tables = self.neo4j_client._execute_query(
                            "MATCH (t:Table {tenant_id: $tenant_id, name: $table_name}) RETURN t.description as description",
                            {"tenant_id": tenant_id, "table_name": related_table}
                        )
                        
                        if tables and tables[0].get("description"):
                            # Truncate description if too long
                            desc = tables[0].get("description", "")
                            if len(desc) > 100:
                                desc = desc[:97] + "..."
                            table_descriptions[related_table] = desc
                    except Exception as e:
                        logger.warning(f"Could not retrieve description for {related_table}: {e}")
            
            # Format relationship summary
            summary = []
            
            # Add outgoing references (foreign key-like relationships)
            if outgoing_references:
                summary.append(f"'{column_name}' references:")
                
                # Sort references by confidence
                sorted_references = sorted(
                    outgoing_references, 
                    key=lambda x: x.get("confidence", 0),
                    reverse=True
                )
                
                # Limit to max_relationships
                for ref in sorted_references[:max_relationships]:
                    target_table = ref["target_table"]
                    target_column = ref["target_column"]
                    confidence = ref.get("confidence", 0)
                    relationship_type = ref.get("relationship_type", "")
                    
                    # Format as: "table.column (type) [confidence: 0.95]"
                    ref_text = f"- {target_table}.{target_column}"
                    
                    if relationship_type:
                        ref_text += f" ({relationship_type})"
                        
                    ref_text += f" [confidence: {confidence:.2f}]"
                    
                    summary.append(ref_text)
                    
                    # Add table description if available
                    if include_table_info and target_table in table_descriptions:
                        summary.append(f"  Table context: {table_descriptions[target_table]}")
            
            # Add incoming references (referenced by other tables)
            if incoming_references:
                summary.append(f"'{column_name}' is referenced by:")
                
                # Sort references by confidence
                sorted_references = sorted(
                    incoming_references, 
                    key=lambda x: x.get("confidence", 0),
                    reverse=True
                )
                
                # Limit to max_relationships
                for ref in sorted_references[:max_relationships]:
                    source_table = ref["source_table"]
                    source_column = ref["source_column"]
                    confidence = ref.get("confidence", 0)
                    relationship_type = ref.get("relationship_type", "")
                    
                    # Format as: "table.column (type) [confidence: 0.95]"
                    ref_text = f"- {source_table}.{source_column}"
                    
                    if relationship_type:
                        ref_text += f" ({relationship_type})"
                        
                    ref_text += f" [confidence: {confidence:.2f}]"
                    
                    summary.append(ref_text)
                    
                    # Add table description if available
                    if include_table_info and source_table in table_descriptions:
                        summary.append(f"  Table context: {table_descriptions[source_table]}")
            
            # Add a conclusion about the role of this column if it participates in relationships
            if outgoing_references or incoming_references:
                # Determine if it's a foreign key, primary key being referenced, or both
                if outgoing_references and not incoming_references:
                    summary.append(f"This suggests '{column_name}' is a foreign key that references data in other tables.")
                elif incoming_references and not outgoing_references:
                    summary.append(f"This suggests '{column_name}' is likely a primary key or unique identifier in {table_name}.")
                elif outgoing_references and incoming_references:
                    summary.append(f"This column participates in relationships as both a foreign key and a referenced key, suggesting it plays a central role in connecting multiple tables.")
            
            return "\n".join(summary) if summary else f"No relationships found involving the '{column_name}' column."
            
        except Exception as e:
            logger.error(f"Error getting column relationships for {table_name}.{column_name}: {e}")
            return ""  # Return empty string on error for graceful degradation
    
    def _process_relationships(
        self, 
        relationships: List[Dict], 
        table_name: str, 
        column_name: str
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Process relationships to extract those involving the specified column.
        
        Args:
            relationships: List of relationship dictionaries from Neo4j
            table_name: The table name
            column_name: The column name
            
        Returns:
            Tuple of (outgoing_references, incoming_references) lists
        """
        outgoing_references = []  # Column references other tables
        incoming_references = []  # Column is referenced by other tables
        
        for rel in relationships:
            try:
                # Ensure relationship has the expected structure
                if not isinstance(rel, dict):
                    continue
                    
                if not all(k in rel for k in ['source', 'r', 'target']):
                    continue
                
                source = rel['source']
                target = rel['target']
                r = rel['r']
                
                # Ensure source and target are dictionaries
                if not (isinstance(source, dict) and isinstance(target, dict)):
                    continue
                
                # Extract relationship type and properties based on r type
                relationship_type = "references"
                confidence = 0.8  # Default confidence
                
                # Handle different r structures:
                # 1. If r is a tuple with format (source_props, rel_type, target_props)
                if isinstance(r, tuple) and len(r) >= 2:
                    relationship_type = r[1]  # Second element is the relationship type
                    
                # 2. If r is a dictionary with properties
                elif isinstance(r, dict):
                    relationship_type = r.get('relationship_type', 'references')
                    confidence = r.get('confidence', 0.8)
                
                # Extract values safely
                source_table = source.get('table_name')
                source_col = source.get('name')
                target_table = target.get('table_name')
                target_col = target.get('name')
                
                # Skip if any required values are missing
                if not all([source_table, source_col, target_table, target_col]):
                    continue
                
                # Match only relationships involving this specific column
                
                # This column references another table's column (outgoing)
                if source_table == table_name and source_col == column_name:
                    outgoing_references.append({
                        'source_table': source_table,
                        'source_column': source_col,
                        'target_table': target_table,
                        'target_column': target_col,
                        'relationship_type': relationship_type,
                        'confidence': confidence
                    })
                
                # Another table's column references this column (incoming)
                elif target_table == table_name and target_col == column_name:
                    incoming_references.append({
                        'source_table': source_table,
                        'source_column': source_col,
                        'target_table': target_table,
                        'target_column': target_col,
                        'relationship_type': relationship_type,
                        'confidence': confidence
                    })
            
            except Exception as e:
                logger.warning(f"Error processing relationship: {e}")
                continue
        
        return outgoing_references, incoming_references
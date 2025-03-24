"""
Table Neighborhood Provider

Provides relationship context for tables to enhance descriptions
with awareness of their position in the data ecosystem.
"""
import logging
from typing import Dict, List, Any, Optional, Tuple, Union

logger = logging.getLogger(__name__)

class TableNeighborhoodProvider:
    """Provides relationship context for a table's connected neighborhood."""
    
    def __init__(self, neo4j_client):
        """
        Initialize table neighborhood provider.
        
        Args:
            neo4j_client: Neo4j client for graph database access
        """
        self.neo4j_client = neo4j_client
        
    def get_table_neighborhood(self, tenant_id: str, table_name: str, max_tables: int = 5, 
                             max_relationships_per_table: int = 3, include_descriptions: bool = False) -> str:
        """
        Get a summary of tables related to the given table.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            max_tables: Maximum number of related tables to include in each direction
            max_relationships_per_table: Maximum number of relationships to show per table
            include_descriptions: Whether to include table descriptions in the output
            
        Returns:
            Formatted string describing the table's neighborhood
        """
        try:
            # Get all relationships for this table
            relationships = self.neo4j_client.get_relationships_for_table(tenant_id, table_name)
            
            # Process relationships to extract references
            outgoing_references, incoming_references = self._process_relationships(relationships, table_name)
            
            # Collect table descriptions if requested
            table_descriptions = {}
            if include_descriptions and (outgoing_references or incoming_references):
                related_tables = set(list(outgoing_references.keys()) + list(incoming_references.keys()))
                for related_table in related_tables:
                    try:
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
            
            # Format neighborhood summary
            summary = []
            
            # Add outgoing references 
            if outgoing_references:
                summary.append(f"Tables referenced by {table_name}:")
                for referenced_table, refs in list(sorted(outgoing_references.items(), 
                                             key=lambda x: max([r['confidence'] for r in x[1]]), 
                                             reverse=True))[:max_tables]:
                    # Sort relationships by confidence
                    sorted_refs = sorted(refs, key=lambda r: r['confidence'], reverse=True)
                    ref_summary = ", ".join([
                        f"{r['local_column']} → {referenced_table}.{r['referenced_column']}" 
                        for r in sorted_refs[:max_relationships_per_table]
                    ])
                    
                    table_entry = f"- {referenced_table} ({ref_summary})"
                    
                    # Add description if available
                    if include_descriptions and referenced_table in table_descriptions:
                        table_entry += f"\n  Description: {table_descriptions[referenced_table]}"
                        
                    summary.append(table_entry)
            
            # Add incoming references
            if incoming_references:
                summary.append(f"Tables that reference {table_name}:")
                for referencing_table, refs in list(sorted(incoming_references.items(), 
                                              key=lambda x: max([r['confidence'] for r in x[1]]), 
                                              reverse=True))[:max_tables]:
                    # Sort relationships by confidence
                    sorted_refs = sorted(refs, key=lambda r: r['confidence'], reverse=True)
                    ref_summary = ", ".join([
                        f"{referencing_table}.{r['referencing_column']} → {r['referenced_column']}" 
                        for r in sorted_refs[:max_relationships_per_table]
                    ])
                    
                    table_entry = f"- {referencing_table} ({ref_summary})"
                    
                    # Add description if available
                    if include_descriptions and referencing_table in table_descriptions:
                        table_entry += f"\n  Description: {table_descriptions[referencing_table]}"
                        
                    summary.append(table_entry)
            
            return "\n".join(summary) if summary else ""
            
        except Exception as e:
            logger.error(f"Error getting table neighborhood for {table_name}: {e}")
            return ""  # Return empty string on error for graceful degradation
            
    def _process_relationships(self, relationships: List[Dict], table_name: str) -> Tuple[Dict, Dict]:
        """
        Process relationships to extract outgoing and incoming references.
        
        Args:
            relationships: List of relationship dictionaries from Neo4j
            table_name: The table name to process relationships for
            
        Returns:
            Tuple of (outgoing_references, incoming_references) dictionaries
        """
        outgoing_references = {}  # Tables referenced by this table
        incoming_references = {}  # Tables that reference this table
        
        for rel in relationships:
            try:
                # Ensure relationship has the expected structure
                if not isinstance(rel, dict):
                    logger.warning(f"Relationship is not a dictionary: {type(rel)}")
                    continue
                    
                if not all(k in rel for k in ['source', 'r', 'target']):
                    logger.warning(f"Relationship missing required keys: {rel.keys() if hasattr(rel, 'keys') else 'no keys'}")
                    continue
                
                source = rel['source']
                target = rel['target']
                r = rel['r']
                
                # Ensure source and target are dictionaries
                if not (isinstance(source, dict) and isinstance(target, dict)):
                    logger.warning(f"Source or target is not a dictionary: source={type(source)}, target={type(target)}")
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
                
                # This table references another table (outgoing)
                if source_table == table_name and target_table != table_name:
                    if target_table not in outgoing_references:
                        outgoing_references[target_table] = []
                    
                    outgoing_references[target_table].append({
                        'local_column': source_col,
                        'referenced_column': target_col,
                        'relationship_type': relationship_type,
                        'confidence': confidence
                    })
                
                # Another table references this table (incoming)
                elif target_table == table_name and source_table != table_name:
                    if source_table not in incoming_references:
                        incoming_references[source_table] = []
                    
                    incoming_references[source_table].append({
                        'referenced_column': target_col,
                        'referencing_column': source_col, 
                        'relationship_type': relationship_type,
                        'confidence': confidence
                    })
            
            except Exception as e:
                logger.warning(f"Error processing relationship: {e}")
                continue
        
        return outgoing_references, incoming_references
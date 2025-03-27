#!/usr/bin/env python3
"""
Neo4j Graph Export Utility

This utility exports a complete Neo4j graph to a format that can be imported into another Neo4j instance.
It can generate either:
1. A sequence of Cypher statements (CREATE/MERGE commands)
2. A GraphML file
3. CSV files for bulk import

The output can be used to restore the complete graph state to another Neo4j instance.

Usage:
    python -m src.semantic_graph_builder.utils.graph_export --tenant-id TENANT_ID --format cypher --output graph_backup.cypher
"""

import os
import sys
import argparse
import logging
import json
import asyncio
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
import uuid
from dotenv import load_dotenv

# Import Neo4j client
from src.graph_storage.neo4j_client import Neo4jClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class GraphExporter:
    """
    Exports a Neo4j graph to various formats for backup and restoration.
    """
    
    def __init__(self, neo4j_client: Neo4jClient, tenant_id: str = None):
        """
        Initialize the graph exporter.
        
        Args:
            neo4j_client: Neo4j client instance
            tenant_id: Optional tenant ID to limit the export scope
        """
        self.neo4j_client = neo4j_client
        self.tenant_id = tenant_id
        
        # Track processed nodes to avoid duplicates
        self.processed_node_ids = set()
        
        # Track node IDs for relationship creation
        self.node_id_mapping = {}
        
    def get_all_node_labels(self) -> List[str]:
        """
        Get all node labels in the graph.
        
        Returns:
            List of node labels
        """
        if self.tenant_id:
            # If tenant_id is specified, get labels for nodes with that tenant_id
            query = """
            MATCH (n)
            WHERE n.tenant_id = $tenant_id OR (n:Tenant AND n.id = $tenant_id)
            WITH labels(n) as labels
            UNWIND labels as label
            RETURN DISTINCT label
            ORDER BY label
            """
            
            result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
            if not result:
                # Log warning about no data
                logger.warning(f"No nodes found for tenant '{self.tenant_id}'")
                return []
                
            # Extract labels from result
            all_labels = [r["label"] for r in result if r.get("label")]
            logger.info(f"Found {len(all_labels)} node labels for tenant '{self.tenant_id}'")
            return all_labels
        else:
            # If no tenant_id, get all labels
            query = """
            CALL db.labels() YIELD label
            RETURN collect(label) as labels
            """
            result = self.neo4j_client._execute_query(query)
            return result[0].get("labels", []) if result else []
    
    def get_all_relationship_types(self) -> List[str]:
        """
        Get all relationship types in the graph.
        
        Returns:
            List of relationship types
        """
        if self.tenant_id:
            # If tenant_id is specified, get types for relationships connecting nodes with that tenant_id
            # Include special case for Tenant nodes
            query = """
            MATCH (n)-[r]->(m)
            WHERE (n.tenant_id = $tenant_id AND m.tenant_id = $tenant_id) 
               OR (n:Tenant AND n.id = $tenant_id) 
               OR (m:Tenant AND m.id = $tenant_id)
            RETURN type(r) as type
            """
            
            result = self.neo4j_client._execute_query(query, {"tenant_id": self.tenant_id})
            if not result:
                # Log warning about no relationships
                logger.warning(f"No relationships found for tenant '{self.tenant_id}'")
                return []
                
            # Extract relationship types from result
            rel_types = set()
            for r in result:
                if r.get("type"):
                    rel_types.add(r["type"])
            
            logger.info(f"Found {len(rel_types)} relationship types for tenant '{self.tenant_id}'")
            return list(rel_types)
        else:
            # If no tenant_id, get all relationship types
            query = """
            CALL db.relationshipTypes() YIELD relationshipType
            RETURN collect(relationshipType) as types
            """
            
            result = self.neo4j_client._execute_query(query)
            return result[0].get("types", []) if result else []
    
    def get_nodes_by_label(self, label: str) -> List[Dict[str, Any]]:
        """
        Get all nodes with a specific label.
        
        Args:
            label: Node label to query
            
        Returns:
            List of nodes with their properties
        """
        # Special handling for Tenant nodes
        if label == "Tenant" and self.tenant_id:
            query = """
            MATCH (n:Tenant)
            WHERE n.id = $tenant_id
            RETURN id(n) as id, n as node, labels(n) as labels
            """
            params = {"tenant_id": self.tenant_id}
        else:
            # For other node types
            where_clause = "WHERE n.tenant_id = $tenant_id" if self.tenant_id else ""
            query = f"""
            MATCH (n:{label})
            {where_clause}
            RETURN id(n) as id, n as node, labels(n) as labels
            """
            params = {"tenant_id": self.tenant_id} if self.tenant_id else {}
        
        return self.neo4j_client._execute_query(query, params)
    
    def get_relationships_by_type(self, rel_type: str) -> List[Dict[str, Any]]:
        """
        Get all relationships of a specific type.
        
        Args:
            rel_type: Relationship type to query
            
        Returns:
            List of relationships with their properties
        """
        query = """
        MATCH (n)-[r:{rel_type}]->(m)
        {where_clause}
        RETURN id(n) as source_id, id(m) as target_id, r as relationship, type(r) as type
        """.format(
            rel_type=rel_type,
            where_clause="WHERE n.tenant_id = $tenant_id AND m.tenant_id = $tenant_id" if self.tenant_id else ""
        )
        
        params = {"tenant_id": self.tenant_id} if self.tenant_id else {}
        
        return self.neo4j_client._execute_query(query, params)
    
    def escape_property_value(self, value: Any) -> str:
        """
        Escape property values for Cypher statements.
        
        Args:
            value: Property value
            
        Returns:
            Escaped value as string
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape quotes and newlines
            escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
            return f"'{escaped}'"
        elif isinstance(value, list):
            # Handle lists
            items = [self.escape_property_value(item) for item in value]
            return f"[{', '.join(items)}]"
        elif isinstance(value, dict):
            # Handle dictionaries
            items = [f"{self.escape_property_value(k)}: {self.escape_property_value(v)}" for k, v in value.items()]
            return f"{{{', '.join(items)}}}"
        elif isinstance(value, datetime.datetime):
            # Format datetime as string
            return f"datetime('{value.isoformat()}')"
        else:
            # Default to string representation
            return f"'{str(value)}'"
    
    def properties_to_cypher(self, properties: Dict[str, Any]) -> str:
        """
        Convert node or relationship properties to Cypher format.
        
        Args:
            properties: Dictionary of properties
            
        Returns:
            Properties formatted for Cypher
        """
        if not properties:
            return "{}"
            
        props = []
        for k, v in properties.items():
            # Skip internal Neo4j properties
            if k.startswith("_neo4j_"):
                continue
                
            # Format the property
            props.append(f"{k}: {self.escape_property_value(v)}")
            
        return "{" + ", ".join(props) + "}"
    
    def export_as_cypher(self, output_file: str) -> bool:
        """
        Export the graph as a sequence of Cypher statements.
        
        Args:
            output_file: Path to output file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get all node labels and relationship types
            node_labels = self.get_all_node_labels()
            rel_types = self.get_all_relationship_types()
            
            logger.info(f"Found {len(node_labels)} node labels and {len(rel_types)} relationship types")
            
            # Open output file
            with open(output_file, 'w') as f:
                # Write header
                f.write("// Neo4j Graph Export\n")
                f.write(f"// Generated: {datetime.datetime.now().isoformat()}\n")
                if self.tenant_id:
                    f.write(f"// Tenant: {self.tenant_id}\n")
                f.write("\n")
                
                # Write schema constraints to ensure uniqueness constraints are preserved
                f.write("// Schema constraints\n")
                f.write("CREATE CONSTRAINT tenant_id_constraint IF NOT EXISTS FOR (n:Tenant) REQUIRE n.id IS UNIQUE;\n")
                f.write("CREATE CONSTRAINT table_constraint IF NOT EXISTS FOR (n:Table) REQUIRE (n.tenant_id, n.name) IS UNIQUE;\n")
                f.write("CREATE CONSTRAINT column_constraint IF NOT EXISTS FOR (n:Column) REQUIRE (n.tenant_id, n.table_name, n.name) IS UNIQUE;\n")
                f.write("\n")
                
                # Start a transaction
                f.write("// Start transaction\n")
                f.write("BEGIN\n\n")
                
                # Process nodes by label
                f.write("// Create nodes\n")
                node_count = 0
                
                for label in sorted(node_labels):
                    f.write(f"// {label} nodes\n")
                    nodes = self.get_nodes_by_label(label)
                    
                    for node_data in nodes:
                        node_id = node_data.get("id")
                        node = node_data.get("node")
                        labels = node_data.get("labels", [label])
                        
                        # Skip if already processed
                        if node_id in self.processed_node_ids:
                            continue
                            
                        # Extract properties - safely convert to dict
                        properties = {}
                        if node is not None:
                            # Handle different types of node objects
                            if hasattr(node, 'items'):
                                # If it already has an items method, use it
                                properties = dict(node)
                            elif isinstance(node, dict):
                                # If it's already a dict
                                properties = node
                            else:
                                # Last resort - try string conversion and warning
                                logger.warning(f"Unexpected node type: {type(node)}")
                                properties = {"value": str(node)}
                        
                        # Create a unique identifier for this node
                        node_var = f"n_{node_count}"
                        self.node_id_mapping[node_id] = node_var
                        
                        # Create node with label and properties
                        node_labels_str = ":".join(labels) if isinstance(labels, list) else labels
                        properties_str = self.properties_to_cypher(properties)
                        
                        # Use MERGE for nodes with unique constraints, CREATE for others
                        if "Tenant" in labels or "Table" in labels or "Column" in labels:
                            if "Tenant" in labels:
                                f.write(f"MERGE ({node_var}:{node_labels_str} {properties_str})\n")
                            elif "Table" in labels:
                                tenant_id = properties.get("tenant_id", "unknown")
                                table_name = properties.get("name", "unknown")
                                f.write(f"MERGE ({node_var}:{node_labels_str} {{tenant_id: {self.escape_property_value(tenant_id)}, name: {self.escape_property_value(table_name)}}}) SET {node_var} += {properties_str}\n")
                            elif "Column" in labels:
                                tenant_id = properties.get("tenant_id", "unknown")
                                table_name = properties.get("table_name", "unknown")
                                column_name = properties.get("name", "unknown")
                                f.write(f"MERGE ({node_var}:{node_labels_str} {{tenant_id: {self.escape_property_value(tenant_id)}, table_name: {self.escape_property_value(table_name)}, name: {self.escape_property_value(column_name)}}}) SET {node_var} += {properties_str}\n")
                        else:
                            f.write(f"CREATE ({node_var}:{node_labels_str} {properties_str})\n")
                        
                        # Mark as processed
                        self.processed_node_ids.add(node_id)
                        node_count += 1
                        
                        # Add a commit every 1000 nodes to avoid transaction size issues
                        if node_count % 1000 == 0:
                            f.write("\nCOMMIT\nBEGIN\n\n")
                    
                # Commit after creating all nodes
                f.write("\nCOMMIT\nBEGIN\n\n")
                
                # Process relationships by type
                f.write("// Create relationships\n")
                rel_count = 0
                
                for rel_type in sorted(rel_types):
                    f.write(f"// {rel_type} relationships\n")
                    relationships = self.get_relationships_by_type(rel_type)
                    
                    for rel_data in relationships:
                        source_id = rel_data.get("source_id")
                        target_id = rel_data.get("target_id")
                        relationship = rel_data.get("relationship")
                        rel_type = rel_data.get("type")
                        
                        # Skip if source or target not processed
                        if source_id not in self.node_id_mapping or target_id not in self.node_id_mapping:
                            continue
                            
                        # Extract properties - safely convert to dict
                        properties = {}
                        if relationship is not None:
                            # Handle different types of relationship objects
                            if hasattr(relationship, 'items'):
                                # If it already has an items method, use it
                                properties = dict(relationship)
                            elif isinstance(relationship, dict):
                                # If it's already a dict
                                properties = relationship
                            else:
                                # Last resort - try string conversion and warning
                                logger.warning(f"Unexpected relationship type: {type(relationship)}")
                                properties = {}
                        
                        # Get node variables
                        source_var = self.node_id_mapping[source_id]
                        target_var = self.node_id_mapping[target_id]
                        
                        # Create relationship with properties
                        properties_str = self.properties_to_cypher(properties)
                        f.write(f"MATCH ({source_var}), ({target_var}) CREATE ({source_var})-[:{rel_type} {properties_str}]->({target_var})\n")
                        
                        rel_count += 1
                        
                        # Add a commit every 1000 relationships to avoid transaction size issues
                        if rel_count % 1000 == 0:
                            f.write("\nCOMMIT\nBEGIN\n\n")
                
                # Commit after creating all relationships
                f.write("\nCOMMIT\n\n")
                
                # Write summary
                f.write(f"// Export complete: {node_count} nodes, {rel_count} relationships\n")
            
            logger.info(f"Successfully exported {node_count} nodes and {rel_count} relationships to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting graph: {e}")
            return False
    
    def export_as_graphml(self, output_file: str) -> bool:
        """
        Export the graph in GraphML format.
        
        Args:
            output_file: Path to output file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use Neo4j's APOC procedure to export GraphML if it's available
            query = """
            CALL apoc.export.graphml.all($file, {useTypes: true, storeNodeIds: true})
            YIELD file, source, format, nodes, relationships, properties, time
            RETURN file, source, format, nodes, relationships, properties, time
            """
            
            # If tenant_id is specified, limit the export
            if self.tenant_id:
                query = """
                MATCH (n)
                WHERE n.tenant_id = $tenant_id
                WITH collect(n) as nodes
                CALL apoc.export.graphml.data(nodes, [], $file, {useTypes: true, storeNodeIds: true})
                YIELD file, source, format, nodes, relationships, properties, time
                RETURN file, source, format, nodes, relationships, properties, time
                """
            
            params = {
                "file": output_file,
                "tenant_id": self.tenant_id
            }
            
            result = self.neo4j_client._execute_query(query, params)
            
            if result:
                stats = result[0]
                logger.info(f"Successfully exported to GraphML: {stats}")
                return True
            else:
                logger.error("Failed to export as GraphML. Check if APOC is installed and configured.")
                return False
                
        except Exception as e:
            logger.error(f"Error exporting graph as GraphML: {e}")
            logger.warning("GraphML export requires the APOC plugin to be installed in Neo4j")
            return False
    
    def export_as_csv(self, output_dir: str) -> bool:
        """
        Export the graph as CSV files for bulk import.
        
        Args:
            output_dir: Directory for output CSV files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Get all node labels and relationship types
            node_labels = self.get_all_node_labels()
            rel_types = self.get_all_relationship_types()
            
            logger.info(f"Found {len(node_labels)} node labels and {len(rel_types)} relationship types")
            
            # Create import metadata file
            with open(os.path.join(output_dir, "import.cypher"), "w") as f:
                f.write("// Neo4j CSV Import\n")
                f.write(f"// Generated: {datetime.datetime.now().isoformat()}\n")
                if self.tenant_id:
                    f.write(f"// Tenant: {self.tenant_id}\n")
                f.write("\n")
                
                # Process nodes by label
                node_count = 0
                for label in sorted(node_labels):
                    # Export nodes to CSV
                    nodes = self.get_nodes_by_label(label)
                    
                    if not nodes:
                        continue
                        
                    # Get all property keys from the first node
                    node = nodes[0].get("node")
                    property_keys = list(dict(node).keys())
                    
                    # Check remaining nodes for additional properties
                    for node_data in nodes[1:]:
                        node = node_data.get("node")
                        for key in dict(node).keys():
                            if key not in property_keys:
                                property_keys.append(key)
                    
                    # Create CSV file for this node label
                    csv_file = os.path.join(output_dir, f"{label}_nodes.csv")
                    with open(csv_file, "w") as csv_f:
                        # Write header
                        csv_f.write("id:ID," + ",".join([f"{key}" for key in property_keys]) + ",:LABEL\n")
                        
                        # Write node data
                        for node_data in nodes:
                            node_id = node_data.get("id")
                            node = node_data.get("node")
                            labels = node_data.get("labels", [label])
                            
                            # Extract properties
                            properties = dict(node)
                            
                            # Write node row
                            row = [str(node_id)]
                            for key in property_keys:
                                value = properties.get(key, "")
                                # Format value for CSV
                                if value is None:
                                    row.append("")
                                elif isinstance(value, str):
                                    # Escape quotes and newlines
                                    escaped = value.replace("\"", "\"\"").replace("\n", "\\n")
                                    row.append(f"\"{escaped}\"")
                                elif isinstance(value, list):
                                    # Convert list to string
                                    row.append(f"\"{json.dumps(value)}\"")
                                elif isinstance(value, dict):
                                    # Convert dict to string
                                    row.append(f"\"{json.dumps(value)}\"")
                                else:
                                    row.append(str(value))
                            
                            # Add labels
                            row.append(";".join(labels))
                            
                            csv_f.write(",".join(row) + "\n")
                            node_count += 1
                    
                    # Add import command to metadata file
                    f.write(f"// Import {label} nodes\n")
                    f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{label}_nodes.csv' AS row\n")
                    f.write(f"CREATE (:{label} {{\n")
                    for key in property_keys:
                        f.write(f"    {key}: row.{key},\n")
                    f.write(f"}})\n\n")
                
                # Process relationships by type
                rel_count = 0
                for rel_type in sorted(rel_types):
                    # Export relationships to CSV
                    relationships = self.get_relationships_by_type(rel_type)
                    
                    if not relationships:
                        continue
                        
                    # Get all property keys from the first relationship
                    rel = relationships[0].get("relationship")
                    property_keys = list(dict(rel).keys())
                    
                    # Check remaining relationships for additional properties
                    for rel_data in relationships[1:]:
                        rel = rel_data.get("relationship")
                        for key in dict(rel).keys():
                            if key not in property_keys:
                                property_keys.append(key)
                    
                    # Create CSV file for this relationship type
                    csv_file = os.path.join(output_dir, f"{rel_type}_rels.csv")
                    with open(csv_file, "w") as csv_f:
                        # Write header
                        csv_f.write(":START_ID,:END_ID," + ",".join([f"{key}" for key in property_keys]) + ",:TYPE\n")
                        
                        # Write relationship data
                        for rel_data in relationships:
                            source_id = rel_data.get("source_id")
                            target_id = rel_data.get("target_id")
                            rel = rel_data.get("relationship")
                            
                            # Extract properties
                            properties = dict(rel)
                            
                            # Write relationship row
                            row = [str(source_id), str(target_id)]
                            for key in property_keys:
                                value = properties.get(key, "")
                                # Format value for CSV
                                if value is None:
                                    row.append("")
                                elif isinstance(value, str):
                                    # Escape quotes and newlines
                                    escaped = value.replace("\"", "\"\"").replace("\n", "\\n")
                                    row.append(f"\"{escaped}\"")
                                elif isinstance(value, list):
                                    # Convert list to string
                                    row.append(f"\"{json.dumps(value)}\"")
                                elif isinstance(value, dict):
                                    # Convert dict to string
                                    row.append(f"\"{json.dumps(value)}\"")
                                else:
                                    row.append(str(value))
                            
                            # Add type
                            row.append(rel_type)
                            
                            csv_f.write(",".join(row) + "\n")
                            rel_count += 1
                    
                    # Add import command to metadata file
                    f.write(f"// Import {rel_type} relationships\n")
                    f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{rel_type}_rels.csv' AS row\n")
                    f.write(f"MATCH (source) WHERE id(source) = toInteger(row.START_ID)\n")
                    f.write(f"MATCH (target) WHERE id(target) = toInteger(row.END_ID)\n")
                    f.write(f"CREATE (source)-[:{rel_type} {{\n")
                    for key in property_keys:
                        f.write(f"    {key}: row.{key},\n")
                    f.write(f"}}]->(target)\n\n")
                
                # Write summary
                f.write(f"// Import complete: {node_count} nodes, {rel_count} relationships\n")
            
            logger.info(f"Successfully exported {node_count} nodes and {rel_count} relationships to {output_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting graph as CSV: {e}")
            return False

async def export_graph(
    neo4j_uri: str, 
    neo4j_user: str, 
    neo4j_password: str, 
    tenant_id: Optional[str] = None,
    export_format: str = "cypher",
    output: str = "graph_export"
) -> bool:
    """
    Export a Neo4j graph.
    
    Args:
        neo4j_uri: Neo4j connection URI
        neo4j_user: Neo4j username
        neo4j_password: Neo4j password
        tenant_id: Optional tenant ID to limit export scope
        export_format: Export format ('cypher', 'graphml', or 'csv')
        output: Output file or directory
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize Neo4j client
        neo4j_client = Neo4jClient(neo4j_uri, neo4j_user, neo4j_password)
        
        # Initialize exporter
        exporter = GraphExporter(neo4j_client, tenant_id)
        
        # Export based on format
        if export_format == "cypher":
            success = exporter.export_as_cypher(output)
        elif export_format == "graphml":
            success = exporter.export_as_graphml(output)
        elif export_format == "csv":
            success = exporter.export_as_csv(output)
        else:
            logger.error(f"Unsupported export format: {export_format}")
            success = False
        
        # Close Neo4j client
        neo4j_client.close()
        
        return success
        
    except Exception as e:
        logger.error(f"Error in export_graph: {e}")
        return False

async def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Export a Neo4j graph for backup and restore")
    
    parser.add_argument("--tenant-id", help="Tenant ID to limit export scope")
    
    parser.add_argument(
        "--format", 
        choices=["cypher", "graphml", "csv"], 
        default="cypher",
        help="Export format (default: cypher)"
    )
    
    parser.add_argument(
        "--output", 
        default="graph_export",
        help="Output file or directory name (default: graph_export)"
    )
    
    parser.add_argument(
        "--uri", 
        default=os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
        help="Neo4j connection URI"
    )
    
    parser.add_argument(
        "--user", 
        default=os.getenv("NEO4J_USERNAME", "neo4j"),
        help="Neo4j username"
    )
    
    parser.add_argument(
        "--password", 
        default=os.getenv("NEO4J_PASSWORD", "password"),
        help="Neo4j password"
    )
    
    args = parser.parse_args()
    
    # Add extension if not provided
    output = args.output
    if args.format == "cypher" and not output.endswith(".cypher"):
        output += ".cypher"
    elif args.format == "graphml" and not output.endswith(".graphml"):
        output += ".graphml"
    
    # Ensure CSV export goes to a directory
    if args.format == "csv":
        if os.path.isfile(output):
            logger.error(f"Output path must be a directory for CSV export: {output}")
            return 1
    
    # Run export
    success = await export_graph(
        args.uri,
        args.user,
        args.password,
        args.tenant_id,
        args.format,
        output
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    asyncio.run(main())
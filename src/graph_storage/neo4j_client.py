"""
Neo4j Graph Storage Client

Handles storing and retrieving schema information in Neo4j graph database.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError

logger = logging.getLogger(__name__)

class Neo4jClient:
    """Client for interacting with Neo4j graph database."""
    
    def __init__(self, uri: str, username: str, password: str):
        """
        Initialize Neo4j client.
        
        Args:
            uri: Neo4j connection URI
            username: Neo4j username
            password: Neo4j password
        """
        self.uri = uri
        self.auth = basic_auth(username, password)
        self.driver = GraphDatabase.driver(uri, auth=self.auth)
    
    def close(self):
        """Close the Neo4j driver."""
        self.driver.close()
    
    def _execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Execute a Cypher query and return the results.
        
        Args:
            query: Cypher query string
            params: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        params = params or {}
        
        with self.driver.session() as session:
            try:
                result = session.run(query, params)
                return [record.data() for record in result]
            except Neo4jError as e:
                logger.error(f"Neo4j query error: {e}")
                raise
    
    def create_schema_constraints(self):
        """Create constraints and indexes for the schema graph."""
        constraints = [
            "CREATE CONSTRAINT tenant_id_constraint IF NOT EXISTS FOR (t:Tenant) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT dataset_id_constraint IF NOT EXISTS FOR (d:Dataset) REQUIRE (d.tenant_id, d.name) IS UNIQUE",
            "CREATE CONSTRAINT table_id_constraint IF NOT EXISTS FOR (t:Table) REQUIRE (t.tenant_id, t.dataset_id, t.name) IS UNIQUE",
            "CREATE CONSTRAINT column_id_constraint IF NOT EXISTS FOR (c:Column) REQUIRE (c.tenant_id, c.dataset_id, c.table_name, c.name) IS UNIQUE",
        ]
        
        indexes = [
            "CREATE INDEX tenant_name_idx IF NOT EXISTS FOR (t:Tenant) ON (t.name)",
            "CREATE INDEX table_name_idx IF NOT EXISTS FOR (t:Table) ON (t.name)",
            "CREATE INDEX column_name_idx IF NOT EXISTS FOR (c:Column) ON (c.name)",
        ]
        
        for constraint in constraints:
            try:
                self._execute_query(constraint)
            except Neo4jError as e:
                logger.warning(f"Could not create constraint: {e}")
        
        for index in indexes:
            try:
                self._execute_query(index)
            except Neo4jError as e:
                logger.warning(f"Could not create index: {e}")
    
    def create_tenant(self, tenant_id: str, name: str, description: Optional[str] = None) -> Dict:
        """
        Create a new tenant node.
        
        Args:
            tenant_id: Unique identifier for the tenant
            name: Display name for the tenant
            description: Optional description
            
        Returns:
            Created tenant node data
        """
        query = """
        MERGE (t:Tenant {id: $tenant_id})
        ON CREATE SET
            t.name = $name,
            t.description = $description,
            t.created_at = datetime()
        ON MATCH SET
            t.name = $name,
            t.description = $description,
            t.updated_at = datetime()
        RETURN t
        """
        
        params = {
            "tenant_id": tenant_id,
            "name": name,
            "description": description
        }
        
        result = self._execute_query(query, params)
        return result[0]["t"] if result else None
    
    def create_dataset(self, tenant_id: str, dataset_id: str, 
                      description: Optional[str] = None) -> Dict:
        """
        Create a dataset node and link to tenant.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset identifier
            description: Optional description
            
        Returns:
            Created dataset node data
        """
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        MERGE (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        ON CREATE SET
            d.description = $description,
            d.created_at = datetime()
        ON MATCH SET
            d.description = $description,
            d.updated_at = datetime()
        MERGE (t)-[:OWNS]->(d)
        RETURN d
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "description": description
        }
        
        result = self._execute_query(query, params)
        return result[0]["d"] if result else None
    
    def create_table(self, tenant_id: str, dataset_id: str, table_data: Dict) -> Dict:
        """
        Create a table node with its metadata.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            table_data: Table metadata
            
        Returns:
            Created table node data
        """
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        MERGE (t:Table {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            name: $table_name
        })
        ON CREATE SET
            t.description = $description,
            t.table_type = $table_type,
            t.created_at = datetime(),
            t.row_count = $row_count,
            t.source = $source,
            t.ddl = $ddl
        ON MATCH SET
            t.description = $description,
            t.table_type = $table_type,
            t.updated_at = datetime(),
            t.row_count = $row_count,
            t.source = $source,
            t.ddl = $ddl
        MERGE (d)-[:CONTAINS]->(t)
        RETURN t
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "table_name": table_data.get("table_name"),
            "description": table_data.get("description"),
            "table_type": table_data.get("table_type", "TABLE"),
            "row_count": table_data.get("statistics", {}).get("row_count"),
            "source": table_data.get("source", "bigquery"),
            "ddl": table_data.get("ddl")
        }
        
        result = self._execute_query(query, params)
        return result[0]["t"] if result else None
    
    def create_column(self, tenant_id: str, dataset_id: str, table_name: str, column_data: Dict) -> Dict:
        """
        Create a column node and link to its table.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            table_name: Table name
            column_data: Column metadata
            
        Returns:
            Created column node data
        """
        query = """
        MATCH (t:Table {tenant_id: $tenant_id, dataset_id: $dataset_id, name: $table_name})
        MERGE (c:Column {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            table_name: $table_name,
            name: $column_name
        })
        ON CREATE SET
            c.description = $description,
            c.data_type = $data_type,
            c.ordinal_position = $ordinal_position,
            c.is_nullable = $is_nullable,
            c.created_at = datetime()
        ON MATCH SET
            c.description = $description,
            c.data_type = $data_type,
            c.ordinal_position = $ordinal_position,
            c.is_nullable = $is_nullable,
            c.updated_at = datetime()
        MERGE (t)-[:HAS_COLUMN]->(c)
        RETURN c
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "column_name": column_data.get("column_name") or column_data.get("name"),
            "description": column_data.get("description"),
            "data_type": column_data.get("data_type"),
            "ordinal_position": column_data.get("ordinal_position") or 0,
            "is_nullable": column_data.get("is_nullable", True)
        }
        
        result = self._execute_query(query, params)
        return result[0]["c"] if result else None
    
    def create_relationship(self, tenant_id: str, source_table: str, source_column: str, 
                          target_table: str, target_column: str, 
                          confidence: float, detection_method: str,
                          relationship_type: str = None, weight: float = None,
                          metadata: Dict = None) -> Dict:
        """
        Create a relationship between two columns with weight and metadata.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            confidence: Confidence score (0.0-1.0)
            detection_method: Method used to detect relationship
            relationship_type: Type of relationship (one-to-many, etc.)
            weight: Edge weight for path finding (defaults to confidence)
            metadata: Additional metadata about the relationship
            
        Returns:
            Created relationship data
        """
        # If weight is not provided, use confidence
        if weight is None:
            weight = confidence
            
        # Initialize metadata - Neo4j doesn't accept Dict objects directly
        # Convert dict to JSON string to avoid Neo4j type errors
        metadata_str = None
        if metadata:
            import json
            metadata_str = json.dumps(metadata)
        
        query = """
        MATCH (source:Column {tenant_id: $tenant_id, table_name: $source_table, name: $source_column})
        MATCH (target:Column {tenant_id: $tenant_id, table_name: $target_table, name: $target_column})
        MERGE (source)-[r:LIKELY_REFERENCES {
            tenant_id: $tenant_id
        }]->(target)
        ON CREATE SET
            r.confidence = $confidence,
            r.detection_method = $detection_method,
            r.relationship_type = $relationship_type,
            r.weight = $weight,
            r.usage_count = 0,
            r.metadata_str = $metadata_str,
            r.created_at = datetime()
        ON MATCH SET
            r.confidence = CASE 
                WHEN r.is_verified = true THEN r.confidence 
                ELSE $confidence 
            END,
            r.detection_method = CASE 
                WHEN r.is_verified = true THEN r.detection_method 
                ELSE $detection_method 
            END,
            r.relationship_type = CASE
                WHEN r.relationship_type IS NULL THEN $relationship_type
                ELSE r.relationship_type
            END,
            r.weight = CASE
                WHEN $weight > r.weight THEN $weight
                ELSE r.weight
            END,
            r.metadata_str = CASE
                WHEN r.metadata_str IS NULL THEN $metadata_str
                ELSE r.metadata_str
            END,
            r.updated_at = datetime()
        RETURN source, r, target
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "confidence": confidence,
            "detection_method": detection_method,
            "relationship_type": relationship_type,
            "weight": weight,
            "metadata_str": metadata_str
        }
        
        result = self._execute_query(query, params)
        return result[0] if result else None
        
    def update_relationship_weight(self, tenant_id: str, source_table: str, source_column: str,
                                  target_table: str, target_column: str, weight_adjustment: float) -> Dict:
        """
        Update relationship weight based on usage or new information.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            weight_adjustment: Amount to adjust weight
            
        Returns:
            Updated relationship data
        """
        query = """
        MATCH (source:Column {tenant_id: $tenant_id, table_name: $source_table, name: $source_column})
        MATCH (target:Column {tenant_id: $tenant_id, table_name: $target_table, name: $target_column})
        MATCH (source)-[r:LIKELY_REFERENCES]->(target)
        SET r.weight = r.weight + $weight_adjustment,
            r.usage_count = COALESCE(r.usage_count, 0) + 1,
            r.last_used = datetime()
        RETURN source, r, target
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "weight_adjustment": weight_adjustment
        }
        
        result = self._execute_query(query, params)
        return result[0] if result else None
    
    def verify_relationship(self, tenant_id: str, source_table: str, source_column: str,
                          target_table: str, target_column: str,
                          verified: bool, verified_by: str) -> Dict:
        """
        Mark a relationship as verified.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            verified: Verification status
            verified_by: User ID who verified
            
        Returns:
            Updated relationship data
        """
        query = """
        MATCH (source:Column {tenant_id: $tenant_id, table_name: $source_table, name: $source_column})
        MATCH (target:Column {tenant_id: $tenant_id, table_name: $target_table, name: $target_column})
        MATCH (source)-[r:LIKELY_REFERENCES]->(target)
        SET r.is_verified = $verified,
            r.verified_by = $verified_by,
            r.verified_at = datetime()
        RETURN source, r, target
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table,
            "target_column": target_column,
            "verified": verified,
            "verified_by": verified_by
        }
        
        result = self._execute_query(query, params)
        return result[0] if result else None
    
    def get_tables_for_tenant(self, tenant_id: str) -> List[Dict]:
        """
        Get all tables for a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            List of table data
        """
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})
        RETURN t
        ORDER BY t.name
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return [record["t"] for record in result]
    
    def get_columns_for_table(self, tenant_id: str, table_name: str) -> List[Dict]:
        """
        Get all columns for a table.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            
        Returns:
            List of column data
        """
        query = """
        MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})-[:HAS_COLUMN]->(c:Column)
        RETURN c
        ORDER BY c.ordinal_position
        """
        
        params = {
            "tenant_id": tenant_id,
            "table_name": table_name
        }
        
        result = self._execute_query(query, params)
        return [record["c"] for record in result]
    
    def get_relationships_for_table(self, tenant_id: str, table_name: str) -> List[Dict]:
        """
        Get all relationships involving a table.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            
        Returns:
            List of relationship data
        """
        query = """
        MATCH (source:Column {tenant_id: $tenant_id, table_name: $table_name})-[r:LIKELY_REFERENCES]->(target:Column)
        RETURN source, r, target
        UNION
        MATCH (source:Column)-[r:LIKELY_REFERENCES]->(target:Column {tenant_id: $tenant_id, table_name: $table_name})
        RETURN source, r, target
        """
        
        params = {
            "tenant_id": tenant_id,
            "table_name": table_name
        }
        
        return self._execute_query(query, params)
    
    def find_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                      min_confidence: float = 0.5, strategy: str = "default", 
                      max_paths: int = 3, max_hops: int = 5) -> List[Dict]:
        """
        Find possible join paths between two tables with multiple strategies.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            target_table: Target table name
            min_confidence: Minimum confidence threshold
            strategy: Path finding strategy:
                - "default": Shortest path with highest confidence
                - "weighted": Path with highest total weight
                - "usage": Path with highest usage count
                - "verified": Prioritize verified relationships
                - "all": Return multiple path strategies
            max_paths: Maximum number of paths to return per strategy
            max_hops: Maximum number of hops in path
            
        Returns:
            List of possible paths with details
        """
        strategies = {
            "default": self._find_default_join_paths,
            "weighted": self._find_weighted_join_paths,
            "usage": self._find_usage_based_join_paths,
            "verified": self._find_verified_join_paths,
            "all": self._find_all_join_paths
        }
        
        # Use the specified strategy or default
        finder = strategies.get(strategy, self._find_default_join_paths)
        
        return finder(tenant_id, source_table, target_table, min_confidence, max_paths, max_hops)
    
    def _find_default_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                               min_confidence: float, max_paths: int, max_hops: int) -> List[Dict]:
        """Find join paths using default strategy (shortest path with highest confidence)"""
        query = """
        MATCH (source:Table {tenant_id: $tenant_id, name: $source_table})
        MATCH (target:Table {tenant_id: $tenant_id, name: $target_table})
        MATCH path = shortestPath(
            (source)-[:HAS_COLUMN]->(:Column)-[:LIKELY_REFERENCES*1..{max_hops}]->(:Column)<-[:HAS_COLUMN]-(target)
        )
        WHERE all(r in relationships(path) WHERE (r.confidence >= $min_confidence OR r.is_verified = true))
        WITH path,
            [n in nodes(path) WHERE n:Column | n.name] as columns,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES'] as refs,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | r.confidence] as confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | COALESCE(r.weight, r.confidence)] as weights
        RETURN path, 
            columns,
            confidences,
            weights,
            reduce(s = 1.0, r IN refs | s * r.confidence) as path_confidence,
            reduce(s = 0.0, w IN weights | s + w) as path_weight,
            length(path) as path_length,
            'default' as strategy
        ORDER BY path_length ASC, path_confidence DESC
        LIMIT {max_paths}
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "target_table": target_table,
            "min_confidence": min_confidence,
            "max_hops": max_hops,
            "max_paths": max_paths
        }
        
        results = self._execute_query(query, params)
        return self._format_path_results(results)
    
    def _find_weighted_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                                min_confidence: float, max_paths: int, max_hops: int) -> List[Dict]:
        """Find join paths using weighted strategy (highest total weight)"""
        query = """
        MATCH (source:Table {tenant_id: $tenant_id, name: $source_table})
        MATCH (target:Table {tenant_id: $tenant_id, name: $target_table})
        MATCH path = (source)-[:HAS_COLUMN]->(:Column)-[:LIKELY_REFERENCES*1..{max_hops}]->(:Column)<-[:HAS_COLUMN]-(target)
        WHERE all(r in relationships(path) WHERE (r.confidence >= $min_confidence OR r.is_verified = true))
        WITH path,
            [n in nodes(path) WHERE n:Column | n.name] as columns,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES'] as refs,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | r.confidence] as confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | COALESCE(r.weight, r.confidence)] as weights
        RETURN path, 
            columns,
            confidences,
            weights,
            reduce(s = 1.0, r IN refs | s * r.confidence) as path_confidence,
            reduce(s = 0.0, w IN weights | s + w) as path_weight,
            length(path) as path_length,
            'weighted' as strategy
        ORDER BY path_weight DESC, path_length ASC
        LIMIT {max_paths}
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "target_table": target_table,
            "min_confidence": min_confidence,
            "max_hops": max_hops,
            "max_paths": max_paths
        }
        
        results = self._execute_query(query, params)
        return self._format_path_results(results)
    
    def _find_usage_based_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                                   min_confidence: float, max_paths: int, max_hops: int) -> List[Dict]:
        """Find join paths using usage-based strategy (most frequently used)"""
        query = """
        MATCH (source:Table {tenant_id: $tenant_id, name: $source_table})
        MATCH (target:Table {tenant_id: $tenant_id, name: $target_table})
        MATCH path = (source)-[:HAS_COLUMN]->(:Column)-[:LIKELY_REFERENCES*1..{max_hops}]->(:Column)<-[:HAS_COLUMN]-(target)
        WHERE all(r in relationships(path) WHERE (r.confidence >= $min_confidence OR r.is_verified = true))
        WITH path,
            [n in nodes(path) WHERE n:Column | n.name] as columns,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES'] as refs,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | r.confidence] as confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | COALESCE(r.usage_count, 0)] as usage_counts
        RETURN path, 
            columns,
            confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | COALESCE(r.weight, r.confidence)] as weights,
            reduce(s = 1.0, r IN refs | s * r.confidence) as path_confidence,
            reduce(s = 0, c IN usage_counts | s + c) as total_usage,
            length(path) as path_length,
            'usage' as strategy
        ORDER BY total_usage DESC, path_length ASC, path_confidence DESC
        LIMIT {max_paths}
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "target_table": target_table,
            "min_confidence": min_confidence,
            "max_hops": max_hops,
            "max_paths": max_paths
        }
        
        results = self._execute_query(query, params)
        return self._format_path_results(results)
    
    def _find_verified_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                                min_confidence: float, max_paths: int, max_hops: int) -> List[Dict]:
        """Find join paths prioritizing verified relationships"""
        query = """
        MATCH (source:Table {tenant_id: $tenant_id, name: $source_table})
        MATCH (target:Table {tenant_id: $tenant_id, name: $target_table})
        MATCH path = (source)-[:HAS_COLUMN]->(:Column)-[:LIKELY_REFERENCES*1..{max_hops}]->(:Column)<-[:HAS_COLUMN]-(target)
        WHERE all(r in relationships(path) WHERE (r.confidence >= $min_confidence OR r.is_verified = true))
        WITH path,
            [n in nodes(path) WHERE n:Column | n.name] as columns,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES'] as refs,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | r.confidence] as confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | CASE WHEN r.is_verified = true THEN 1 ELSE 0 END] as is_verified
        RETURN path, 
            columns,
            confidences,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | COALESCE(r.weight, r.confidence)] as weights,
            reduce(s = 1.0, r IN refs | s * r.confidence) as path_confidence,
            reduce(s = 0, v IN is_verified | s + v) as verified_count,
            length(path) as path_length,
            'verified' as strategy
        ORDER BY verified_count DESC, path_length ASC, path_confidence DESC
        LIMIT {max_paths}
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "target_table": target_table,
            "min_confidence": min_confidence,
            "max_hops": max_hops,
            "max_paths": max_paths
        }
        
        results = self._execute_query(query, params)
        return self._format_path_results(results)
    
    def _find_all_join_paths(self, tenant_id: str, source_table: str, target_table: str,
                           min_confidence: float, max_paths: int, max_hops: int) -> List[Dict]:
        """Find join paths using all strategies"""
        # Get paths from all strategies
        default_paths = self._find_default_join_paths(
            tenant_id, source_table, target_table, min_confidence, max_paths, max_hops
        )
        weighted_paths = self._find_weighted_join_paths(
            tenant_id, source_table, target_table, min_confidence, max_paths, max_hops
        )
        usage_paths = self._find_usage_based_join_paths(
            tenant_id, source_table, target_table, min_confidence, max_paths, max_hops
        )
        verified_paths = self._find_verified_join_paths(
            tenant_id, source_table, target_table, min_confidence, max_paths, max_hops
        )
        
        # Combine all paths and add strategy information
        all_paths = default_paths + weighted_paths + usage_paths + verified_paths
        
        # Sort by combined score (this example prioritizes verified and weighted paths)
        # Could be customized based on specific prioritization needs
        return sorted(all_paths, key=lambda p: (
            p.get("verified_count", 0) * 10 +
            p.get("path_weight", 0) * 5 + 
            p.get("path_confidence", 0) * 3 - 
            p.get("path_length", 10)
        ), reverse=True)[:max_paths]
    
    def _format_path_results(self, results: List[Dict]) -> List[Dict]:
        """Format path results into a standardized structure"""
        formatted_paths = []
        
        for result in results:
            # Extract path nodes and relationships
            path = result.get("path")
            if not path:
                continue
                
            columns = result.get("columns", [])
            confidences = result.get("confidences", [])
            weights = result.get("weights", [])
            
            # Format join conditions
            join_conditions = []
            join_tables = []
            
            # Extract tables and join conditions from path
            table_nodes = [n for n in path.nodes if hasattr(n, "labels") and "Table" in n.labels]
            for i in range(len(table_nodes) - 1):
                source_table = table_nodes[i]["name"]
                target_table = table_nodes[i + 1]["name"]
                
                # Find columns connecting these tables
                source_col = None
                target_col = None
                
                # Simple implementation - in reality would need to analyze path structure
                for j in range(len(columns) - 1):
                    if columns[j].split(".")[0] == source_table and columns[j+1].split(".")[0] == target_table:
                        source_col = columns[j]
                        target_col = columns[j+1]
                        break
                
                if source_col and target_col:
                    join_conditions.append(f"{source_col} = {target_col}")
                    join_tables.append((source_table, target_table))
            
            # Create formatted path object
            formatted_path = {
                "path_id": str(hash(str(path))),
                "source_table": table_nodes[0]["name"] if table_nodes else "",
                "target_table": table_nodes[-1]["name"] if table_nodes else "",
                "join_conditions": join_conditions,
                "join_tables": join_tables,
                "path_length": result.get("path_length", 0),
                "path_confidence": result.get("path_confidence", 0),
                "path_weight": result.get("path_weight", 0),
                "verified_count": result.get("verified_count", 0),
                "strategy": result.get("strategy", "unknown"),
                "columns": columns,
                "confidences": confidences,
                "weights": weights
            }
            
            formatted_paths.append(formatted_path)
            
        return formatted_paths
        
    def get_datasets_for_tenant(self, tenant_id: str) -> List[Dict]:
        """
        Get all datasets for a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            List of dataset data
        """
        query = """
        MATCH (t:Tenant {id: $tenant_id})-[:OWNS]->(d:Dataset)
        RETURN d
        ORDER BY d.name
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return [record["d"] for record in result]
        
    # Backward compatibility for existing code
    def find_join_path(self, tenant_id: str, source_table: str, target_table: str, 
                     min_confidence: float = 0.5) -> List[Dict]:
        """
        Find possible join paths between two tables (legacy method).
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            target_table: Target table name
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of possible paths
        """
        return self.find_join_paths(tenant_id, source_table, target_table, min_confidence)
    
    def get_schema_summary(self, tenant_id: str) -> Dict:
        """
        Get a summary of the schema for a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            Schema summary statistics
        """
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        OPTIONAL MATCH (t)-[:OWNS]->(d:Dataset)
        OPTIONAL MATCH (d)-[:CONTAINS]->(table:Table)
        OPTIONAL MATCH (table)-[:HAS_COLUMN]->(c:Column)
        OPTIONAL MATCH (c1:Column {tenant_id: $tenant_id})-[r:LIKELY_REFERENCES]->(c2:Column {tenant_id: $tenant_id})
        
        RETURN 
            count(DISTINCT d) as dataset_count,
            count(DISTINCT table) as table_count,
            count(DISTINCT c) as column_count,
            count(DISTINCT r) as relationship_count,
            sum(CASE WHEN r.is_verified = true THEN 1 ELSE 0 END) as verified_relationship_count
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return result[0] if result else {
            "dataset_count": 0,
            "table_count": 0,
            "column_count": 0,
            "relationship_count": 0,
            "verified_relationship_count": 0
        }
        
    def update_table_metadata(self, tenant_id: str, table_name: str, metadata: Dict) -> Dict:
        """
        Update metadata for a table.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            metadata: Metadata to update
            
        Returns:
            Updated table data
        """
        # Create SET clause dynamically based on metadata keys
        set_clauses = []
        for key in metadata:
            set_clauses.append(f"t.{key} = ${key}")
        
        # Always update the last_modified timestamp
        set_clauses.append("t.last_modified = datetime()")
        
        query = f"""
        MATCH (t:Table {{tenant_id: $tenant_id, name: $table_name}})
        SET {', '.join(set_clauses)}
        RETURN t
        """
        
        # Add tenant_id and table_name to params
        params = metadata.copy()
        params["tenant_id"] = tenant_id
        params["table_name"] = table_name
        
        result = self._execute_query(query, params)
        return result[0]["t"] if result else None
    
    def update_column_metadata(self, tenant_id: str, table_name: str, column_name: str, metadata: Dict) -> Dict:
        """
        Update metadata for a column.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            column_name: Column name
            metadata: Metadata to update
            
        Returns:
            Updated column data
        """
        # Create SET clause dynamically based on metadata keys
        set_clauses = []
        for key in metadata:
            set_clauses.append(f"c.{key} = ${key}")
        
        # Always update the last_modified timestamp
        set_clauses.append("c.last_modified = datetime()")
        
        query = f"""
        MATCH (c:Column {{tenant_id: $tenant_id, table_name: $table_name, name: $column_name}})
        SET {', '.join(set_clauses)}
        RETURN c
        """
        
        # Add tenant_id, table_name, and column_name to params
        params = metadata.copy()
        params["tenant_id"] = tenant_id
        params["table_name"] = table_name
        params["column_name"] = column_name
        
        result = self._execute_query(query, params)
        return result[0]["c"] if result else None
    
    def update_dataset_metadata(self, tenant_id: str, dataset_id: str, metadata: Dict) -> Dict:
        """
        Update metadata for a dataset.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            metadata: Metadata to update
            
        Returns:
            Updated dataset data
        """
        # Create SET clause dynamically based on metadata keys
        set_clauses = []
        for key in metadata:
            set_clauses.append(f"d.{key} = ${key}")
        
        # Always update the last_modified timestamp
        set_clauses.append("d.last_modified = datetime()")
        
        query = f"""
        MATCH (d:Dataset {{tenant_id: $tenant_id, name: $dataset_id}})
        SET {', '.join(set_clauses)}
        RETURN d
        """
        
        # Add tenant_id and dataset_id to params
        params = metadata.copy()
        params["tenant_id"] = tenant_id
        params["dataset_id"] = dataset_id
        
        result = self._execute_query(query, params)
        return result[0]["d"] if result else None
    
    def store_business_glossary(self, tenant_id: str, dataset_id: str, glossary: str, metadata: Dict = None) -> Dict:
        """
        Store business glossary for a dataset.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            glossary: Business glossary text
            metadata: Additional metadata
            
        Returns:
            Created glossary node data
        """
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        MERGE (g:BusinessGlossary {tenant_id: $tenant_id, dataset_id: $dataset_id})
        ON CREATE SET
            g.content = $glossary,
            g.created_at = datetime()
        ON MATCH SET
            g.content = $glossary,
            g.updated_at = datetime()
        
        WITH g
        
        UNWIND keys($metadata) as key
        SET g[key] = $metadata[key]
        
        MERGE (d)-[:HAS_GLOSSARY]->(g)
        RETURN g
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "glossary": glossary,
            "metadata": metadata or {}
        }
        
        result = self._execute_query(query, params)
        return result[0]["g"] if result else None
    
    def store_sample_queries(self, tenant_id: str, queries: str, metadata: Dict = None) -> Dict:
        """
        Store sample queries for a tenant.
        
        Args:
            tenant_id: Tenant ID
            queries: Sample SQL queries
            metadata: Additional metadata
            
        Returns:
            Created sample queries node data
        """
        query = """
        MATCH (t:Tenant {id: $tenant_id})
        MERGE (sq:SampleQueries {tenant_id: $tenant_id})
        ON CREATE SET
            sq.content = $queries,
            sq.created_at = datetime()
        ON MATCH SET
            sq.content = $queries,
            sq.updated_at = datetime()
        
        WITH sq
        
        UNWIND keys($metadata) as key
        SET sq[key] = $metadata[key]
        
        MERGE (t)-[:HAS_SAMPLE_QUERIES]->(sq)
        RETURN sq
        """
        
        params = {
            "tenant_id": tenant_id,
            "queries": queries,
            "metadata": metadata or {}
        }
        
        result = self._execute_query(query, params)
        return result[0]["sq"] if result else None
    
    def record_workflow_status(self, tenant_id: str, dataset_id: str, workflow_name: str, 
                             status: str, metadata: Dict = None) -> Dict:
        """
        Record workflow execution status.
        
        Args:
            tenant_id: Tenant ID
            dataset_id: Dataset ID
            workflow_name: Workflow name
            status: Workflow status (completed, failed, etc.)
            metadata: Additional metadata
            
        Returns:
            Created workflow status node data
        """
        query = """
        MATCH (d:Dataset {tenant_id: $tenant_id, name: $dataset_id})
        CREATE (ws:WorkflowStatus {
            tenant_id: $tenant_id,
            dataset_id: $dataset_id,
            workflow_name: $workflow_name,
            status: $status,
            created_at: datetime()
        })
        
        WITH ws
        
        UNWIND keys($metadata) as key
        SET ws[key] = $metadata[key]
        
        MERGE (d)-[:HAS_WORKFLOW_STATUS]->(ws)
        RETURN ws
        """
        
        params = {
            "tenant_id": tenant_id,
            "dataset_id": dataset_id,
            "workflow_name": workflow_name,
            "status": status,
            "metadata": metadata or {}
        }
        
        result = self._execute_query(query, params)
        return result[0]["ws"] if result else None
        
    def get_business_glossary(self, tenant_id: str) -> Dict:
        """
        Get the business glossary for a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            Business glossary node data or None if not found
        """
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})
        RETURN g LIMIT 1
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return result[0]["g"] if result else None
    
    def get_glossary_terms(self, tenant_id: str) -> List[Dict]:
        """
        Get all glossary terms for a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            List of glossary term nodes
        """
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_TERM]->(t:GlossaryTerm)
        RETURN t
        ORDER BY t.name
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return [record["t"] for record in result]
        
    def get_table_details(self, tenant_id: str, table_name: str) -> Dict:
        """
        Get detailed information about a specific table.
        
        Args:
            tenant_id: Tenant ID
            table_name: Table name
            
        Returns:
            Dictionary with table details including columns
        """
        query = """
        MATCH (t:Table {tenant_id: $tenant_id, name: $table_name})
        RETURN t
        """
        
        params = {
            "tenant_id": tenant_id,
            "table_name": table_name
        }
        
        result = self._execute_query(query, params)
        if not result:
            return None
            
        table = result[0]["t"]
        
        # Get columns
        columns = self.get_columns_for_table(tenant_id, table_name)
        
        # Add columns to table details
        table_details = dict(table)
        table_details["columns"] = columns
        
        return table_details
    
    def get_glossary_term_details(self, tenant_id: str, term_name: str) -> Dict:
        """
        Get detailed information about a specific glossary term including mappings.
        
        Args:
            tenant_id: Tenant ID
            term_name: Term name
            
        Returns:
            Dictionary with term details including mappings to tables/columns
        """
        query = """
        MATCH (t:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
        OPTIONAL MATCH (t)-[:MAPS_TO]->(entity)
        RETURN t,
            collect(distinct case when 'Table' in labels(entity) then entity.name else null end) AS mapped_tables,
            collect(distinct case when 'Column' in labels(entity) then {table: entity.table_name, column: entity.name} else null end) AS mapped_columns
        """
        
        params = {
            "tenant_id": tenant_id,
            "term_name": term_name
        }
        
        result = self._execute_query(query, params)
        if not result:
            return None
            
        record = result[0]
        term = record["t"]
        
        # Filter out None values
        mapped_tables = [t for t in record["mapped_tables"] if t is not None]
        mapped_columns = [c for c in record["mapped_columns"] if c is not None]
        
        term_details = dict(term)
        term_details["mapped_tables"] = mapped_tables
        term_details["mapped_columns"] = mapped_columns
        
        return term_details
    
    def search_glossary_terms(self, tenant_id: str, search_text: str) -> List[Dict]:
        """
        Search for glossary terms that match the search text.
        
        Args:
            tenant_id: Tenant ID
            search_text: Text to search for in term names and definitions
            
        Returns:
            List of matching glossary terms
        """
        # Prepare search text for CONTAINS clause (case-insensitive)
        search_lower = search_text.lower()
        
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_TERM]->(t:GlossaryTerm)
        WHERE toLower(t.name) CONTAINS $search_text OR toLower(t.definition) CONTAINS $search_text
        RETURN t
        ORDER BY t.name
        """
        
        params = {
            "tenant_id": tenant_id,
            "search_text": search_lower
        }
        
        result = self._execute_query(query, params)
        return [record["t"] for record in result]
    
    def get_glossary_metrics(self, tenant_id: str) -> List[Dict]:
        """
        Get all business metrics from the glossary.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            List of business metric nodes
        """
        query = """
        MATCH (g:BusinessGlossary {tenant_id: $tenant_id})-[:HAS_METRIC]->(m:BusinessMetric)
        RETURN m
        ORDER BY m.name
        """
        
        params = {"tenant_id": tenant_id}
        
        result = self._execute_query(query, params)
        return [record["m"] for record in result]
    
    def get_term_relationships(self, tenant_id: str, term_name: str) -> List[Dict]:
        """
        Get relationships between a glossary term and other terms.
        
        Args:
            tenant_id: Tenant ID
            term_name: Term name
            
        Returns:
            List of related terms with relationship type
        """
        query = """
        MATCH (t1:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})-[r:RELATED_TO]->(t2:GlossaryTerm)
        RETURN t2.name AS related_term, r.type AS relationship_type
        UNION
        MATCH (t2:GlossaryTerm)-[r:RELATED_TO]->(t1:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
        RETURN t2.name AS related_term, r.type AS relationship_type
        """
        
        params = {
            "tenant_id": tenant_id,
            "term_name": term_name
        }
        
        return self._execute_query(query, params)
        
    def update_term_mapping_usage(self, tenant_id: str, term_name: str, table_name: str) -> Dict:
        """
        Update usage metrics for a business term mapping to a table.
        Increments usage count and updates weight for the mapping.
        
        Args:
            tenant_id: Tenant ID
            term_name: Business glossary term
            table_name: Table name the term maps to
            
        Returns:
            Updated term mapping data
        """
        query = """
        MATCH (t:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})
        MATCH (table:Table {tenant_id: $tenant_id, name: $table_name})
        MERGE (t)-[r:MAPS_TO]->(table)
        ON CREATE SET
            r.usage_count = 1,
            r.weight = 1.0,
            r.created_at = datetime()
        ON MATCH SET
            r.usage_count = COALESCE(r.usage_count, 0) + 1,
            r.last_used_at = datetime(),
            // Increase weight slightly with each usage
            r.weight = CASE 
                WHEN r.weight IS NULL THEN 1.0
                ELSE r.weight + 0.05
            END
        
        WITH t, r, table
        
        // Update term's overall usage metrics
        SET t.total_usage_count = COALESCE(t.total_usage_count, 0) + 1,
            t.last_used_at = datetime()
            
        RETURN t, r, table
        """
        
        params = {
            "tenant_id": tenant_id,
            "term_name": term_name,
            "table_name": table_name
        }
        
        result = self._execute_query(query, params)
        return result[0] if result else None
    
    def get_term_mapping_stats(self, tenant_id: str, term_name: str = None) -> List[Dict]:
        """
        Get usage statistics for term mappings.
        
        Args:
            tenant_id: Tenant ID
            term_name: Optional term name to filter by
            
        Returns:
            List of term mapping statistics
        """
        if term_name:
            query = """
            MATCH (t:GlossaryTerm {tenant_id: $tenant_id, name: $term_name})-[r:MAPS_TO]->(entity)
            RETURN 
                t.name as term_name,
                CASE WHEN 'Table' IN labels(entity) THEN entity.name ELSE null END as table_name,
                CASE WHEN 'Column' IN labels(entity) THEN entity.name ELSE null END as column_name,
                CASE WHEN 'Column' IN labels(entity) THEN entity.table_name ELSE null END as column_table,
                COALESCE(r.usage_count, 0) as usage_count,
                COALESCE(r.weight, 1.0) as weight,
                r.last_used_at as last_used
            ORDER BY r.usage_count DESC
            """
            
            params = {
                "tenant_id": tenant_id,
                "term_name": term_name
            }
        else:
            query = """
            MATCH (t:GlossaryTerm {tenant_id: $tenant_id})-[r:MAPS_TO]->(entity)
            RETURN 
                t.name as term_name,
                CASE WHEN 'Table' IN labels(entity) THEN entity.name ELSE null END as table_name,
                CASE WHEN 'Column' IN labels(entity) THEN entity.name ELSE null END as column_name,
                CASE WHEN 'Column' IN labels(entity) THEN entity.table_name ELSE null END as column_table,
                COALESCE(r.usage_count, 0) as usage_count,
                COALESCE(r.weight, 1.0) as weight,
                r.last_used_at as last_used
            ORDER BY r.usage_count DESC
            LIMIT 100
            """
            
            params = {
                "tenant_id": tenant_id
            }
        
        return self._execute_query(query, params)
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
                          confidence: float, detection_method: str) -> Dict:
        """
        Create a relationship between two columns.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            source_column: Source column name
            target_table: Target table name
            target_column: Target column name
            confidence: Confidence score (0.0-1.0)
            detection_method: Method used to detect relationship
            
        Returns:
            Created relationship data
        """
        query = """
        MATCH (source:Column {tenant_id: $tenant_id, table_name: $source_table, name: $source_column})
        MATCH (target:Column {tenant_id: $tenant_id, table_name: $target_table, name: $target_column})
        MERGE (source)-[r:LIKELY_REFERENCES {
            tenant_id: $tenant_id
        }]->(target)
        ON CREATE SET
            r.confidence = $confidence,
            r.detection_method = $detection_method,
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
            "detection_method": detection_method
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
    
    def find_join_path(self, tenant_id: str, source_table: str, target_table: str, 
                     min_confidence: float = 0.5) -> List[Dict]:
        """
        Find possible join paths between two tables.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            target_table: Target table name
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of possible paths
        """
        query = """
        MATCH (source:Table {tenant_id: $tenant_id, name: $source_table})
        MATCH (target:Table {tenant_id: $tenant_id, name: $target_table})
        MATCH path = shortestPath(
            (source)-[:HAS_COLUMN]->(:Column)-[:LIKELY_REFERENCES*1..5]->(:Column)<-[:HAS_COLUMN]-(target)
        )
        WHERE all(r in relationships(path) WHERE (r.confidence >= $min_confidence OR r.is_verified = true))
        RETURN path,
            [n in nodes(path) WHERE n:Column | n.name] as columns,
            [r in relationships(path) WHERE type(r) = 'LIKELY_REFERENCES' | r.confidence] as confidences
        LIMIT 5
        """
        
        params = {
            "tenant_id": tenant_id,
            "source_table": source_table,
            "target_table": target_table,
            "min_confidence": min_confidence
        }
        
        return self._execute_query(query, params)
    
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
"""
Graph Context Provider for text2sql integration.

Provides a unified interface for accessing semantic graph
data to enhance text2sql functionality.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from src.graph_storage.neo4j_client import Neo4jClient
from src.semantic_graph_builder.enhanced_glossary.utils.graph_context import GraphContextProvider

logger = logging.getLogger(__name__)


class SemanticGraphContextProvider:
    """
    Unified interface for accessing semantic graph context.
    
    This class serves as a bridge between the text2sql components
    and the semantic graph, providing standardized access methods
    and caching for performance.
    """
    
    def __init__(self, neo4j_client: Neo4jClient):
        """
        Initialize semantic graph context provider.
        
        Args:
            neo4j_client: Neo4j client for graph database access
        """
        self.neo4j_client = neo4j_client
        self.graph_context_provider = GraphContextProvider(neo4j_client)
        self._cache = {}
    
    def get_schema_context(self, tenant_id: str, include_glossary: bool = True) -> Dict[str, Any]:
        """
        Get comprehensive schema context for SQL generation.
        
        Args:
            tenant_id: Tenant ID
            include_glossary: Whether to include business glossary
            
        Returns:
            Dictionary with schema context information
        """
        cache_key = f"schema_context_{tenant_id}_{include_glossary}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Get tables
        tables = self._get_tables_with_columns(tenant_id)
        
        # Get business glossary terms
        glossary_terms = {}
        if include_glossary:
            glossary_terms = self._get_glossary_terms(tenant_id)
        
        # Compile context
        schema_context = {
            "tables": tables,
            "glossary_terms": glossary_terms,
            "metadata": {
                "generation_time": datetime.now().isoformat(),
                "tenant_id": tenant_id
            }
        }
        
        # Cache for performance
        self._cache[cache_key] = schema_context
        return schema_context
    
    def get_entity_resolution_context(self, tenant_id: str, entity_name: str) -> Dict[str, Any]:
        """
        Get context for entity resolution.
        
        Args:
            tenant_id: Tenant ID
            entity_name: Entity name to resolve
            
        Returns:
            Dictionary with entity resolution context
        """
        # First check if it's a business term
        term_info = self._search_glossary_term(tenant_id, entity_name)
        if term_info:
            return {
                "type": "business_term",
                "resolution": term_info
            }
        
        # Then check for table name match
        table_info = self._find_matching_tables(tenant_id, entity_name)
        if table_info:
            return {
                "type": "table",
                "resolution": table_info
            }
        
        # Then check for column name match
        column_info = self._find_matching_columns(tenant_id, entity_name)
        if column_info:
            return {
                "type": "column",
                "resolution": column_info
            }
        
        # Check for semantic concepts
        concept_info = self._find_matching_concepts(tenant_id, entity_name)
        if concept_info:
            return {
                "type": "semantic_concept",
                "resolution": concept_info
            }
        
        # No matches found
        return {
            "type": "unknown",
            "resolution": None
        }
    
    def get_attribute_resolution_context(self, tenant_id: str, attribute_name: str, table_hint: str = None) -> Dict[str, Any]:
        """
        Get context for attribute resolution.
        
        Args:
            tenant_id: Tenant ID
            attribute_name: Attribute name to resolve
            table_hint: Optional table name to restrict search
            
        Returns:
            Dictionary with attribute resolution context
        """
        # Check for direct column match
        column_info = self._find_matching_columns(tenant_id, attribute_name, table_hint)
        if column_info:
            return {
                "type": "column",
                "resolution": column_info
            }
        
        # Check for glossary term that maps to column
        term_info = self._search_glossary_term_with_column_mapping(tenant_id, attribute_name)
        if term_info:
            return {
                "type": "business_term_column",
                "resolution": term_info
            }
        
        # Check for derived attributes
        derived_info = self._find_derived_attributes(tenant_id, attribute_name)
        if derived_info:
            return {
                "type": "derived_attribute",
                "resolution": derived_info
            }
        
        # No matches found
        return {
            "type": "unknown",
            "resolution": None
        }
    
    def get_join_paths(self, tenant_id: str, source_table: str, target_table: str, 
                      min_confidence: float = 0.5, strategy: str = "default") -> List[Dict[str, Any]]:
        """
        Get possible join paths between tables.
        
        Args:
            tenant_id: Tenant ID
            source_table: Source table name
            target_table: Target table name
            min_confidence: Minimum confidence threshold (0.0-1.0)
            strategy: Path finding strategy
            
        Returns:
            List of possible join paths
        """
        cache_key = f"join_paths_{tenant_id}_{source_table}_{target_table}_{min_confidence}_{strategy}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        join_paths = self.neo4j_client.find_join_paths(
            tenant_id=tenant_id,
            source_table=source_table,
            target_table=target_table,
            min_confidence=min_confidence,
            strategy=strategy
        )
        
        # Cache for performance
        self._cache[cache_key] = join_paths
        return join_paths
    
    def get_semantic_concepts(self, tenant_id: str, entity_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get semantic concepts available for query resolution.
        
        Args:
            tenant_id: Tenant ID
            entity_names: Optional list of entity names to filter by
            
        Returns:
            Dictionary of semantic concepts
        """
        # Retrieve different types of semantic concepts
        concepts = {}
        
        # Get composite concepts
        composite_concepts = self._get_composite_concepts(tenant_id, entity_names)
        for name, concept in composite_concepts.items():
            concepts[name] = concept
        
        # Get business process concepts
        process_concepts = self._get_business_process_concepts(tenant_id, entity_names)
        for name, concept in process_concepts.items():
            concepts[name] = concept
        
        # Get relationship concepts
        relationship_concepts = self._get_relationship_concepts(tenant_id, entity_names)
        for name, concept in relationship_concepts.items():
            concepts[name] = concept
            
        # Get hierarchical concepts
        hierarchical_concepts = self._get_hierarchical_concepts(tenant_id, entity_names)
        for name, concept in hierarchical_concepts.items():
            concepts[name] = concept
        
        return concepts
    
    def get_graph_enhanced_context(self, tenant_id: str) -> Dict[str, Any]:
        """
        Get graph structure-aware context for query resolution.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            Dictionary with graph-enhanced context
        """
        cache_key = f"graph_enhanced_{tenant_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # Use the GraphContextProvider from semantic_graph_builder
        graph_context = self.graph_context_provider.get_graph_context(tenant_id)
        
        # Add additional information specific to text2sql
        graph_context["term_relationships"] = self._get_term_relationships(tenant_id)
        graph_context["frequently_used_terms"] = self._get_frequently_used_terms(tenant_id)
        
        # Cache for performance
        self._cache[cache_key] = graph_context
        return graph_context
    
    def clear_cache(self) -> None:
        """Clear the internal cache."""
        self._cache = {}
    
    def _get_tables_with_columns(self, tenant_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all tables with their columns."""
        tables = {}
        
        # Get all tables
        table_list = self.neo4j_client.get_tables_for_tenant(tenant_id)
        
        # For each table, get columns
        for table in table_list:
            table_name = table.get("name")
            if table_name:
                # Get columns
                columns = self.neo4j_client.get_columns_for_table(tenant_id, table_name)
                
                # Format table info
                tables[table_name] = {
                    "name": table_name,
                    "description": table.get("description", ""),
                    "dataset_id": table.get("dataset_id", ""),
                    "columns": columns
                }
        
        return tables
    
    def _get_glossary_terms(self, tenant_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all business glossary terms."""
        terms = {}
        
        term_list = self.neo4j_client.get_glossary_terms(tenant_id)
        
        for term in term_list:
            term_name = term.get("name")
            if term_name:
                # Get detailed term info
                term_info = self.neo4j_client.get_glossary_term_details(tenant_id, term_name)
                if term_info:
                    terms[term_name] = term_info
        
        return terms
    
    def _search_glossary_term(self, tenant_id: str, term_name: str) -> Dict[str, Any]:
        """Search for a glossary term."""
        # First try exact match
        term_info = self.neo4j_client.get_glossary_term_details(tenant_id, term_name)
        if term_info:
            return term_info
            
        # Then try partial match
        term_list = self.neo4j_client.search_glossary_terms(tenant_id, term_name)
        if term_list:
            # Get full details of first match
            first_match = term_list[0]
            term_info = self.neo4j_client.get_glossary_term_details(tenant_id, first_match.get("name"))
            if term_info:
                return term_info
        
        return None
    
    def _find_matching_tables(self, tenant_id: str, table_name: str) -> List[Dict[str, Any]]:
        """Find tables matching the name."""
        query = """
        MATCH (t:Table {tenant_id: $tenant_id})
        WHERE toLower(t.name) CONTAINS toLower($table_name)
        RETURN t
        ORDER BY 
            CASE WHEN toLower(t.name) = toLower($table_name) THEN 0
                 WHEN toLower(t.name) STARTS WITH toLower($table_name) THEN 1
                 ELSE 2 END,
            t.name
        LIMIT 5
        """
        
        try:
            result = self.neo4j_client._execute_query(query, {
                "tenant_id": tenant_id,
                "table_name": table_name
            })
            
            return [record.get("t") for record in result if "t" in record]
        except Exception as e:
            logger.error(f"Error finding matching tables: {e}")
            return []
    
    def _find_matching_columns(self, tenant_id: str, column_name: str, table_hint: str = None) -> List[Dict[str, Any]]:
        """Find columns matching the name."""
        if table_hint:
            query = """
            MATCH (t:Table {tenant_id: $tenant_id, name: $table_hint})-[:HAS_COLUMN]->(c:Column)
            WHERE toLower(c.name) CONTAINS toLower($column_name)
            RETURN c, t.name as table_name
            ORDER BY 
                CASE WHEN toLower(c.name) = toLower($column_name) THEN 0
                     WHEN toLower(c.name) STARTS WITH toLower($column_name) THEN 1
                     ELSE 2 END,
                c.name
            LIMIT 5
            """
            params = {
                "tenant_id": tenant_id,
                "column_name": column_name,
                "table_hint": table_hint
            }
        else:
            query = """
            MATCH (t:Table {tenant_id: $tenant_id})-[:HAS_COLUMN]->(c:Column)
            WHERE toLower(c.name) CONTAINS toLower($column_name)
            RETURN c, t.name as table_name
            ORDER BY 
                CASE WHEN toLower(c.name) = toLower($column_name) THEN 0
                     WHEN toLower(c.name) STARTS WITH toLower($column_name) THEN 1
                     ELSE 2 END,
                c.name
            LIMIT 5
            """
            params = {
                "tenant_id": tenant_id,
                "column_name": column_name
            }
        
        try:
            result = self.neo4j_client._execute_query(query, params)
            
            columns = []
            for record in result:
                if "c" in record and "table_name" in record:
                    column = record["c"]
                    column["table_name"] = record["table_name"]
                    columns.append(column)
            
            return columns
        except Exception as e:
            logger.error(f"Error finding matching columns: {e}")
            return []
    
    def _search_glossary_term_with_column_mapping(self, tenant_id: str, term_name: str) -> Dict[str, Any]:
        """Search for a glossary term that maps to columns."""
        query = """
        MATCH (t:GlossaryTerm {tenant_id: $tenant_id})-[:MAPS_TO]->(c:Column)
        WHERE toLower(t.name) CONTAINS toLower($term_name)
        RETURN t, collect({column: c.name, table: c.table_name}) as mapped_columns
        ORDER BY 
            CASE WHEN toLower(t.name) = toLower($term_name) THEN 0
                 WHEN toLower(t.name) STARTS WITH toLower($term_name) THEN 1
                 ELSE 2 END,
            t.name
        LIMIT 1
        """
        
        try:
            result = self.neo4j_client._execute_query(query, {
                "tenant_id": tenant_id,
                "term_name": term_name
            })
            
            if result and "t" in result[0] and "mapped_columns" in result[0]:
                term = result[0]["t"]
                term["mapped_columns"] = result[0]["mapped_columns"]
                return term
            
            return None
        except Exception as e:
            logger.error(f"Error searching glossary term with column mapping: {e}")
            return None
    
    def _find_matching_concepts(self, tenant_id: str, concept_name: str) -> Dict[str, Any]:
        """Find semantic concepts matching the name."""
        # Check for composite concepts
        query = """
        MATCH (cc:CompositeConcept {tenant_id: $tenant_id})
        WHERE toLower(cc.name) CONTAINS toLower($concept_name)
        RETURN cc
        UNION
        MATCH (bp:BusinessProcess {tenant_id: $tenant_id})
        WHERE toLower(bp.name) CONTAINS toLower($concept_name)
        RETURN bp as cc
        UNION
        MATCH (rc:RelationshipConcept {tenant_id: $tenant_id})
        WHERE toLower(rc.name) CONTAINS toLower($concept_name)
        RETURN rc as cc
        UNION
        MATCH (hc:HierarchicalConcept {tenant_id: $tenant_id})
        WHERE toLower(hc.name) CONTAINS toLower($concept_name)
        RETURN hc as cc
        ORDER BY 
            CASE WHEN toLower(cc.name) = toLower($concept_name) THEN 0
                 WHEN toLower(cc.name) STARTS WITH toLower($concept_name) THEN 1
                 ELSE 2 END,
            cc.name
        LIMIT 1
        """
        
        try:
            result = self.neo4j_client._execute_query(query, {
                "tenant_id": tenant_id,
                "concept_name": concept_name
            })
            
            return result[0]["cc"] if result and "cc" in result[0] else None
        except Exception as e:
            logger.error(f"Error finding matching concepts: {e}")
            return None
    
    def _find_derived_attributes(self, tenant_id: str, attribute_name: str) -> List[Dict[str, Any]]:
        """Find derived attributes matching the name."""
        # This method would look for calculated fields or virtual columns
        # For now, return an empty list as implementation depends on specific schema
        return []
    
    def _get_composite_concepts(self, tenant_id: str, entity_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get composite concepts."""
        if entity_names:
            entity_filter = " OR ".join([f"toLower(cc.name) CONTAINS toLower('{name}')" for name in entity_names])
            query = f"""
            MATCH (cc:CompositeConcept {{tenant_id: $tenant_id}})
            WHERE {entity_filter}
            RETURN cc
            """
        else:
            query = """
            MATCH (cc:CompositeConcept {tenant_id: $tenant_id})
            RETURN cc
            """
        
        try:
            result = self.neo4j_client._execute_query(query, {"tenant_id": tenant_id})
            
            concepts = {}
            for record in result:
                if "cc" in record:
                    concept = record["cc"]
                    concepts[concept.get("name")] = concept
            
            return concepts
        except Exception as e:
            logger.error(f"Error getting composite concepts: {e}")
            return {}
    
    def _get_business_process_concepts(self, tenant_id: str, entity_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get business process concepts."""
        if entity_names:
            entity_filter = " OR ".join([f"toLower(bp.name) CONTAINS toLower('{name}')" for name in entity_names])
            query = f"""
            MATCH (bp:BusinessProcess {{tenant_id: $tenant_id}})
            WHERE {entity_filter}
            RETURN bp
            """
        else:
            query = """
            MATCH (bp:BusinessProcess {tenant_id: $tenant_id})
            RETURN bp
            """
        
        try:
            result = self.neo4j_client._execute_query(query, {"tenant_id": tenant_id})
            
            concepts = {}
            for record in result:
                if "bp" in record:
                    concept = record["bp"]
                    concepts[concept.get("name")] = concept
            
            return concepts
        except Exception as e:
            logger.error(f"Error getting business process concepts: {e}")
            return {}
    
    def _get_relationship_concepts(self, tenant_id: str, entity_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get relationship concepts."""
        if entity_names:
            entity_filter = " OR ".join([f"toLower(rc.name) CONTAINS toLower('{name}')" for name in entity_names])
            query = f"""
            MATCH (rc:RelationshipConcept {{tenant_id: $tenant_id}})
            WHERE {entity_filter}
            RETURN rc
            """
        else:
            query = """
            MATCH (rc:RelationshipConcept {tenant_id: $tenant_id})
            RETURN rc
            """
        
        try:
            result = self.neo4j_client._execute_query(query, {"tenant_id": tenant_id})
            
            concepts = {}
            for record in result:
                if "rc" in record:
                    concept = record["rc"]
                    concepts[concept.get("name")] = concept
            
            return concepts
        except Exception as e:
            logger.error(f"Error getting relationship concepts: {e}")
            return {}
    
    def _get_hierarchical_concepts(self, tenant_id: str, entity_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Get hierarchical concepts."""
        if entity_names:
            entity_filter = " OR ".join([f"toLower(hc.name) CONTAINS toLower('{name}')" for name in entity_names])
            query = f"""
            MATCH (hc:HierarchicalConcept {{tenant_id: $tenant_id}})
            WHERE {entity_filter}
            RETURN hc
            """
        else:
            query = """
            MATCH (hc:HierarchicalConcept {tenant_id: $tenant_id})
            RETURN hc
            """
        
        try:
            result = self.neo4j_client._execute_query(query, {"tenant_id": tenant_id})
            
            concepts = {}
            for record in result:
                if "hc" in record:
                    concept = record["hc"]
                    concepts[concept.get("name")] = concept
            
            return concepts
        except Exception as e:
            logger.error(f"Error getting hierarchical concepts: {e}")
            return {}
    
    def _get_term_relationships(self, tenant_id: str) -> List[Dict[str, Any]]:
        """Get relationships between business glossary terms."""
        query = """
        MATCH (t1:GlossaryTerm {tenant_id: $tenant_id})-[r:RELATED_TO]->(t2:GlossaryTerm)
        RETURN t1.name as source_term, t2.name as target_term, r.type as relationship_type
        """
        
        try:
            return self.neo4j_client._execute_query(query, {"tenant_id": tenant_id})
        except Exception as e:
            logger.error(f"Error getting term relationships: {e}")
            return []
    
    def _get_frequently_used_terms(self, tenant_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most frequently used business glossary terms."""
        query = """
        MATCH (t:GlossaryTerm {tenant_id: $tenant_id})
        WHERE EXISTS(t.total_usage_count)
        RETURN t.name as term_name, t.definition as definition, t.total_usage_count as usage_count
        ORDER BY t.total_usage_count DESC
        LIMIT $limit
        """
        
        try:
            return self.neo4j_client._execute_query(query, {
                "tenant_id": tenant_id,
                "limit": limit
            })
        except Exception as e:
            logger.error(f"Error getting frequently used terms: {e}")
            return []
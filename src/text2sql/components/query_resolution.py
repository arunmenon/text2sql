import json
import logging
from typing import Dict, Any, Optional, List, Tuple

from src.llm.client import LLMClient
from src.graph_storage.neo4j_client import Neo4jClient
from src.text2sql.models import (
    StructuredQuery, ResolvedQuery, ResolvedEntity, 
    ResolvedAttribute, ResolvedConcept, QueryInterpretation
)

logger = logging.getLogger(__name__)

class QueryResolutionComponent:
    """Component for resolving queries against schema"""
    
    def __init__(self, neo4j_client: Neo4jClient, llm_client: LLMClient):
        """
        Initialize query resolution component.
        
        Args:
            neo4j_client: Neo4j client for schema access
            llm_client: LLM client for text generation
        """
        self.neo4j_client = neo4j_client
        self.llm_client = llm_client
        
    async def resolve_query(self, structured_query: StructuredQuery, tenant_id: str) -> ResolvedQuery:
        """
        Resolve parsed query against actual schema.
        
        Args:
            structured_query: Structured representation of the query
            tenant_id: Tenant ID
            
        Returns:
            Resolved query with connections to actual schema
        """
        # Get schema context
        schema_context = await self._get_schema_context(tenant_id, structured_query)
        
        # Resolve entities to actual tables
        resolved_entities = await self._resolve_entities(
            structured_query.main_entities, 
            schema_context
        )
        
        # Resolve attributes to actual columns
        resolved_attributes = await self._resolve_attributes(
            structured_query.attributes, 
            resolved_entities,
            schema_context
        )
        
        # Handle semantic concepts
        resolved_concepts = await self._resolve_semantic_concepts(
            structured_query.identified_ambiguities,
            schema_context
        )
        
        # Discover join paths between tables
        join_paths = await self._discover_join_paths(resolved_entities, tenant_id)
        
        # Generate multiple interpretations if highly ambiguous
        interpretations = await self._generate_interpretations(
            structured_query,
            resolved_entities,
            resolved_attributes,
            resolved_concepts,
            join_paths,
            schema_context
        )
        
        return ResolvedQuery(
            resolved_entities=resolved_entities,
            resolved_attributes=resolved_attributes,
            resolved_concepts=resolved_concepts,
            join_paths=join_paths,
            interpretations=interpretations,
            schema_context=schema_context
        )
    
    async def _get_schema_context(self, tenant_id: str, structured_query: StructuredQuery) -> Dict[str, Any]:
        """Get relevant schema context for this query"""
        # Extract potential table names from query
        potential_tables = structured_query.main_entities
        
        # Get schema information for these tables
        tables_info = {}
        for table in potential_tables:
            # Get basic table info
            try:
                table_info = self.neo4j_client.get_table_details(tenant_id, table)
                if table_info:
                    tables_info[table] = table_info
            except Exception as e:
                logger.warning(f"Error getting table details for {table}: {e}")
                
        # If we didn't find any tables, try to get all tables for tenant
        if not tables_info:
            try:
                all_tables = self.neo4j_client.get_tables_for_tenant(tenant_id)
                for table in all_tables:
                    table_name = table.get("name")
                    if table_name:
                        tables_info[table_name] = table
            except Exception as e:
                logger.warning(f"Error getting all tables for tenant {tenant_id}: {e}")
        
        # Get schema summary for context
        try:
            schema_summary = self.neo4j_client.get_schema_summary(tenant_id)
        except Exception as e:
            logger.warning(f"Error getting schema summary: {e}")
            schema_summary = {}
                
        return {
            "tables": tables_info,
            "summary": schema_summary,
            "tenant_id": tenant_id
        }
    
    async def _resolve_entities(self, entity_mentions: List[str], schema_context: Dict[str, Any]) -> Dict[str, ResolvedEntity]:
        """
        Map entity mentions to actual tables.
        
        Args:
            entity_mentions: List of entity mentions from NL query
            schema_context: Schema context for resolution
            
        Returns:
            Dictionary mapping entity mentions to resolved entities
        """
        resolved = {}
        
        # Get available tables
        available_tables = list(schema_context["tables"].keys())
        
        for entity in entity_mentions:
            # Direct match
            if entity in schema_context["tables"]:
                resolved[entity] = ResolvedEntity(
                    table_name=entity,
                    confidence=1.0,
                    resolution_method="direct_match"
                )
                continue
            
            # Try lowercase matching
            entity_lower = entity.lower()
            for table_name in available_tables:
                if table_name.lower() == entity_lower:
                    resolved[entity] = ResolvedEntity(
                        table_name=table_name,
                        confidence=0.9,
                        resolution_method="case_insensitive_match"
                    )
                    break
            
            # If still not resolved, try fuzzy matching using LLM
            if entity not in resolved:
                match_prompt = self._build_entity_matching_prompt(entity, schema_context)
                match_schema = {
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string"},
                        "confidence": {"type": "number"},
                        "reasoning": {"type": "string"}
                    }
                }
                
                match_result = await self.llm_client.generate_structured(match_prompt, match_schema)
                
                if match_result and "table_name" in match_result:
                    resolved[entity] = ResolvedEntity(
                        table_name=match_result["table_name"],
                        confidence=match_result.get("confidence", 0.7),
                        resolution_method="llm_fuzzy_match"
                    )
                
        return resolved
    
    def _build_entity_matching_prompt(self, entity: str, schema_context: Dict[str, Any]) -> str:
        """Build prompt for entity matching"""
        tables_info = "\n".join([
            f"- {table_name}: {table_info.get('description', 'No description')}"
            for table_name, table_info in schema_context["tables"].items()
        ])
        
        return f"""
        I'm trying to match the entity "{entity}" mentioned in a database query to an actual database table.
        
        Available tables:
        {tables_info}
        
        Please determine which table (if any) is the most likely match for "{entity}".
        Consider plural/singular forms, abbreviations, and semantic connections.
        
        Return a JSON object with:
        - table_name: The name of the matched table, or your best guess if no exact match
        - confidence: Your confidence in the match from 0.0 to 1.0
        - reasoning: Brief explanation of why you matched to this table
        """
    
    async def _resolve_attributes(
        self, attribute_mentions: List[str], 
        resolved_entities: Dict[str, ResolvedEntity],
        schema_context: Dict[str, Any]
    ) -> Dict[str, ResolvedAttribute]:
        """Resolve attribute mentions to actual columns"""
        resolved = {}
        
        # Get table names from resolved entities
        table_names = [entity.table_name for entity in resolved_entities.values()]
        
        # Collect columns for all tables
        table_columns = {}
        for table_name in table_names:
            if table_name in schema_context["tables"]:
                table_info = schema_context["tables"][table_name]
                columns = table_info.get("columns", [])
                table_columns[table_name] = columns
        
        # For each attribute mention
        for attribute in attribute_mentions:
            # Check direct matches across all tables
            found = False
            for table_name, columns in table_columns.items():
                for column in columns:
                    column_name = column.get("name") or column.get("column_name")
                    if not column_name:
                        continue
                        
                    if column_name.lower() == attribute.lower():
                        resolved[attribute] = ResolvedAttribute(
                            table_name=table_name,
                            column_name=column_name,
                            confidence=0.9,
                            resolution_method="direct_match"
                        )
                        found = True
                        break
                        
                if found:
                    break
            
            # If not found, use LLM to resolve
            if not found:
                match_prompt = self._build_attribute_matching_prompt(attribute, table_columns)
                match_schema = {
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string"},
                        "column_name": {"type": "string"},
                        "confidence": {"type": "number"},
                        "reasoning": {"type": "string"}
                    }
                }
                
                match_result = await self.llm_client.generate_structured(match_prompt, match_schema)
                
                if match_result and "table_name" in match_result and "column_name" in match_result:
                    resolved[attribute] = ResolvedAttribute(
                        table_name=match_result["table_name"],
                        column_name=match_result["column_name"],
                        confidence=match_result.get("confidence", 0.7),
                        resolution_method="llm_fuzzy_match"
                    )
                
        return resolved
    
    def _build_attribute_matching_prompt(self, attribute: str, table_columns: Dict[str, List[Dict]]) -> str:
        """Build prompt for attribute matching"""
        columns_info = []
        for table_name, columns in table_columns.items():
            for column in columns:
                column_name = column.get("name") or column.get("column_name")
                column_description = column.get("description", "No description")
                column_type = column.get("data_type", "Unknown type")
                
                if column_name:
                    columns_info.append(f"- {table_name}.{column_name}: {column_type} - {column_description}")
        
        columns_text = "\n".join(columns_info)
        
        return f"""
        I'm trying to match the attribute "{attribute}" mentioned in a database query to an actual database column.
        
        Available columns:
        {columns_text}
        
        Please determine which column (if any) is the most likely match for "{attribute}".
        Consider synonyms, abbreviations, and semantic connections.
        
        Return a JSON object with:
        - table_name: The name of the table containing the matched column
        - column_name: The name of the matched column
        - confidence: Your confidence in the match from 0.0 to 1.0
        - reasoning: Brief explanation of why you matched to this column
        """
    
    async def _resolve_semantic_concepts(
        self, identified_ambiguities: List[str],
        schema_context: Dict[str, Any]
    ) -> Dict[str, ResolvedConcept]:
        """Resolve semantic concepts to database constructs"""
        resolved = {}
        
        for concept in identified_ambiguities:
            resolution_prompt = self._build_concept_resolution_prompt(concept, schema_context)
            resolution_schema = {
                "type": "object",
                "properties": {
                    "interpretation": {"type": "string"},
                    "implementation": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "sql_fragment": {"type": "string"},
                            "tables_involved": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "columns_involved": {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        }
                    },
                    "confidence": {"type": "number"},
                    "alternatives": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "interpretation": {"type": "string"},
                                "confidence": {"type": "number"}
                            }
                        }
                    }
                }
            }
            
            resolution_result = await self.llm_client.generate_structured(resolution_prompt, resolution_schema)
            
            if resolution_result and "interpretation" in resolution_result and "implementation" in resolution_result:
                resolved[concept] = ResolvedConcept(
                    concept=concept,
                    interpretation=resolution_result["interpretation"],
                    implementation=resolution_result["implementation"],
                    confidence=resolution_result.get("confidence", 0.7)
                )
                
        return resolved
    
    def _build_concept_resolution_prompt(self, concept: str, schema_context: Dict[str, Any]) -> str:
        """Build prompt for concept resolution"""
        # Format tables info
        tables_info = []
        for table_name, table_info in schema_context["tables"].items():
            description = table_info.get("description", "No description")
            columns = table_info.get("columns", [])
            
            columns_text = ", ".join([
                f"{c.get('name') or c.get('column_name', 'Unknown')} ({c.get('data_type', 'Unknown')})"
                for c in columns if c.get("name") or c.get("column_name")
            ])
            
            tables_info.append(f"- {table_name}: {description}\n  Columns: {columns_text}")
        
        tables_text = "\n".join(tables_info)
        
        return f"""
        I'm trying to interpret the semantic concept "{concept}" in a database query and map it to database constructs.
        
        Database schema:
        {tables_text}
        
        Please interpret this concept and suggest how it could be implemented using SQL constructs.
        Consider different possible interpretations if ambiguous.
        
        Return a JSON object with:
        - interpretation: A clear interpretation of the concept
        - implementation: Details on how to implement this in SQL
          - type: The implementation type (filter, join, aggregation, etc.)
          - sql_fragment: Example SQL fragment implementing this concept
          - tables_involved: List of tables involved
          - columns_involved: List of columns involved
        - confidence: Your confidence in the interpretation from 0.0 to 1.0
        - alternatives: Array of alternative interpretations with their confidence
        """
    
    async def _discover_join_paths(self, resolved_entities: Dict[str, ResolvedEntity], tenant_id: str) -> Dict[str, Any]:
        """Discover join paths between all resolved entities"""
        join_paths = {}
        
        # Get distinct table names
        table_names = list(set(entity.table_name for entity in resolved_entities.values()))
        
        # For each pair of tables
        for i, source_table in enumerate(table_names):
            for j, target_table in enumerate(table_names):
                if i >= j:  # Skip self-comparisons and duplicates
                    continue
                    
                path_key = f"{source_table}_to_{target_table}"
                
                try:
                    # Find join path using Neo4j
                    path = self.neo4j_client.find_join_path(
                        tenant_id,
                        source_table,
                        target_table,
                        min_confidence=0.7
                    )
                    
                    if path:
                        join_paths[path_key] = path
                except Exception as e:
                    logger.warning(f"Error finding join path from {source_table} to {target_table}: {e}")
                
        return join_paths
    
    async def _generate_interpretations(
        self, 
        structured_query: StructuredQuery,
        resolved_entities: Dict[str, ResolvedEntity],
        resolved_attributes: Dict[str, ResolvedAttribute],
        resolved_concepts: Dict[str, ResolvedConcept],
        join_paths: Dict[str, Any],
        schema_context: Dict[str, Any]
    ) -> List[QueryInterpretation]:
        """Generate multiple possible interpretations of the query"""
        interpretations = []
        
        # If query is not ambiguous, just provide one interpretation
        if structured_query.ambiguity_assessment.score < 0.5:
            interpretations.append(QueryInterpretation(
                entities=resolved_entities,
                attributes=resolved_attributes,
                concepts=resolved_concepts,
                join_paths=join_paths,
                confidence=1.0,
                is_primary=True,
                rationale="Single clear interpretation"
            ))
            return interpretations
            
        # For ambiguous queries, generate multiple interpretations
        # Use LLM to generate different interpretations
        interpretations_prompt = self._build_interpretations_prompt(
            structured_query, resolved_entities, resolved_attributes, 
            resolved_concepts, schema_context
        )
        
        interpretations_schema = {
            "type": "object",
            "properties": {
                "interpretations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string"},
                            "entities": {
                                "type": "object",
                                "additionalProperties": {
                                    "type": "object",
                                    "properties": {
                                        "table_name": {"type": "string"},
                                        "confidence": {"type": "number"}
                                    }
                                }
                            },
                            "concepts": {
                                "type": "object",
                                "additionalProperties": {
                                    "type": "object",
                                    "properties": {
                                        "interpretation": {"type": "string"},
                                        "implementation": {"type": "object"}
                                    }
                                }
                            },
                            "confidence": {"type": "number"},
                            "rationale": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        interpretations_result = await self.llm_client.generate_structured(
            interpretations_prompt, interpretations_schema
        )
        
        if interpretations_result and "interpretations" in interpretations_result:
            # Convert to our internal format
            for idx, interp in enumerate(interpretations_result["interpretations"]):
                # Process entities
                entities = {}
                for entity_name, entity_info in interp.get("entities", {}).items():
                    if "table_name" in entity_info:
                        entities[entity_name] = ResolvedEntity(
                            table_name=entity_info["table_name"],
                            confidence=entity_info.get("confidence", 0.8),
                            resolution_method="interpretation_variant"
                        )
                
                # Process concepts
                concepts = {}
                for concept_name, concept_info in interp.get("concepts", {}).items():
                    if "interpretation" in concept_info and "implementation" in concept_info:
                        concepts[concept_name] = ResolvedConcept(
                            concept=concept_name,
                            interpretation=concept_info["interpretation"],
                            implementation=concept_info["implementation"],
                            confidence=concept_info.get("confidence", 0.8)
                        )
                
                # Create interpretation
                interpretation = QueryInterpretation(
                    entities=entities or resolved_entities,  # Fall back to original if empty
                    attributes=resolved_attributes,  # Use original attributes
                    concepts=concepts,  # Use variant concepts
                    join_paths=join_paths,  # Use original join paths
                    confidence=interp.get("confidence", 0.7),
                    is_primary=(idx == 0),  # First one is primary
                    rationale=interp.get("rationale", "")
                )
                
                interpretations.append(interpretation)
        
        # If no interpretations were generated, provide the default
        if not interpretations:
            interpretations.append(QueryInterpretation(
                entities=resolved_entities,
                attributes=resolved_attributes,
                concepts=resolved_concepts,
                join_paths=join_paths,
                confidence=0.8,
                is_primary=True,
                rationale="Default interpretation (ambiguous query)"
            ))
        
        return interpretations
    
    def _build_interpretations_prompt(
        self,
        structured_query: StructuredQuery,
        resolved_entities: Dict[str, ResolvedEntity],
        resolved_attributes: Dict[str, ResolvedAttribute],
        resolved_concepts: Dict[str, ResolvedConcept],
        schema_context: Dict[str, Any]
    ) -> str:
        """Build prompt for generating multiple interpretations"""
        # Format original query
        original_query = structured_query._meta.raw_query
        
        # Format ambiguities
        ambiguities = "\n".join([f"- {a}" for a in structured_query.identified_ambiguities])
        
        # Format entities
        entities = "\n".join([
            f"- {entity}: resolved to table '{resolved.table_name}' with confidence {resolved.confidence}"
            for entity, resolved in resolved_entities.items()
        ])
        
        # Format concepts
        concepts = "\n".join([
            f"- {concept}: interpreted as '{resolved.interpretation}' with confidence {resolved.confidence}"
            for concept, resolved in resolved_concepts.items()
        ])
        
        # Format schema context
        schema = "\n".join([
            f"- Table: {table_name}\n  Description: {info.get('description', 'No description')}\n  Columns: {', '.join([c.get('name') or c.get('column_name', 'Unknown') for c in info.get('columns', []) if c.get('name') or c.get('column_name')])}"
            for table_name, info in schema_context["tables"].items()
        ])
        
        return f"""
        I have a potentially ambiguous database query that needs multiple interpretations.
        
        Original query: "{original_query}"
        
        Ambiguities identified:
        {ambiguities if ambiguities else "None explicitly identified"}
        
        Current entity resolution:
        {entities if entities else "No entities resolved"}
        
        Current concept resolution:
        {concepts if concepts else "No concepts resolved"}
        
        Database schema:
        {schema}
        
        Please generate 2-3 different possible interpretations of this query, considering different ways the ambiguities could be resolved.
        For each interpretation, provide:
        1. A description of the interpretation
        2. How entities should be mapped to tables
        3. How ambiguous concepts should be interpreted
        4. Confidence score for this interpretation (0.0-1.0)
        5. Rationale for this interpretation
        
        Format the response as JSON with an array of interpretations.
        """
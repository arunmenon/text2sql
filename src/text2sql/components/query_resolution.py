import json
import logging
import math
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
        
        # Get business glossary terms
        glossary_terms = []
        try:
            glossary_terms = self.neo4j_client.get_glossary_terms(tenant_id)
        except Exception as e:
            logger.warning(f"Error getting glossary terms: {e}")
        
        # Get business metrics
        business_metrics = []
        try:
            business_metrics = self.neo4j_client.get_glossary_metrics(tenant_id)
        except Exception as e:
            logger.warning(f"Error getting business metrics: {e}")
                
        return {
            "tables": tables_info,
            "summary": schema_summary,
            "tenant_id": tenant_id,
            "glossary_terms": glossary_terms,
            "business_metrics": business_metrics
        }
    
    async def _resolve_entities(self, entity_mentions: List[str], schema_context: Dict[str, Any]) -> Dict[str, ResolvedEntity]:
        """
        Map entity mentions to actual tables with weighted term mapping.
        
        Args:
            entity_mentions: List of entity mentions from NL query
            schema_context: Schema context for resolution
            
        Returns:
            Dictionary mapping entity mentions to resolved entities
        """
        resolved = {}
        
        # Get available tables
        available_tables = list(schema_context["tables"].keys())
        
        # Get glossary terms and their mappings for lookup
        glossary_term_mappings = {}
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name")
                if not term_name:
                    continue
                    
                # Try to get term details with mappings
                try:
                    term_details = self.neo4j_client.get_glossary_term_details(
                        schema_context["tenant_id"], term_name
                    )
                    if term_details and "mapped_tables" in term_details and term_details["mapped_tables"]:
                        glossary_term_mappings[term_name.lower()] = {
                            "tables": term_details["mapped_tables"],
                            "usage_count": term_details.get("usage_count", 0),
                            "weight": term_details.get("weight", 1.0)
                        }
                except Exception as e:
                    logger.warning(f"Error getting term details for {term_name}: {e}")
        
        for entity in entity_mentions:
            entity_lower = entity.lower()
            potential_matches = []
            
            # Step 1: Direct match
            if entity in schema_context["tables"]:
                potential_matches.append({
                    "table_name": entity,
                    "confidence": 1.0,
                    "resolution_method": "direct_match",
                    "weight": self._calculate_term_weight(1.0, usage_count=10, term_similarity=1.0)  # Direct matches get highest weight
                })
            
            # Step 2: Try lowercase matching
            for table_name in available_tables:
                if table_name.lower() == entity_lower:
                    potential_matches.append({
                        "table_name": table_name,
                        "confidence": 0.9,
                        "resolution_method": "case_insensitive_match",
                        "weight": self._calculate_term_weight(0.9, usage_count=8, term_similarity=0.95)
                    })
            
            # Step 3: Check if entity matches a glossary term with table mappings
            if entity_lower in glossary_term_mappings:
                mapping_info = glossary_term_mappings[entity_lower]
                mapped_tables = mapping_info["tables"]
                usage_count = mapping_info.get("usage_count", 0)
                term_weight = mapping_info.get("weight", 1.0)
                
                if mapped_tables and len(mapped_tables) > 0:
                    # Add all mapped tables as potential matches with weights
                    for table_name in mapped_tables:
                        potential_matches.append({
                            "table_name": table_name,
                            "confidence": 0.85,
                            "resolution_method": "glossary_term_mapping",
                            "weight": self._calculate_term_weight(0.85, usage_count, term_similarity=0.9, term_weight=term_weight)
                        })
            
            # Step 4: Try to fuzzy match with glossary terms
            if "glossary_terms" in schema_context:
                for term in schema_context["glossary_terms"]:
                    term_name = term.get("name", "")
                    if not term_name:
                        continue
                    
                    term_lower = term_name.lower()
                    term_similarity = self._calculate_text_similarity(entity_lower, term_lower)
                    
                    # Check for partial matches with similarity threshold
                    if term_similarity >= 0.5:
                        try:
                            term_details = self.neo4j_client.get_glossary_term_details(
                                schema_context["tenant_id"], term_name
                            )
                            
                            if term_details and "mapped_tables" in term_details and term_details["mapped_tables"]:
                                mapped_tables = term_details["mapped_tables"]
                                usage_count = term_details.get("usage_count", 0)
                                term_weight = term_details.get("weight", 1.0)
                                
                                if mapped_tables and len(mapped_tables) > 0:
                                    # Add all mapped tables as potential matches with weights
                                    for table_name in mapped_tables:
                                        potential_matches.append({
                                            "table_name": table_name,
                                            "confidence": 0.75,
                                            "resolution_method": "fuzzy_glossary_match",
                                            "weight": self._calculate_term_weight(0.75, usage_count, term_similarity, term_weight)
                                        })
                        except Exception as e:
                            logger.warning(f"Error getting term details for fuzzy match {term_name}: {e}")
            
            # Step 5: If we have potential matches, select the one with highest weight
            if potential_matches:
                # Sort by weight in descending order
                potential_matches.sort(key=lambda x: x["weight"], reverse=True)
                best_match = potential_matches[0]
                
                # Track usage for this term mapping
                try:
                    if best_match["resolution_method"] in ["glossary_term_mapping", "fuzzy_glossary_match"]:
                        self._update_term_mapping_usage(schema_context["tenant_id"], entity, best_match["table_name"])
                except Exception as e:
                    logger.warning(f"Error updating term mapping usage: {e}")
                
                resolved[entity] = ResolvedEntity(
                    table_name=best_match["table_name"],
                    confidence=best_match["confidence"],
                    resolution_method=best_match["resolution_method"]
                )
            else:
                # Step 6: If still not resolved, try fuzzy matching using LLM with enhanced context
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
        
    def _calculate_term_weight(self, confidence: float, usage_count: int = 0, term_similarity: float = 1.0, term_weight: float = 1.0) -> float:
        """
        Calculate the weight for a term mapping based on multiple factors.
        
        Args:
            confidence: Base confidence of the match (0.0-1.0)
            usage_count: How many times this mapping has been used successfully
            term_similarity: Text similarity between query term and glossary term (0.0-1.0)
            term_weight: Explicit weight assigned to the term
            
        Returns:
            Calculated weight value
        """
        # Base weight is the confidence
        weight = confidence
        
        # Apply usage bonus (logarithmic to prevent excessive dominance of frequent terms)
        # A term used 10 times would get a +0.3 bonus, 100 times +0.6, etc.
        if usage_count > 0:
            usage_bonus = min(0.3 * (1 + math.log10(usage_count)), 0.6)
            weight += usage_bonus
        
        # Apply similarity factor (more similar = higher weight)
        weight *= term_similarity
        
        # Apply explicit term weight (allowing business users to influence resolution)
        weight *= term_weight
        
        return weight
    
    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate text similarity between two strings.
        
        Args:
            text1: First text string
            text2: Second text string
            
        Returns:
            Similarity score (0.0-1.0)
        """
        # Simple implementation - could be enhanced with more sophisticated NLP
        # Check for exact match
        if text1 == text2:
            return 1.0
            
        # Check for containment
        if text1 in text2:
            return 0.9
        if text2 in text1:
            return 0.8
            
        # Check for word-level similarity
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        # Jaccard similarity
        if words1 or words2:  # Prevent division by zero
            intersection = len(words1.intersection(words2))
            union = len(words1.union(words2))
            return intersection / union
        
        return 0.0
    
    def _update_term_mapping_usage(self, tenant_id: str, term: str, table_name: str) -> None:
        """
        Update usage statistics for term mapping to improve future weighting.
        
        Args:
            tenant_id: Tenant ID
            term: Business term used
            table_name: Table name it was mapped to
        """
        try:
            # First try to find an exact match in glossary terms
            terms = self.neo4j_client.search_glossary_terms(tenant_id, term)
            
            if terms:
                # If we found the term, update its mapping usage
                term_name = terms[0].get("name")
                if term_name:
                    # Update mapping usage in Neo4j
                    self.neo4j_client.update_term_mapping_usage(tenant_id, term_name, table_name)
                    logger.info(f"Updated usage count for term '{term_name}' mapping to table '{table_name}'")
            else:
                # Term not in glossary yet, could add it automatically in the future
                # For now, just log that we would add it
                logger.info(f"Term '{term}' was used to refer to table '{table_name}' but is not in glossary")
        except Exception as e:
            logger.warning(f"Error updating term mapping usage: {e}")
    
    def _build_entity_matching_prompt(self, entity: str, schema_context: Dict[str, Any]) -> str:
        """Build prompt for entity matching"""
        tables_info = "\n".join([
            f"- {table_name}: {table_info.get('description', 'No description')}"
            for table_name, table_info in schema_context["tables"].items()
        ])
        
        # Add glossary terms that have mappings to tables
        glossary_info = ""
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            relevant_terms = []
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name", "")
                definition = term.get("definition", "")
                
                # Look for approximate matches to help with resolution
                if term_name and (entity.lower() in term_name.lower() or 
                                   term_name.lower() in entity.lower() or
                                   any(word in term_name.lower() for word in entity.lower().split())):
                    relevant_terms.append(f"- {term_name}: {definition[:100]}...")
            
            if relevant_terms:
                glossary_info = "Relevant business glossary terms:\n" + "\n".join(relevant_terms)
        
        return f"""
        I'm trying to match the entity "{entity}" mentioned in a database query to an actual database table.
        
        Available tables:
        {tables_info}
        
        {glossary_info}
        
        Please determine which table (if any) is the most likely match for "{entity}".
        Consider:
        1. Plural/singular forms and abbreviations
        2. Semantic connections and business terminology
        3. If the entity matches a business term, identify which table that term refers to
        4. Common naming patterns (e.g., "customers" referring to a "customer" table)
        
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
        
        # Get glossary term mappings to columns
        glossary_column_mappings = {}
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name")
                if not term_name:
                    continue
                    
                # Try to get term details with mappings to columns
                try:
                    term_details = self.neo4j_client.get_glossary_term_details(
                        schema_context["tenant_id"], term_name
                    )
                    if term_details and "mapped_columns" in term_details and term_details["mapped_columns"]:
                        glossary_column_mappings[term_name.lower()] = term_details["mapped_columns"]
                except Exception as e:
                    logger.warning(f"Error getting term details for columns of {term_name}: {e}")
        
        # For each attribute mention
        for attribute in attribute_mentions:
            attribute_lower = attribute.lower()
            
            # Step 1: Check direct matches across all tables
            found = False
            for table_name, columns in table_columns.items():
                for column in columns:
                    column_name = column.get("name") or column.get("column_name")
                    if not column_name:
                        continue
                        
                    if column_name.lower() == attribute_lower:
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
            
            # Step 2: Check if attribute matches a glossary term with column mappings
            if not found and attribute_lower in glossary_column_mappings:
                mapped_columns = glossary_column_mappings[attribute_lower]
                if mapped_columns and len(mapped_columns) > 0:
                    # Use the first mapped column with high confidence
                    mapped_column = mapped_columns[0]
                    if "table" in mapped_column and "column" in mapped_column:
                        resolved[attribute] = ResolvedAttribute(
                            table_name=mapped_column["table"],
                            column_name=mapped_column["column"],
                            confidence=0.85,
                            resolution_method="glossary_term_mapping"
                        )
                        found = True
            
            # Step 3: Try to fuzzy match with glossary terms that map to columns
            if not found and "glossary_terms" in schema_context:
                # Check for partial matches with glossary terms
                for term in schema_context["glossary_terms"]:
                    term_name = term.get("name", "")
                    if not term_name:
                        continue
                        
                    # Check for partial matches
                    if attribute_lower in term_name.lower() or term_name.lower() in attribute_lower:
                        # Check if term has column mappings
                        try:
                            term_details = self.neo4j_client.get_glossary_term_details(
                                schema_context["tenant_id"], term_name
                            )
                            if term_details and "mapped_columns" in term_details and term_details["mapped_columns"]:
                                mapped_columns = term_details["mapped_columns"]
                                if mapped_columns and len(mapped_columns) > 0:
                                    # Use the first mapped column
                                    mapped_column = mapped_columns[0]
                                    if "table" in mapped_column and "column" in mapped_column:
                                        # Only use it if the table is one of our resolved entities
                                        if mapped_column["table"] in table_names:
                                            resolved[attribute] = ResolvedAttribute(
                                                table_name=mapped_column["table"],
                                                column_name=mapped_column["column"],
                                                confidence=0.75,
                                                resolution_method="fuzzy_glossary_match"
                                            )
                                            found = True
                                            break
                        except Exception as e:
                            logger.warning(f"Error getting column mappings for fuzzy match {term_name}: {e}")
            
            # Step 4: If not found, use LLM to resolve with enhanced context
            if not found:
                match_prompt = self._build_attribute_matching_prompt(attribute, table_columns, schema_context)
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
    
    def _build_attribute_matching_prompt(self, attribute: str, table_columns: Dict[str, List[Dict]], schema_context: Dict[str, Any] = None) -> str:
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
        
        # Add relevant glossary terms if available
        glossary_info = ""
        if schema_context and "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            relevant_terms = []
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name", "")
                definition = term.get("definition", "")
                
                # Look for approximate matches to help with resolution
                if term_name and (attribute.lower() in term_name.lower() or 
                                   term_name.lower() in attribute.lower() or
                                   any(word in term_name.lower() for word in attribute.lower().split())):
                    # Try to get column mappings for this term
                    try:
                        term_details = self.neo4j_client.get_glossary_term_details(
                            schema_context["tenant_id"], term_name
                        )
                        if term_details and "mapped_columns" in term_details:
                            mapped_text = ", ".join([f"{m['table']}.{m['column']}" for m in term_details["mapped_columns"] if "table" in m and "column" in m])
                            if mapped_text:
                                relevant_terms.append(f"- {term_name}: {definition[:100]}... (maps to: {mapped_text})")
                            else:
                                relevant_terms.append(f"- {term_name}: {definition[:100]}...")
                        else:
                            relevant_terms.append(f"- {term_name}: {definition[:100]}...")
                    except Exception:
                        relevant_terms.append(f"- {term_name}: {definition[:100]}...")
            
            if relevant_terms:
                glossary_info = "Relevant business glossary terms:\n" + "\n".join(relevant_terms)
        
        return f"""
        I'm trying to match the attribute "{attribute}" mentioned in a database query to an actual database column.
        
        Available columns:
        {columns_text}
        
        {glossary_info}
        
        Please determine which column (if any) is the most likely match for "{attribute}".
        Consider:
        1. Synonyms, abbreviations, and semantic connections
        2. Business terminology and concepts
        3. If a glossary term maps to one of these columns, consider that information
        4. Common naming patterns (e.g., "id" vs. "identifier")
        
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
        """Resolve semantic concepts to database constructs with composite concept detection"""
        resolved = {}
        
        # Step 1: Analyze ambiguities for composite concepts
        composite_concepts = self._detect_composite_concepts(identified_ambiguities)
        
        # Process both single and composite concepts
        all_concepts = identified_ambiguities + composite_concepts
        
        # Try to match concepts with business glossary terms first
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            for concept in all_concepts:
                concept_lower = concept.lower()
                matched_terms = []
                
                # Step 2: Check for multiple term matches within a single concept
                # This is crucial for composite concepts like "active customers"
                for term in schema_context["glossary_terms"]:
                    term_name = term.get("name", "")
                    if not term_name:
                        continue
                    
                    term_lower = term_name.lower()
                    # Check if this term is part of our concept
                    if (term_lower == concept_lower or 
                        term_lower in concept_lower or 
                        self._calculate_text_similarity(term_lower, concept_lower) > 0.6):
                        matched_terms.append(term)
                
                if matched_terms:
                    # For composite concepts with multiple term matches
                    if len(matched_terms) > 1:
                        resolved[concept] = await self._resolve_composite_concept(
                            concept, matched_terms, schema_context
                        )
                    else:
                        # Single term match - process as before
                        term = matched_terms[0]
                        term_name = term.get("name", "")
                        
                        try:
                            # Get term details with mappings
                            term_details = self.neo4j_client.get_glossary_term_details(
                                schema_context["tenant_id"], term_name
                            )
                            
                            if term_details:
                                # Prepare implementation based on mappings
                                implementation = {
                                    "type": "glossary_term",
                                    "sql_fragment": "",
                                    "tables_involved": term_details.get("mapped_tables", []),
                                    "columns_involved": [
                                        f"{col['table']}.{col['column']}" 
                                        for col in term_details.get("mapped_columns", [])
                                        if "table" in col and "column" in col
                                    ]
                                }
                                
                                # If there are mappings, generate SQL fragment suggestion
                                if implementation["tables_involved"] or implementation["columns_involved"]:
                                    # Simple implementation - could be enhanced
                                    if implementation["columns_involved"]:
                                        implementation["sql_fragment"] = f"SELECT {', '.join(implementation['columns_involved'])} FROM {', '.join(set(implementation['tables_involved']))}"
                                    elif implementation["tables_involved"]:
                                        implementation["sql_fragment"] = f"SELECT * FROM {implementation['tables_involved'][0]}"
                                
                                # Create resolved concept
                                resolved[concept] = ResolvedConcept(
                                    concept=concept,
                                    interpretation=term_details.get("definition", f"Business term: {term_name}"),
                                    implementation=implementation,
                                    confidence=0.9
                                )
                        except Exception as e:
                            logger.warning(f"Error resolving concept with glossary term {term_name}: {e}")
        
        # For any unresolved concepts, use LLM
        unresolved_concepts = [c for c in all_concepts if c not in resolved]
        
        for concept in unresolved_concepts:
            # Use a different prompt for composite concepts
            if concept in composite_concepts:
                resolution_prompt = self._build_composite_concept_resolution_prompt(concept, schema_context)
            else:
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
        
    def _detect_composite_concepts(self, identified_ambiguities: List[str]) -> List[str]:
        """
        Detect composite concepts from individual ambiguities.
        
        Args:
            identified_ambiguities: List of identified ambiguous terms
            
        Returns:
            List of detected composite concepts
        """
        composite_concepts = []
        
        # Look for multi-word concepts
        for concept in identified_ambiguities:
            words = concept.split()
            if len(words) > 1:
                # This is already a multi-word concept
                continue
                
            # Look for pairs of concepts that might form composites
            for other_concept in identified_ambiguities:
                if concept == other_concept:
                    continue
                    
                # Create possible composite (e.g., "active" + "customers" = "active customers")
                composite = f"{concept} {other_concept}"
                composite_concepts.append(composite)
                
                # Also try reverse order
                composite_reverse = f"{other_concept} {concept}"
                composite_concepts.append(composite_reverse)
        
        return composite_concepts
        
    async def _resolve_composite_concept(
        self, concept: str, matched_terms: List[Dict[str, Any]], schema_context: Dict[str, Any]
    ) -> ResolvedConcept:
        """
        Resolve a composite concept made up of multiple business terms.
        
        Args:
            concept: The composite concept
            matched_terms: List of matched glossary terms
            schema_context: Schema context
            
        Returns:
            Resolved concept
        """
        # Get term details for all matched terms
        term_details_list = []
        for term in matched_terms:
            term_name = term.get("name", "")
            if not term_name:
                continue
                
            try:
                term_details = self.neo4j_client.get_glossary_term_details(
                    schema_context["tenant_id"], term_name
                )
                if term_details:
                    term_details_list.append(term_details)
            except Exception as e:
                logger.warning(f"Error getting details for term in composite concept: {term_name}, {e}")
        
        # If we couldn't get details for any terms, use LLM
        if not term_details_list:
            prompt = self._build_composite_concept_resolution_prompt(concept, schema_context)
            resolution_schema = {
                "type": "object",
                "properties": {
                    "interpretation": {"type": "string"},
                    "implementation": {"type": "object"},
                    "confidence": {"type": "number"}
                }
            }
            
            resolution_result = await self.llm_client.generate_structured(prompt, resolution_schema)
            
            if resolution_result and "interpretation" in resolution_result:
                return ResolvedConcept(
                    concept=concept,
                    interpretation=resolution_result["interpretation"],
                    implementation=resolution_result.get("implementation", {}),
                    confidence=resolution_result.get("confidence", 0.7)
                )
        
        # Analyze term mappings to generate appropriate SQL for the composite
        tables_involved = set()
        all_columns = []
        
        for term_details in term_details_list:
            # Collect tables and columns
            if "mapped_tables" in term_details:
                for table in term_details.get("mapped_tables", []):
                    tables_involved.add(table)
                    
            if "mapped_columns" in term_details:
                for col in term_details.get("mapped_columns", []):
                    if "table" in col and "column" in col:
                        all_columns.append(f"{col['table']}.{col['column']}")
        
        # Generate a SQL fragment based on the composite terms
        sql_fragment = ""
        
        # Combine definitions for interpretation
        interpretation = "Composite business concept: "
        interpretation += " and ".join([
            term_details.get("definition", term_details.get("name", "Unknown term"))
            for term_details in term_details_list
        ])
        
        # Determine SQL fragment type based on composite analysis
        # Example: "active customers" -> WHERE customers.status = 'active'
        concept_words = concept.lower().split()
        
        # Check for modifiers that suggest filtering conditions
        modifiers = ["active", "new", "premium", "deleted", "archived", "top", "recent", "high", "low"]
        
        if any(mod in concept_words for mod in modifiers):
            # This is likely a filtering composite
            impl_type = "filter"
            
            # Simple logic to generate filter condition
            if tables_involved and "active" in concept_words:
                table = next(iter(tables_involved))  # Get first table
                sql_fragment = f"SELECT * FROM {table} WHERE status = 'active'"
            elif tables_involved:
                # Generic filter
                modifier = next((mod for mod in modifiers if mod in concept_words), "")
                table = next(iter(tables_involved))
                sql_fragment = f"SELECT * FROM {table} WHERE {modifier} = true"
        else:
            # Default to a selection composite
            impl_type = "selection"
            
            if all_columns:
                tables = ", ".join(tables_involved)
                columns = ", ".join(all_columns)
                sql_fragment = f"SELECT {columns} FROM {tables}"
            elif tables_involved:
                table = next(iter(tables_involved))
                sql_fragment = f"SELECT * FROM {table}"
        
        # Create implementation
        implementation = {
            "type": impl_type,
            "sql_fragment": sql_fragment,
            "tables_involved": list(tables_involved),
            "columns_involved": all_columns
        }
        
        return ResolvedConcept(
            concept=concept,
            interpretation=interpretation,
            implementation=implementation,
            confidence=0.85
        )
        
    def _build_composite_concept_resolution_prompt(self, concept: str, schema_context: Dict[str, Any]) -> str:
        """
        Build prompt for resolving composite business concepts.
        
        Args:
            concept: The composite concept
            schema_context: Schema context
            
        Returns:
            Generated prompt
        """
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
        
        # Find relevant glossary terms
        glossary_info = ""
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            concept_words = concept.lower().split()
            relevant_terms = []
            
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name", "")
                if not term_name:
                    continue
                    
                term_lower = term_name.lower()
                term_definition = term.get("definition", "")
                
                # Check if term is relevant to the composite concept
                if any(word in term_lower for word in concept_words) or term_lower in concept.lower():
                    try:
                        term_details = self.neo4j_client.get_glossary_term_details(
                            schema_context["tenant_id"], term_name
                        )
                        
                        if term_details:
                            # Format mappings information
                            tables_mapped = term_details.get("mapped_tables", [])
                            columns_mapped = term_details.get("mapped_columns", [])
                            
                            mappings_info = ""
                            if tables_mapped:
                                mappings_info += f"\n    Tables: {', '.join(tables_mapped)}"
                            
                            if columns_mapped:
                                formatted_cols = [f"{col['table']}.{col['column']}" for col in columns_mapped if "table" in col and "column" in col]
                                if formatted_cols:
                                    mappings_info += f"\n    Columns: {', '.join(formatted_cols)}"
                            
                            if mappings_info:
                                relevant_terms.append(f"- {term_name}: {term_definition[:150]}...{mappings_info}")
                            else:
                                relevant_terms.append(f"- {term_name}: {term_definition[:200]}...")
                    except Exception:
                        # Fall back to basic term information
                        relevant_terms.append(f"- {term_name}: {term_definition[:200]}...")
            
            if relevant_terms:
                glossary_info = "Relevant business glossary terms:\n" + "\n".join(relevant_terms)
        
        return f"""
        I'm trying to interpret the composite business concept "{concept}" in a database query.
        This appears to combine multiple business terms or concepts that need to be translated into SQL.
        
        Database schema:
        {tables_text}
        
        {glossary_info}
        
        Please analyze this composite concept and determine how it should be implemented in SQL.
        Consider:
        1. This is a multi-term concept that may combine multiple business terms
        2. It may map to multiple tables and/or columns
        3. It might represent a filter condition (e.g., "active customers" -> WHERE status='active')
        4. It could represent a join between multiple entities (e.g., "customer orders" -> JOIN)
        5. It might be a derived metric or calculation
        
        Return a JSON object with:
        - interpretation: A clear interpretation of this composite concept
        - implementation: Details on how to implement this in SQL, including:
          - type: Implementation type (filter, join, derived_metric, etc.)
          - sql_fragment: Example SQL fragment that implements this concept
          - tables_involved: List of tables involved
          - columns_involved: List of columns involved
        - confidence: Your confidence in this interpretation from 0.0 to 1.0
        """
    
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
        
        # Find relevant glossary terms
        glossary_info = ""
        if "glossary_terms" in schema_context and schema_context["glossary_terms"]:
            concept_lower = concept.lower()
            relevant_terms = []
            
            for term in schema_context["glossary_terms"]:
                term_name = term.get("name", "")
                if not term_name:
                    continue
                    
                term_definition = term.get("definition", "")
                
                # Check if term is relevant to the concept
                if (concept_lower in term_name.lower() or 
                    term_name.lower() in concept_lower or 
                    any(word in term_name.lower() for word in concept_lower.split()) or
                    concept_lower in term_definition.lower()):
                    # Try to get mappings
                    try:
                        term_details = self.neo4j_client.get_glossary_term_details(
                            schema_context["tenant_id"], term_name
                        )
                        
                        if term_details:
                            # Format mappings information
                            tables_mapped = term_details.get("mapped_tables", [])
                            columns_mapped = term_details.get("mapped_columns", [])
                            
                            mappings_info = ""
                            if tables_mapped:
                                mappings_info += f"\n    Tables: {', '.join(tables_mapped)}"
                            
                            if columns_mapped:
                                formatted_cols = [f"{col['table']}.{col['column']}" for col in columns_mapped if "table" in col and "column" in col]
                                if formatted_cols:
                                    mappings_info += f"\n    Columns: {', '.join(formatted_cols)}"
                            
                            if mappings_info:
                                relevant_terms.append(f"- {term_name}: {term_definition[:150]}...{mappings_info}")
                            else:
                                relevant_terms.append(f"- {term_name}: {term_definition[:200]}...")
                    except Exception:
                        # Fall back to basic term information
                        relevant_terms.append(f"- {term_name}: {term_definition[:200]}...")
            
            if relevant_terms:
                glossary_info = "Relevant business glossary terms:\n" + "\n".join(relevant_terms)
        
        return f"""
        I'm trying to interpret the semantic concept "{concept}" in a database query and map it to database constructs.
        
        Database schema:
        {tables_text}
        
        {glossary_info}
        
        Please interpret this concept and suggest how it could be implemented using SQL constructs.
        Consider different possible interpretations if ambiguous.
        If there are relevant business glossary terms, use those to guide your interpretation.
        
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
        original_query = structured_query.meta.raw_query
        
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
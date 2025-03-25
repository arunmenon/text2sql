"""
Entity Recognition Agent for transparent text2sql reasoning.

This agent is responsible for identifying and resolving entities in natural language queries,
leveraging the semantic graph for improved accuracy.
"""

import logging
import re
from typing import Dict, Any, List, Optional

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent, ResolutionStrategy
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider


class EntityResolutionStrategy(ResolutionStrategy):
    """Base class for entity resolution strategies."""
    
    def __init__(self, graph_context_provider: SemanticGraphContextProvider):
        """
        Initialize resolution strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
        """
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(__name__)
    
    @property
    def strategy_name(self) -> str:
        """Get the name of this resolution strategy."""
        return self.__class__.__name__
    
    @property
    def description(self) -> str:
        """Get description of this resolution strategy."""
        return "Base entity resolution strategy"


class DirectTableMatchStrategy(EntityResolutionStrategy):
    """Strategy for direct matching with database tables."""
    
    @property
    def description(self) -> str:
        return "Matches entity names directly with database table names"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity by direct table matching.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If direct table match found
        if entity_context["type"] == "table" and entity_context["resolution"]:
            table_info = entity_context["resolution"][0]
            table_name = table_info.get("name")
            
            return {
                "entity": entity,
                "resolved_to": table_name,
                "resolution_type": "table",
                "confidence": 0.9,  # High confidence for direct matches
                "strategy": self.strategy_name,
                "metadata": {
                    "table_description": table_info.get("description", ""),
                    "dataset_id": table_info.get("dataset_id", "")
                }
            }
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }


class GlossaryTermMatchStrategy(EntityResolutionStrategy):
    """Strategy for matching with business glossary terms."""
    
    @property
    def description(self) -> str:
        return "Maps entity names to tables via business glossary terms"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity via business glossary terms.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If glossary term match found
        if entity_context["type"] == "business_term" and entity_context["resolution"]:
            term_info = entity_context["resolution"]
            
            if "mapped_tables" in term_info and term_info["mapped_tables"]:
                table_name = term_info["mapped_tables"][0]
                
                return {
                    "entity": entity,
                    "resolved_to": table_name,
                    "resolution_type": "business_term",
                    "confidence": 0.85,  # Good confidence for glossary matches
                    "strategy": self.strategy_name,
                    "metadata": {
                        "term_name": term_info.get("name", entity),
                        "term_definition": term_info.get("definition", ""),
                        "term_domain": term_info.get("domain", "")
                    }
                }
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }


class SemanticConceptMatchStrategy(EntityResolutionStrategy):
    """Strategy for matching with semantic concepts in the graph."""
    
    @property
    def description(self) -> str:
        return "Maps entity names to tables via semantic concepts in the knowledge graph"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity via semantic concepts.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        intent = context.get("intent", {}).get("intent_type", "selection")
        entity_context = self.graph_context.get_entity_resolution_context(tenant_id, entity)
        
        # If semantic concept match found
        if entity_context["type"] == "semantic_concept" and entity_context["resolution"]:
            concept_info = entity_context["resolution"]
            
            if "implementation" in concept_info and "tables_involved" in concept_info["implementation"]:
                tables = concept_info["implementation"]["tables_involved"]
                
                if tables:
                    # For aggregation intents, prioritize tables with numeric columns
                    if intent == "aggregation":
                        # In a real implementation, we would check for numeric columns
                        # For now, just use the first table
                        table_name = tables[0]
                    else:
                        table_name = tables[0]
                    
                    concept_type = self._determine_concept_type(concept_info)
                    
                    return {
                        "entity": entity,
                        "resolved_to": table_name,
                        "resolution_type": "semantic_concept",
                        "confidence": 0.8,  # Good confidence for concept matches
                        "strategy": self.strategy_name,
                        "metadata": {
                            "concept_name": concept_info.get("name", entity),
                            "concept_type": concept_type,
                            "concept_description": concept_info.get("description", ""),
                            "all_tables": tables
                        }
                    }
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
    
    def _determine_concept_type(self, concept_info: Dict[str, Any]) -> str:
        """
        Determine the type of semantic concept.
        
        Args:
            concept_info: Concept information
            
        Returns:
            Concept type as string
        """
        labels = concept_info.get("labels", [])
        
        if isinstance(labels, list):
            if "CompositeConcept" in labels:
                return "composite"
            elif "BusinessProcess" in labels:
                return "business_process"
            elif "RelationshipConcept" in labels:
                return "relationship"
            elif "HierarchicalConcept" in labels:
                return "hierarchical"
        elif isinstance(labels, str):
            if "CompositeConcept" in labels:
                return "composite"
            elif "BusinessProcess" in labels:
                return "business_process"
            elif "RelationshipConcept" in labels:
                return "relationship"
            elif "HierarchicalConcept" in labels:
                return "hierarchical"
        
        return "general"


class LLMBasedResolutionStrategy(EntityResolutionStrategy):
    """Strategy for resolving entities using LLM."""
    
    def __init__(self, graph_context_provider: SemanticGraphContextProvider, 
                llm_client: LLMClient, prompt_loader: PromptLoader):
        """
        Initialize LLM-based resolution strategy.
        
        Args:
            graph_context_provider: Provider for semantic graph context
            llm_client: LLM client for text generation
            prompt_loader: Loader for prompt templates
        """
        super().__init__(graph_context_provider)
        self.llm_client = llm_client
        self.prompt_loader = prompt_loader
    
    @property
    def description(self) -> str:
        return "Uses language model to resolve entities based on query context and available schema"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity using LLM.
        
        Args:
            entity: Entity to resolve
            context: Resolution context
            
        Returns:
            Resolution result
        """
        tenant_id = context.get("tenant_id", "default")
        query = context.get("query", "")
        intent = context.get("intent", {}).get("intent_type", "selection")
        
        # Get schema context for LLM prompt
        graph_context = self.graph_context.get_graph_enhanced_context(tenant_id)
        tables_info = self._format_tables_info(graph_context)
        
        # Create prompt for LLM resolution
        prompt = self.prompt_loader.format_prompt(
            "entity_resolution",
            query=query,
            entity_name=entity,
            intent_type=intent,
            tables_info=tables_info
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "confidence": {"type": "number"},
                "reasoning": {"type": "string"}
            },
            "required": ["table_name", "confidence"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "table_name" in response:
                table_name = response["table_name"]
                confidence = response.get("confidence", 0.7)
                
                return {
                    "entity": entity,
                    "resolved_to": table_name,
                    "resolution_type": "llm",
                    "confidence": confidence,
                    "strategy": self.strategy_name,
                    "metadata": {
                        "reasoning": response.get("reasoning", ""),
                        "llm_confidence": confidence
                    }
                }
        except Exception as e:
            self.logger.error(f"Error in LLM resolution: {e}")
        
        return {
            "entity": entity,
            "resolved_to": None,
            "resolution_type": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "metadata": {}
        }
    
    def _format_tables_info(self, graph_context: Dict[str, Any]) -> str:
        """
        Format tables information for LLM prompts.
        
        Args:
            graph_context: Graph context information
            
        Returns:
            Formatted string with tables information
        """
        tables_info = []
        
        for table_name, table_data in graph_context.get("tables", {}).items():
            description = table_data.get("description", "No description available")
            columns = table_data.get("columns", [])
            
            column_str = ", ".join([
                f"{col.get('name')} ({col.get('data_type', 'unknown')})" 
                for col in columns[:5]  # Limit to first 5 columns
            ])
            
            tables_info.append(f"- Table: {table_name}\n  Description: {description}\n  Columns: {column_str}")
        
        return "\n".join(tables_info)


class EntityAgent(Agent):
    """Agent for entity recognition with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader):
        """
        Initialize entity agent.
        
        Args:
            llm_client: LLM client for text generation
            graph_context_provider: Provider for semantic graph context
            prompt_loader: Loader for prompt templates
        """
        super().__init__(prompt_loader)
        self.llm_client = llm_client
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(__name__)
        
        # Initialize resolution strategies
        self.resolution_strategies = [
            DirectTableMatchStrategy(graph_context_provider),
            GlossaryTermMatchStrategy(graph_context_provider),
            SemanticConceptMatchStrategy(graph_context_provider),
            LLMBasedResolutionStrategy(graph_context_provider, llm_client, prompt_loader)
        ]
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "entity"
    
    async def process(self, context: Dict[str, Any], reasoning_stream: ReasoningStream) -> Dict[str, Any]:
        """
        Process query for entity recognition.
        
        Args:
            context: Context with query information
            reasoning_stream: Reasoning stream to update
            
        Returns:
            Entity recognition results
        """
        query = context["query"]
        intent = context.get("intent", {})
        boundary_registry = context.get("boundary_registry", BoundaryRegistry())
        
        # Start entity recognition stage
        stage = reasoning_stream.start_stage(
            name="Entity Recognition",
            description="Identifying and resolving database entities in the query"
        )
        
        # Step 1: Extract entity candidates
        candidates = self._extract_entity_candidates(query)
        reasoning_stream.add_step(
            description="Extracting potential entity mentions from query",
            evidence={
                "extraction_methods": candidates["methods"],
                "potential_entities": candidates["entities"],
                "details": candidates["details"]
            },
            confidence=candidates["confidence"]
        )
        
        # Step 2: Filter candidates based on intent
        filtered_candidates = self._filter_by_intent(candidates["entities"], intent)
        reasoning_stream.add_step(
            description=f"Filtering candidates based on {intent.get('intent_type', 'unknown')} intent",
            evidence={
                "intent_type": intent.get("intent_type", "unknown"),
                "before_filtering": len(candidates["entities"]),
                "after_filtering": len(filtered_candidates),
                "filtered_entities": filtered_candidates
            },
            confidence=0.85
        )
        
        # Step 3: Resolve entities using strategies
        resolved_entities = await self._resolve_entities(filtered_candidates, context)
        reasoning_stream.add_step(
            description="Resolving entities using multiple resolution strategies",
            evidence={
                "strategies_used": [s.__class__.__name__ for s in self.resolution_strategies],
                "entities_resolved": resolved_entities["entities"],
                "resolution_details": resolved_entities["details"]
            },
            confidence=resolved_entities["confidence"]
        )
        
        # Step 4: Handle unknown entities as knowledge boundaries
        unknown_entities = [
            entity for entity in filtered_candidates 
            if entity not in resolved_entities["entities"] or resolved_entities["entities"][entity]["confidence"] < 0.4
        ]
        
        if unknown_entities:
            for entity in unknown_entities:
                boundary = KnowledgeBoundary(
                    boundary_type=BoundaryType.UNKNOWN_ENTITY,
                    component=f"entity_{entity}",
                    confidence=0.2,
                    explanation=f"Could not reliably map '{entity}' to any database table or concept",
                    suggestions=[
                        f"Did you mean a different term for '{entity}'?",
                        "Try using a more specific business term",
                        "Check if this entity exists in the database"
                    ]
                )
                boundary_registry.add_boundary(boundary)
            
            reasoning_stream.add_step(
                description="Identifying unknown or unmappable entities",
                evidence={
                    "unknown_entities": unknown_entities,
                    "boundary_count": len(unknown_entities),
                    "requires_clarification": len(unknown_entities) > 0
                },
                confidence=0.9
            )
        
        # Step 5: Generate alternatives for ambiguous entities
        alternatives = []
        for entity_name, entity_info in resolved_entities["entities"].items():
            if 0.4 <= entity_info["confidence"] < 0.7:
                alt_options = await self._generate_entity_alternatives(entity_name, entity_info, context)
                alternatives.extend(alt_options)
        
        # Conclude the reasoning stage
        entity_count = len(resolved_entities["entities"])
        avg_confidence = resolved_entities["avg_confidence"]
        unknown_count = len(unknown_entities)
        
        conclusion = f"Identified {entity_count} entities with average confidence {avg_confidence:.2f}"
        if unknown_count > 0:
            conclusion += f" and {unknown_count} unknown entities"
        
        reasoning_stream.conclude_stage(
            conclusion=conclusion,
            final_output=resolved_entities["entities"],
            alternatives=[Alternative(
                description=alt["description"],
                confidence=alt["confidence"],
                reason=alt["reason"]
            ) for alt in alternatives]
        )
        
        # Update context with boundary registry
        context["boundary_registry"] = boundary_registry
        context["entities"] = resolved_entities["entities"]
        
        return resolved_entities["entities"]
    
    def _extract_entity_candidates(self, query: str) -> Dict[str, Any]:
        """
        Extract potential entity candidates using multiple methods.
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary with extraction results
        """
        methods_used = []
        all_candidates = set()
        method_details = {}
        
        # Method 1: Extract by capitalization
        cap_candidates = self._extract_by_capitalization(query)
        if cap_candidates:
            methods_used.append("capitalization")
            all_candidates.update(cap_candidates)
            method_details["capitalization"] = {
                "description": "Extracted terms that begin with capital letters",
                "candidates": cap_candidates
            }
        
        # Method 2: Extract by SQL keyword indicators
        keyword_candidates = self._extract_by_sql_keywords(query)
        if keyword_candidates:
            methods_used.append("sql_keywords")
            all_candidates.update(keyword_candidates)
            method_details["sql_keywords"] = {
                "description": "Extracted terms following SQL-like keywords",
                "candidates": keyword_candidates
            }
        
        # Method 3: Extract noun phrases
        np_candidates = self._extract_noun_phrases(query)
        if np_candidates:
            methods_used.append("noun_phrases")
            all_candidates.update(np_candidates)
            method_details["noun_phrases"] = {
                "description": "Extracted noun phrases as potential entities",
                "candidates": np_candidates
            }
        
        # Calculate confidence based on method coverage
        method_count = len(methods_used)
        confidence = min(0.6 + (method_count * 0.1), 0.9)  # Cap at 0.9
        
        return {
            "entities": list(all_candidates),
            "methods": methods_used,
            "details": method_details,
            "confidence": confidence
        }
    
    def _extract_by_capitalization(self, query: str) -> List[str]:
        """
        Extract terms that begin with capital letters.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        words = query.split()
        candidates = []
        
        # Extract single capitalized words
        for word in words:
            if word and word[0].isupper() and len(word) > 3:
                clean_word = word.strip(",.;:()\"'")
                if clean_word and len(clean_word) > 3:
                    candidates.append(clean_word)
        
        # Extract adjacent capitalized words
        for i in range(len(words) - 1):
            if words[i] and words[i+1] and words[i][0].isupper() and words[i+1][0].isupper():
                clean_term = f"{words[i]} {words[i+1]}".strip(",.;:()\"'")
                candidates.append(clean_term)
        
        return candidates
    
    def _extract_by_sql_keywords(self, query: str) -> List[str]:
        """
        Extract terms following SQL-like keywords.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        query_lower = query.lower()
        words = query_lower.split()
        candidates = []
        
        # SQL-like keywords that might precede entities
        sql_indicators = ["select", "from", "where", "join", "group", "order", 
                          "show", "find", "get", "list", "display", "about"]
        
        for i, word in enumerate(words):
            if i < len(words) - 1 and word in sql_indicators:
                next_word = words[i + 1].strip(",.;:()\"'")
                if next_word and len(next_word) > 3:
                    # Get the original case version
                    original_case = query.split()[i + 1].strip(",.;:()\"'")
                    candidates.append(original_case)
        
        return candidates
    
    def _extract_noun_phrases(self, query: str) -> List[str]:
        """
        Extract noun phrases as potential entities.
        
        Args:
            query: Natural language query
            
        Returns:
            List of extracted terms
        """
        # This is a simplified version - in a real implementation,
        # we would use a proper NLP library for part-of-speech tagging
        
        words = query.split()
        candidates = []
        
        # Common adjectives that might precede entity nouns
        adjectives = ["active", "new", "all", "recent", "past", "top", "high", "low", 
                      "best", "worst", "available", "current", "previous", "next"]
        
        # Common nouns that might be database entities
        entity_nouns = ["customer", "order", "product", "sale", "transaction", 
                       "user", "account", "item", "invoice", "payment", "location", 
                       "store", "facility", "asset", "report", "proposal"]
        
        # Check for adjective + noun combinations
        for i, word in enumerate(words):
            if i < len(words) - 1:
                if word.lower() in adjectives and words[i+1].lower() in entity_nouns:
                    candidates.append(f"{word} {words[i+1]}")
                elif word.lower() in entity_nouns:
                    candidates.append(word)
        
        return candidates
    
    def _filter_by_intent(self, candidates: List[str], intent: Dict[str, Any]) -> List[str]:
        """
        Filter entity candidates based on query intent.
        
        Args:
            candidates: List of entity candidates
            intent: Intent information
            
        Returns:
            Filtered list of candidates
        """
        intent_type = intent.get("intent_type", "selection")
        
        # For aggregation queries, prioritize entities that make sense for counting/summing
        if intent_type == "aggregation":
            # Entities that are commonly aggregated
            aggregation_entities = ["order", "sale", "transaction", "customer", 
                                  "product", "invoice", "payment", "visit", "asset",
                                  "inspection", "proposal", "facility"]
            
            # Prioritize entities that match aggregation targets
            filtered = []
            for candidate in candidates:
                if any(entity in candidate.lower() for entity in aggregation_entities):
                    filtered.append(candidate)
            
            # If we filtered out everything, return original list
            return filtered if filtered else candidates
        
        # For selection queries, keep all candidates
        return candidates
    
    async def _resolve_entities(self, candidates: List[str], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve entity candidates using all available strategies.
        
        Args:
            candidates: List of entity candidates
            context: Processing context
            
        Returns:
            Dictionary with resolution results
        """
        resolved_entities = {}
        resolution_details = {}
        confidences = []
        
        for entity in candidates:
            best_resolution = None
            best_confidence = 0.0
            
            # Try each resolution strategy
            for strategy in self.resolution_strategies:
                result = await strategy.resolve(entity, context)
                
                # If strategy produced a result with confidence
                if result["resolved_to"] and result["confidence"] > best_confidence:
                    best_resolution = result
                    best_confidence = result["confidence"]
            
            # Store best resolution if found
            if best_resolution:
                resolved_entities[entity] = best_resolution
                confidences.append(best_resolution["confidence"])
                
                resolution_details[entity] = {
                    "strategy_used": best_resolution["strategy"],
                    "resolved_to": best_resolution["resolved_to"],
                    "confidence": best_resolution["confidence"],
                    "resolution_type": best_resolution["resolution_type"]
                }
        
        # Calculate average confidence
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        
        return {
            "entities": resolved_entities,
            "details": resolution_details,
            "confidence": max(confidences) if confidences else 0.0,
            "avg_confidence": avg_confidence
        }
    
    async def _generate_entity_alternatives(self, entity_name: str, entity_info: Dict[str, Any], 
                                    context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate alternative interpretations for an entity.
        
        Args:
            entity_name: Entity name
            entity_info: Entity resolution information
            context: Processing context
            
        Returns:
            List of alternative interpretations
        """
        # First, gather heuristic-based alternatives
        alternatives = []
        
        # For business term matches, suggest direct table interpretation
        if entity_info["resolution_type"] == "business_term":
            alternatives.append({
                "description": f"'{entity_name}' could refer directly to data rather than to {entity_info['resolved_to']}",
                "confidence": 0.5,
                "reason": "Term might be used colloquially rather than as a business entity"
            })
        
        # For semantic concept matches, suggest alternate tables
        elif entity_info["resolution_type"] == "semantic_concept":
            if "all_tables" in entity_info["metadata"] and len(entity_info["metadata"]["all_tables"]) > 1:
                alt_table = entity_info["metadata"]["all_tables"][1]  # Use second table
                alternatives.append({
                    "description": f"'{entity_name}' could refer to {alt_table} instead of {entity_info['resolved_to']}",
                    "confidence": 0.6,
                    "reason": f"Entity is part of a composite concept involving multiple tables"
                })
        
        # For LLM-based matches, suggest general unknown entity
        elif entity_info["resolution_type"] == "llm":
            alternatives.append({
                "description": f"'{entity_name}' might refer to a concept not in the database",
                "confidence": 0.4,
                "reason": "This term was resolved based on linguistic patterns only"
            })
        
        # If we already have sufficient alternatives based on heuristics, return them
        if len(alternatives) >= 2:
            return alternatives
            
        # Otherwise, try generating alternatives using LLM
        try:
            query = context.get("query", "")
            prompt = self.prompt_loader.format_prompt(
                "entity_alternatives",
                query=query,
                entity_name=entity_name,
                current_interpretation=entity_info["resolved_to"],
                confidence=entity_info["confidence"]
            )
            
            # Define schema for structured response
            schema = {
                "type": "object",
                "properties": {
                    "alternatives": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "table_name": {"type": "string"},
                                "description": {"type": "string"},
                                "confidence": {"type": "number"},
                                "reason": {"type": "string"}
                            }
                        }
                    }
                }
            }
            
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "alternatives" in response and response["alternatives"]:
                # Transform response to our standard alternative format
                llm_alternatives = []
                for alt in response["alternatives"]:
                    llm_alternatives.append({
                        "description": alt.get("description", f"Alternative interpretation: {alt.get('table_name', 'unknown')}"),
                        "confidence": alt.get("confidence", 0.5),
                        "reason": alt.get("reason", "Generated by language model")
                    })
                
                # Add LLM-generated alternatives
                alternatives.extend(llm_alternatives)
                
                # Limit to top 3 alternatives by confidence
                alternatives.sort(key=lambda x: x["confidence"], reverse=True)
                return alternatives[:3]
                
        except Exception as e:
            self.logger.error(f"Error generating alternatives with LLM: {e}")
            
        return alternatives
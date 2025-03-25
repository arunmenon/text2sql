"""
Entity Recognition Agent for transparent text2sql reasoning.

This agent coordinates entity extraction and resolution using
multiple strategies, with knowledge boundary awareness.
"""

import logging
import importlib
import os
from typing import Dict, List, Any, Type, Optional

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider
from src.text2sql.reasoning.registry import StrategyRegistry
from src.text2sql.reasoning.config import ConfigLoader
from src.text2sql.reasoning.agents.entity.base import EntityResolutionStrategy, EntityExtractor


class EntityAgent(Agent):
    """Agent for entity recognition with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader,
                config_path: Optional[str] = None):
        """
        Initialize entity agent.
        
        Args:
            llm_client: LLM client for text generation
            graph_context_provider: Provider for semantic graph context
            prompt_loader: Loader for prompt templates
            config_path: Optional path to configuration file
        """
        super().__init__(prompt_loader)
        self.llm_client = llm_client
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        config_path = config_path or ConfigLoader.get_default_config_path("entity_agent")
        self.config = ConfigLoader.load_config(config_path)
        
        # Import all strategy modules to ensure registration
        self._import_strategy_modules()
        
        # Initialize extractors
        self.extractors = self._load_extractors()
        
        # Initialize resolution strategies
        self.resolution_strategies = self._load_strategies()
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "entity"
    
    def _import_strategy_modules(self) -> None:
        """Import all strategy modules to ensure registration."""
        # Import all strategy modules
        strategies_dir = os.path.join(os.path.dirname(__file__), "strategies")
        if os.path.exists(strategies_dir):
            for filename in os.listdir(strategies_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.entity.strategies.{module_name}")
        
        # Import all extractor modules
        extractors_dir = os.path.join(os.path.dirname(__file__), "extractors")
        if os.path.exists(extractors_dir):
            for filename in os.listdir(extractors_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.entity.extractors.{module_name}")
    
    def _load_extractors(self) -> List[EntityExtractor]:
        """
        Load entity extractors based on configuration.
        
        Returns:
            List of entity extractors
        """
        extractors = []
        
        # If config doesn't specify extractors, load default set
        if not self.config or "extractors" not in self.config:
            from src.text2sql.reasoning.agents.entity.extractors.capitalization import CapitalizationExtractor
            from src.text2sql.reasoning.agents.entity.extractors.keyword_based import KeywordBasedExtractor
            from src.text2sql.reasoning.agents.entity.extractors.noun_phrase import NounPhraseExtractor
            
            extractors = [
                CapitalizationExtractor(),
                KeywordBasedExtractor(),
                NounPhraseExtractor()
            ]
        else:
            # Load extractors from config
            for extractor_config in self.config.get("extractors", []):
                if not extractor_config.get("enabled", True):
                    continue
                    
                extractor_type = extractor_config["type"]
                module_path = f"src.text2sql.reasoning.agents.entity.extractors.{extractor_type}"
                
                try:
                    module = importlib.import_module(module_path)
                    
                    # Find extractor class in module
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        try:
                            if (isinstance(attr, type) and 
                                hasattr(attr, "extract") and 
                                hasattr(attr, "name")):
                                extractors.append(attr())
                                break
                        except (AttributeError, TypeError):
                            continue
                except ImportError as e:
                    self.logger.error(f"Error loading extractor {extractor_type}: {e}")
        
        return extractors
    
    def _load_strategies(self) -> List[EntityResolutionStrategy]:
        """
        Load resolution strategies based on configuration.
        
        Returns:
            List of resolution strategies
        """
        strategies = []
        
        # If config doesn't specify strategies, use registry to load them
        if not self.config or "strategies" not in self.config:
            # Try to load common strategies from registry
            for strategy_type in ["direct_table_match", "glossary_term_match", 
                                "semantic_concept_match", "llm_based_resolution"]:
                try:
                    strategy_class = StrategyRegistry.get_strategy(strategy_type)
                    
                    # Special case for LLM-based strategy
                    if "llm" in strategy_type.lower():
                        strategies.append(
                            strategy_class(self.graph_context, self.llm_client, self.prompt_loader)
                        )
                    else:
                        strategies.append(strategy_class(self.graph_context))
                except ValueError as e:
                    self.logger.warning(f"Strategy not found: {strategy_type}")
        else:
            # Load strategies from config and sort by priority
            strategy_configs = sorted(
                [s for s in self.config.get("strategies", []) if s.get("enabled", True)],
                key=lambda s: s.get("priority", 999)
            )
            
            for strategy_config in strategy_configs:
                strategy_type = strategy_config["type"]
                try:
                    strategy_class = StrategyRegistry.get_strategy(strategy_type)
                    
                    # Special case for LLM-based strategy
                    if "llm" in strategy_type.lower():
                        strategies.append(
                            strategy_class(
                                self.graph_context, 
                                self.llm_client, 
                                self.prompt_loader,
                                **strategy_config.get("params", {})
                            )
                        )
                    else:
                        strategies.append(
                            strategy_class(
                                self.graph_context,
                                **strategy_config.get("params", {})
                            )
                        )
                except ValueError as e:
                    self.logger.warning(f"Strategy not found: {strategy_type}")
        
        return strategies
    
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
        candidates = await self._extract_entity_candidates(query)
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
                "strategies_used": [s.strategy_name for s in self.resolution_strategies],
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
    
    async def _extract_entity_candidates(self, query: str) -> Dict[str, Any]:
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
        
        # Run all extractors
        for extractor in self.extractors:
            candidates = extractor.extract(query)
            if candidates:
                methods_used.append(extractor.name)
                all_candidates.update(candidates)
                method_details[extractor.name] = {
                    "description": f"Extracted using {extractor.name} method",
                    "candidates": candidates
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
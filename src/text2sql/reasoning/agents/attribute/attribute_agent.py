"""
Attribute Agent for transparent text2sql reasoning.

This agent coordinates attribute extraction and resolution using
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
from src.text2sql.reasoning.agents.attribute.base import AttributeResolutionStrategy, AttributeExtractor, AttributeType


class AttributeAgent(Agent):
    """Agent for attribute recognition and resolution with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader,
                config_path: Optional[str] = None):
        """
        Initialize attribute agent.
        
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
        config_path = config_path or ConfigLoader.get_default_config_path("attribute_agent")
        self.config = ConfigLoader.load_config(config_path)
        
        # Import all strategy modules to ensure registration
        self._import_strategy_modules()
        
        # Initialize extractors
        self.extractors = self._load_extractors()
        
        # Initialize resolution strategies
        self.resolution_strategies = self._load_strategies()
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "attribute"
    
    def _import_strategy_modules(self) -> None:
        """Import all strategy modules to ensure registration."""
        # Import all strategy modules
        strategies_dir = os.path.join(os.path.dirname(__file__), "strategies")
        if os.path.exists(strategies_dir):
            for filename in os.listdir(strategies_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.attribute.strategies.{module_name}")
        
        # Import all extractor modules
        extractors_dir = os.path.join(os.path.dirname(__file__), "extractors")
        if os.path.exists(extractors_dir):
            for filename in os.listdir(extractors_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.attribute.extractors.{module_name}")
    
    def _load_extractors(self) -> List[AttributeExtractor]:
        """
        Load attribute extractors based on configuration.
        
        Returns:
            List of attribute extractors
        """
        extractors = []
        
        # If config doesn't specify extractors, load default set
        if not self.config or "extractors" not in self.config:
            from src.text2sql.reasoning.agents.attribute.extractors.keyword_based import KeywordBasedAttributeExtractor
            from src.text2sql.reasoning.agents.attribute.extractors.nlp_based import NLPBasedAttributeExtractor
            from src.text2sql.reasoning.agents.attribute.extractors.llm_based import LLMBasedAttributeExtractor
            
            extractors = [
                KeywordBasedAttributeExtractor(),
                NLPBasedAttributeExtractor()
            ]
            
            # Add LLM-based extractor
            extractors.append(LLMBasedAttributeExtractor(self.llm_client, self.prompt_loader))
        else:
            # Load extractors from config
            for extractor_config in self.config.get("extractors", []):
                if not extractor_config.get("enabled", True):
                    continue
                    
                extractor_type = extractor_config["type"]
                module_path = f"src.text2sql.reasoning.agents.attribute.extractors.{extractor_type}"
                
                try:
                    module = importlib.import_module(module_path)
                    
                    # Find extractor class in module
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        try:
                            if (isinstance(attr, type) and 
                                hasattr(attr, "extract") and 
                                hasattr(attr, "name")):
                                
                                # Special case for LLM-based extractor
                                if "llm" in extractor_type.lower():
                                    extractors.append(
                                        attr(self.llm_client, self.prompt_loader)
                                    )
                                else:
                                    extractors.append(attr())
                                break
                        except (AttributeError, TypeError):
                            continue
                except ImportError as e:
                    self.logger.error(f"Error loading extractor {extractor_type}: {e}")
        
        return extractors
    
    def _load_strategies(self) -> List[AttributeResolutionStrategy]:
        """
        Load resolution strategies based on configuration.
        
        Returns:
            List of resolution strategies
        """
        strategies = []
        
        # If config doesn't specify strategies, use registry to load them
        if not self.config or "strategies" not in self.config:
            # Try to load common strategies from registry
            for strategy_type in ["column_based", "semantic_concept", "llm_based"]:
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
        Process query for attribute extraction and resolution.
        
        Args:
            context: Context with query and entity information
            reasoning_stream: Reasoning stream to update
            
        Returns:
            Attribute processing results
        """
        query = context["query"]
        entities = context.get("entities", {})
        boundary_registry = context.get("boundary_registry", BoundaryRegistry())
        
        # Start attribute recognition stage
        stage = reasoning_stream.start_stage(
            name="Attribute Processing",
            description="Identifying and resolving query attributes like filters, aggregations, and groupings"
        )
        
        # Prepare entity context for attribute extraction
        entity_context = {
            "entities": entities,
            "schema": self._get_schema_context(context)
        }
        
        # Step 1: Extract attribute candidates
        attributes = await self._extract_attributes(query, entity_context)
        reasoning_stream.add_step(
            description="Extracting potential query attributes",
            evidence={
                "extraction_methods": attributes["methods"],
                "filter_count": len(attributes["attributes"].get(AttributeType.FILTER, [])),
                "aggregation_count": len(attributes["attributes"].get(AttributeType.AGGREGATION, [])),
                "grouping_count": len(attributes["attributes"].get(AttributeType.GROUPING, [])),
                "sorting_count": len(attributes["attributes"].get(AttributeType.SORTING, [])),
                "limit_count": len(attributes["attributes"].get(AttributeType.LIMIT, [])),
                "details": attributes["details"]
            },
            confidence=attributes["confidence"]
        )
        
        # Step 2: Resolve attributes using strategies
        resolved_attributes = await self._resolve_attributes(attributes["attributes"], context)
        reasoning_stream.add_step(
            description="Resolving attributes to SQL components",
            evidence={
                "strategies_used": [s.strategy_name for s in self.resolution_strategies],
                "attributes_resolved": {
                    "filters": len(resolved_attributes.get(AttributeType.FILTER, [])),
                    "aggregations": len(resolved_attributes.get(AttributeType.AGGREGATION, [])),
                    "groupings": len(resolved_attributes.get(AttributeType.GROUPING, [])),
                    "sortings": len(resolved_attributes.get(AttributeType.SORTING, [])),
                    "limits": len(resolved_attributes.get(AttributeType.LIMIT, []))
                },
                "resolution_details": resolved_attributes.get("details", {})
            },
            confidence=resolved_attributes["confidence"]
        )
        
        # Step 3: Handle unresolvable attributes as knowledge boundaries
        unresolved_attributes = self._identify_unresolved_attributes(attributes["attributes"], resolved_attributes)
        
        if unresolved_attributes:
            for attr_type, attr_values in unresolved_attributes.items():
                for attr in attr_values:
                    boundary = KnowledgeBoundary(
                        boundary_type=BoundaryType.UNMAPPABLE_CONCEPT,
                        component=f"attribute_{attr_type}_{attr.get('attribute_value', 'unknown')}",
                        confidence=0.3,
                        explanation=f"Could not map attribute '{attr.get('attribute_value', '')}' to a database column",
                        suggestions=[
                            "Try using more specific attribute descriptions",
                            "Use column names from the database schema",
                            "Provide more context for the attribute"
                        ]
                    )
                    boundary_registry.add_boundary(boundary)
            
            reasoning_stream.add_step(
                description="Identifying unmappable attributes",
                evidence={
                    "unmapped_attributes": {
                        attr_type: [attr.get("attribute_value") for attr in attr_values]
                        for attr_type, attr_values in unresolved_attributes.items()
                    },
                    "boundary_count": sum(len(attr_values) for attr_values in unresolved_attributes.values()),
                    "requires_clarification": True
                },
                confidence=0.9
            )
        
        # Step 4: Generate alternatives for ambiguous attributes
        alternatives = []
        for attr_type, attr_list in resolved_attributes.items():
            if attr_type != "details" and attr_type != "confidence":
                for attr in attr_list:
                    if 0.4 <= attr.get("confidence", 0.0) < 0.7:
                        alt_options = await self._generate_attribute_alternatives(attr_type, attr, context)
                        alternatives.extend(alt_options)
        
        # Conclude the reasoning stage
        total_resolved = sum(
            len(resolved_attributes.get(attr_type, []))
            for attr_type in [AttributeType.FILTER, AttributeType.AGGREGATION, 
                           AttributeType.GROUPING, AttributeType.SORTING, 
                           AttributeType.LIMIT]
        )
        
        total_unresolved = sum(len(attrs) for attrs in unresolved_attributes.values()) if unresolved_attributes else 0
        
        conclusion = f"Processed {total_resolved} query attributes"
        if total_unresolved > 0:
            conclusion += f" with {total_unresolved} unmappable attributes"
        
        reasoning_stream.conclude_stage(
            conclusion=conclusion,
            final_output=resolved_attributes,
            alternatives=[Alternative(
                description=alt["description"],
                confidence=alt["confidence"],
                reason=alt["reason"]
            ) for alt in alternatives]
        )
        
        # Update context with boundary registry and attribute results
        context["boundary_registry"] = boundary_registry
        context["attributes"] = resolved_attributes
        
        return resolved_attributes
    
    def _get_schema_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema context for entity tables."""
        schema_context = {}
        
        # Get all entities and their table schemas
        entities = context.get("entities", {})
        
        for entity_name, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                table_name = entity_info["resolved_to"]
                
                # Get table schema using the graph context provider
                table_schema = self.graph_context.get_table_schema(table_name)
                
                if table_schema:
                    schema_context[table_name] = table_schema
        
        return schema_context
    
    async def _extract_attributes(self, query: str, entity_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract potential attribute candidates using multiple methods.
        
        Args:
            query: Natural language query
            entity_context: Context with entity information
            
        Returns:
            Dictionary with extraction results
        """
        methods_used = []
        all_attributes = {
            AttributeType.FILTER: [],
            AttributeType.AGGREGATION: [],
            AttributeType.GROUPING: [],
            AttributeType.SORTING: [],
            AttributeType.LIMIT: []
        }
        method_details = {}
        
        # Run all extractors
        for extractor in self.extractors:
            # Handle async extractors
            if hasattr(extractor, "extract") and callable(extractor.extract):
                if "async" in extractor.extract.__code__.co_varnames:
                    # This is an async extractor
                    attributes = await extractor.extract(query, entity_context)
                else:
                    # This is a sync extractor
                    attributes = extractor.extract(query, entity_context)
                
                if attributes:
                    methods_used.append(extractor.name)
                    
                    # Merge attributes by type
                    for attr_type, attr_list in attributes.items():
                        if attr_list:
                            all_attributes[attr_type].extend(attr_list)
                    
                    method_details[extractor.name] = {
                        "description": f"Extracted using {extractor.name} method",
                        "filter_count": len(attributes.get(AttributeType.FILTER, [])),
                        "aggregation_count": len(attributes.get(AttributeType.AGGREGATION, [])),
                        "grouping_count": len(attributes.get(AttributeType.GROUPING, [])),
                        "sorting_count": len(attributes.get(AttributeType.SORTING, [])),
                        "limit_count": len(attributes.get(AttributeType.LIMIT, []))
                    }
        
        # Calculate confidence based on method coverage and attribute counts
        method_count = len(methods_used)
        attr_count = sum(len(attr_list) for attr_list in all_attributes.values())
        
        # Base confidence on method coverage (more methods = higher confidence)
        method_confidence = min(0.6 + (method_count * 0.1), 0.9)
        
        # Adjust based on attribute counts (more attributes = slightly lower confidence)
        attr_adjustment = max(0.0, min(0.2, 0.02 * attr_count))
        
        # Final confidence calculation
        confidence = max(0.5, method_confidence - attr_adjustment)
        
        return {
            "attributes": all_attributes,
            "methods": methods_used,
            "details": method_details,
            "confidence": confidence
        }
    
    async def _resolve_attributes(self, attributes: Dict[str, List[Dict[str, Any]]], 
                              context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve attribute candidates using all available strategies.
        
        Args:
            attributes: Dictionary of attribute candidates by type
            context: Processing context
            
        Returns:
            Dictionary with resolution results
        """
        resolved_attributes = {
            AttributeType.FILTER: [],
            AttributeType.AGGREGATION: [],
            AttributeType.GROUPING: [],
            AttributeType.SORTING: [],
            AttributeType.LIMIT: [],
            "details": {}
        }
        
        confidences = []
        
        # Process each attribute type
        for attr_type, attr_list in attributes.items():
            for attr in attr_list:
                best_resolution = None
                best_confidence = 0.0
                
                # Try each resolution strategy
                for strategy in self.resolution_strategies:
                    result = await strategy.resolve(attr_type, attr, context)
                    
                    # If strategy produced a result with confidence
                    if result["resolved_to"] and result["confidence"] > best_confidence:
                        best_resolution = result
                        best_confidence = result["confidence"]
                
                # Store best resolution if found
                if best_resolution:
                    resolved_attributes[attr_type].append(best_resolution)
                    confidences.append(best_resolution["confidence"])
                    
                    # Add to details
                    attr_value = attr.get("attribute_value", str(attr))
                    resolved_attributes["details"][f"{attr_type}_{attr_value}"] = {
                        "strategy_used": best_resolution["strategy"],
                        "resolved_to": best_resolution["resolved_to"],
                        "confidence": best_resolution["confidence"]
                    }
        
        # Calculate average confidence
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        resolved_attributes["confidence"] = avg_confidence
        
        return resolved_attributes
    
    def _identify_unresolved_attributes(self, candidates: Dict[str, List[Dict[str, Any]]], 
                                    resolved: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Identify attributes that couldn't be resolved.
        
        Args:
            candidates: Dictionary of candidate attributes
            resolved: Dictionary of resolved attributes
            
        Returns:
            Dictionary of unresolved attributes
        """
        unresolved = {}
        
        for attr_type, attr_list in candidates.items():
            # Skip non-attribute keys like "details" or "confidence"
            if attr_type in [AttributeType.FILTER, AttributeType.AGGREGATION, 
                          AttributeType.GROUPING, AttributeType.SORTING, 
                          AttributeType.LIMIT]:
                
                # Get resolved attributes for this type
                resolved_list = resolved.get(attr_type, [])
                
                # Create sets of attribute values for easy comparison
                resolved_values = set()
                for res_attr in resolved_list:
                    if isinstance(res_attr, dict):
                        attr_val = res_attr.get("attribute_value")
                        if attr_val:
                            resolved_values.add(str(attr_val))
                
                # Find unresolved attributes
                unresolved_attrs = []
                for attr in attr_list:
                    attr_val = attr.get("attribute_value", str(attr))
                    
                    # Check if this attribute wasn't resolved
                    if str(attr_val) not in resolved_values:
                        unresolved_attrs.append(attr)
                
                if unresolved_attrs:
                    unresolved[attr_type] = unresolved_attrs
        
        return unresolved
    
    async def _generate_attribute_alternatives(self, attr_type: str, attr_info: Dict[str, Any], 
                                        context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate alternative interpretations for an attribute.
        
        Args:
            attr_type: Attribute type
            attr_info: Attribute resolution information
            context: Processing context
            
        Returns:
            List of alternative interpretations
        """
        # First, gather heuristic-based alternatives
        alternatives = []
        
        # For resolved attributes with medium confidence, suggest alternatives
        if attr_info.get("confidence", 0.0) >= 0.4 and attr_info.get("confidence", 0.0) < 0.7:
            # Check metadata for table and column
            table = attr_info.get("metadata", {}).get("table")
            column = attr_info.get("metadata", {}).get("column")
            
            if table and column:
                # Look for similar columns in the same table
                schema_context = self._get_schema_context(context)
                if table in schema_context:
                    table_schema = schema_context[table]
                    
                    # Find similar columns
                    for alt_column in table_schema.get("columns", []):
                        alt_column_name = alt_column.get("name")
                        
                        # Skip the same column
                        if alt_column_name == column:
                            continue
                        
                        # Check for similarity in column names
                        if alt_column_name and (
                            any(word in alt_column_name.lower() for word in column.lower().split("_")) or
                            any(word in column.lower() for word in alt_column_name.lower().split("_"))
                        ):
                            # Create alternative based on attribute type
                            if attr_type == AttributeType.FILTER:
                                alternatives.append({
                                    "description": f"Filter on {table}.{alt_column_name} instead of {column}",
                                    "confidence": 0.5,
                                    "reason": "Similar column name in the same table"
                                })
                            elif attr_type == AttributeType.AGGREGATION:
                                alternatives.append({
                                    "description": f"Aggregate {table}.{alt_column_name} instead of {column}",
                                    "confidence": 0.5,
                                    "reason": "Similar column name in the same table"
                                })
                            elif attr_type == AttributeType.GROUPING:
                                alternatives.append({
                                    "description": f"Group by {table}.{alt_column_name} instead of {column}",
                                    "confidence": 0.5,
                                    "reason": "Similar column name in the same table"
                                })
                            elif attr_type == AttributeType.SORTING:
                                alternatives.append({
                                    "description": f"Sort by {table}.{alt_column_name} instead of {column}",
                                    "confidence": 0.5,
                                    "reason": "Similar column name in the same table"
                                })
            
        # If we already have sufficient alternatives based on heuristics, return them
        if len(alternatives) >= 2:
            return alternatives
            
        # Otherwise, try generating alternatives using LLM
        try:
            query = context.get("query", "")
            prompt = self.prompt_loader.format_prompt(
                "attribute_alternatives",
                query=query,
                attribute_type=attr_type,
                attribute_value=attr_info.get("attribute_value", ""),
                current_interpretation=attr_info.get("resolved_to", ""),
                confidence=attr_info.get("confidence", 0.5)
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
                                "sql_component": {"type": "string"},
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
                        "description": alt.get("description", f"Alternative interpretation: {alt.get('sql_component', 'unknown')}"),
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
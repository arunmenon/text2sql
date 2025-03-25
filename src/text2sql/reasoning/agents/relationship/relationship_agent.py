"""
Relationship Discovery Agent for transparent text2sql reasoning.

This agent is responsible for discovering relationships between entities
and determining optimal join paths for query execution.
"""

import logging
import importlib
import os
from typing import Dict, List, Any, Tuple, Type, Optional

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider
from src.text2sql.reasoning.registry import StrategyRegistry
from src.text2sql.reasoning.config import ConfigLoader
from src.text2sql.reasoning.agents.relationship.base import JoinPathStrategy, RelationshipExtractor


class RelationshipAgent(Agent):
    """Agent for discovering relationships between entities with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader,
                config_path: Optional[str] = None):
        """
        Initialize relationship agent.
        
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
        config_path = config_path or ConfigLoader.get_default_config_path("relationship_agent")
        self.config = ConfigLoader.load_config(config_path)
        
        # Import all strategy modules to ensure registration
        self._import_strategy_modules()
        
        # Initialize extractors
        self.extractors = self._load_extractors()
        
        # Initialize join path strategies
        self.join_strategies = self._load_strategies()
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "relationship"
    
    def _import_strategy_modules(self) -> None:
        """Import all strategy modules to ensure registration."""
        # Import all strategy modules
        strategies_dir = os.path.join(os.path.dirname(__file__), "strategies")
        if os.path.exists(strategies_dir):
            for filename in os.listdir(strategies_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.relationship.strategies.{module_name}")
        
        # Import all extractor modules
        extractors_dir = os.path.join(os.path.dirname(__file__), "extractors")
        if os.path.exists(extractors_dir):
            for filename in os.listdir(extractors_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    module_name = filename[:-3]
                    importlib.import_module(f"src.text2sql.reasoning.agents.relationship.extractors.{module_name}")
    
    def _load_extractors(self) -> List[RelationshipExtractor]:
        """
        Load relationship extractors based on configuration.
        
        Returns:
            List of relationship extractors
        """
        extractors = []
        
        # If config doesn't specify extractors, load default set
        if not self.config or "extractors" not in self.config:
            from src.text2sql.reasoning.agents.relationship.extractors.keyword_based import KeywordBasedRelationshipExtractor
            
            extractors = [
                KeywordBasedRelationshipExtractor()
            ]
        else:
            # Load extractors from config
            for extractor_config in self.config.get("extractors", []):
                if not extractor_config.get("enabled", True):
                    continue
                    
                extractor_type = extractor_config["type"]
                module_path = f"src.text2sql.reasoning.agents.relationship.extractors.{extractor_type}"
                
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
    
    def _load_strategies(self) -> List[JoinPathStrategy]:
        """
        Load join path strategies based on configuration.
        
        Returns:
            List of join path strategies
        """
        strategies = []
        
        # If config doesn't specify strategies, use registry to load them
        if not self.config or "strategies" not in self.config:
            # Try to load common strategies from registry
            for strategy_type in ["direct_foreign_key", "common_column", 
                                "concept_based_join", "llm_based_join"]:
                try:
                    strategy_class = StrategyRegistry.get_strategy(strategy_type)
                    
                    # Special case for LLM-based strategy
                    if "llm" in strategy_type.lower():
                        strategies.append(
                            strategy_class(self.graph_context, self.llm_client, self.prompt_loader)
                        )
                    else:
                        strategies.append(strategy_class(self.graph_context))
                except ValueError:
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
                except ValueError:
                    self.logger.warning(f"Strategy not found: {strategy_type}")
        
        return strategies
    
    async def process(self, context: Dict[str, Any], reasoning_stream: ReasoningStream) -> Dict[str, Any]:
        """
        Process entity relationships for join path discovery.
        
        Args:
            context: Context with query and entity information
            reasoning_stream: Reasoning stream to update
            
        Returns:
            Relationship discovery results
        """
        query = context["query"]
        entities = context.get("entities", {})
        boundary_registry = context.get("boundary_registry", BoundaryRegistry())
        tenant_id = context.get("tenant_id", "default")
        
        # Start relationship discovery stage
        stage = reasoning_stream.start_stage(
            name="Relationship Discovery",
            description="Discovering relationships between entities for join path determination"
        )
        
        # If we have fewer than 2 entities, no need for join discovery
        if len(entities) < 2:
            reasoning_stream.add_step(
                description="Checking if join paths are needed",
                evidence={
                    "entity_count": len(entities),
                    "requires_joins": False,
                    "explanation": "Query involves only one entity, no joins needed"
                },
                confidence=0.95
            )
            
            reasoning_stream.conclude_stage(
                conclusion="No join paths needed for single-entity query",
                final_output={"relationships": {}, "requires_joins": False}
            )
            
            return {"relationships": {}, "requires_joins": False}
        
        # Get resolved tables from entities
        tables = []
        entity_names = []
        for entity_name, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                tables.append(entity_info["resolved_to"])
                entity_names.append(entity_name)
        
        # Step 1: Analyze entity relationships from query
        relationship_hints = await self._extract_relationship_hints(query, entity_names)
        reasoning_stream.add_step(
            description="Analyzing query for relationship hints",
            evidence={
                "entity_count": len(entities),
                "relationship_hints": relationship_hints
            },
            confidence=relationship_hints["confidence"]
        )
        
        # Step 2: Discover join paths between entities
        join_paths = {}
        
        # Use pairwise (if >2 entities) join path discovery for all table combinations
        entity_pairs = self._get_entity_pairs(entities)
        
        for source_entity, target_entity in entity_pairs:
            source_table = entities[source_entity]["resolved_to"]
            target_table = entities[target_entity]["resolved_to"]
            
            if source_table == target_table:
                continue  # Skip self-joins
            
            join_result = await self._discover_join_path(
                source_table, 
                target_table, 
                context
            )
            
            pair_key = f"{source_entity}_to_{target_entity}"
            join_paths[pair_key] = {
                "source_entity": source_entity,
                "target_entity": target_entity,
                "source_table": source_table,
                "target_table": target_table,
                "join_path": join_result["join_path"],
                "confidence": join_result["confidence"],
                "strategy": join_result["strategy"]
            }
            
            # Add alternatives if available
            if "alternative_paths" in join_result and join_result["alternative_paths"]:
                join_paths[pair_key]["alternative_paths"] = join_result["alternative_paths"]
        
        reasoning_stream.add_step(
            description="Discovering join paths between entities",
            evidence={
                "join_paths_found": {k: {
                    "source": v["source_entity"],
                    "target": v["target_entity"],
                    "confidence": v["confidence"],
                    "strategy": v["strategy"]
                } for k, v in join_paths.items()},
                "total_pairs": len(entity_pairs),
                "total_paths_found": len([p for p in join_paths.values() if p["join_path"] is not None])
            },
            confidence=max([p["confidence"] for p in join_paths.values()]) if join_paths else 0.5
        )
        
        # Step 3: Handle missing relationships as knowledge boundaries
        missing_relationships = [
            (entities[pair[0]]["resolved_to"], entities[pair[1]]["resolved_to"])
            for pair in entity_pairs
            if f"{pair[0]}_to_{pair[1]}" in join_paths and
            join_paths[f"{pair[0]}_to_{pair[1]}"]["join_path"] is None
        ]
        
        if missing_relationships:
            for source_table, target_table in missing_relationships:
                boundary = KnowledgeBoundary(
                    boundary_type=BoundaryType.MISSING_RELATIONSHIP,
                    component=f"relationship_{source_table}_to_{target_table}",
                    confidence=0.2,
                    explanation=f"Could not determine how to join {source_table} with {target_table}",
                    suggestions=[
                        "The tables might not be directly related",
                        "Consider using a different entity that relates to both",
                        "Check if intermediate tables are needed"
                    ]
                )
                boundary_registry.add_boundary(boundary)
            
            reasoning_stream.add_step(
                description="Identifying missing relationships between entities",
                evidence={
                    "missing_relationships": [
                        f"{source} to {target}" for source, target in missing_relationships
                    ],
                    "boundary_count": len(missing_relationships),
                    "requires_clarification": len(missing_relationships) > 0
                },
                confidence=0.9
            )
        
        # Step 4: Generate alternatives for ambiguous join paths
        alternatives = []
        for path_key, path_info in join_paths.items():
            if path_info["join_path"] is not None and 0.4 <= path_info["confidence"] < 0.8:
                if "alternative_paths" in path_info and path_info["alternative_paths"]:
                    for alt_path in path_info["alternative_paths"]:
                        alternatives.append(Alternative(
                            description=f"Alternative join between {path_info['source_entity']} and {path_info['target_entity']}",
                            confidence=alt_path.get("confidence", 0.5),
                            reason=f"Based on {alt_path.get('strategy', 'alternative strategy')}"
                        ))
        
        # Step 5: Determine optimal join tree if more than 2 entities
        join_tree = None
        if len(entities) > 2:
            join_tree = self._determine_optimal_join_tree(entities, join_paths)
            reasoning_stream.add_step(
                description="Determining optimal join order for multiple entities",
                evidence={
                    "entity_count": len(entities),
                    "join_tree": join_tree,
                    "approach": "Minimizing join complexity and maximizing confidence"
                },
                confidence=0.8
            )
        
        # Prepare final result
        result = {
            "relationships": join_paths,
            "requires_joins": len(entities) > 1,
            "join_tree": join_tree,
            "missing_relationships": [
                {"source": source, "target": target} 
                for source, target in missing_relationships
            ] if missing_relationships else []
        }
        
        # Conclude the reasoning stage
        path_count = len([p for p in join_paths.values() if p["join_path"] is not None])
        missing_count = len(missing_relationships)
        
        conclusion = f"Discovered {path_count} join paths between {len(entities)} entities"
        if missing_count > 0:
            conclusion += f" with {missing_count} unresolved relationships"
        
        reasoning_stream.conclude_stage(
            conclusion=conclusion,
            final_output=result,
            alternatives=alternatives
        )
        
        # Update context with boundary registry
        context["boundary_registry"] = boundary_registry
        context["relationships"] = result
        
        return result
    
    async def _extract_relationship_hints(self, query: str, entities: List[str]) -> Dict[str, Any]:
        """
        Extract relationship hints from the query using multiple extractors.
        
        Args:
            query: Natural language query
            entities: List of entity names
            
        Returns:
            Dictionary with relationship hints
        """
        all_hints = {}
        methods_used = []
        confidence = 0.5  # Default confidence
        
        # Run all extractors
        for extractor in self.extractors:
            hints = extractor.extract(query, entities)
            if hints and hints.get("relationship_types"):
                methods_used.append(extractor.name)
                # Merge relationship hints
                for rel_key, rel_info in hints["relationship_types"].items():
                    if rel_key not in all_hints or rel_info["confidence"] > all_hints[rel_key]["confidence"]:
                        all_hints[rel_key] = rel_info
        
        # Calculate overall confidence
        if all_hints:
            confidence = max([r["confidence"] for r in all_hints.values()])
        
        return {
            "relationship_types": all_hints,
            "methods": methods_used,
            "confidence": confidence
        }
    
    def _get_entity_pairs(self, entities: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str]]:
        """Get all entity pairs that need to be joined."""
        entity_names = [
            name for name, info in entities.items() 
            if "resolved_to" in info and info["resolved_to"]
        ]
        
        # Generate all possible pairs
        pairs = []
        for i, entity1 in enumerate(entity_names):
            for entity2 in entity_names[i+1:]:
                pairs.append((entity1, entity2))
        
        return pairs
    
    async def _discover_join_path(self, source_table: str, target_table: str, 
                               context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Discover join paths between two tables using all available strategies.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Context with query information
            
        Returns:
            Dictionary with join path result
        """
        best_result = None
        best_confidence = 0.0
        
        # Try each join strategy
        for strategy in self.join_strategies:
            result = await strategy.resolve(source_table, target_table, context)
            
            # If strategy produced a result with confidence and it's better than current best
            if result["join_path"] and result["confidence"] > best_confidence:
                best_result = result
                best_confidence = result["confidence"]
        
        # If no strategy found a join path
        if not best_result:
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": None,
                "confidence": 0.0,
                "strategy": "none",
                "alternative_paths": []
            }
        
        return best_result
    
    def _determine_optimal_join_tree(self, entities: Dict[str, Dict[str, Any]], 
                                 join_paths: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Determine optimal join tree for multiple entities.
        
        Args:
            entities: Resolved entities
            join_paths: Discovered join paths
            
        Returns:
            Dictionary with optimal join tree
        """
        # Get entity tables with their confidence
        tables = {
            entity_name: {
                "table": info["resolved_to"],
                "confidence": info["confidence"]
            }
            for entity_name, info in entities.items() 
            if "resolved_to" in info and info["resolved_to"]
        }
        
        # Get valid join paths
        valid_paths = {
            key: path for key, path in join_paths.items() 
            if path["join_path"] is not None
        }
        
        if not valid_paths:
            return None
        
        # Sort paths by confidence
        sorted_paths = sorted(
            valid_paths.values(), 
            key=lambda p: p["confidence"], 
            reverse=True
        )
        
        # Strategy selection
        strategy = self.config.get("join_tree", {}).get("strategy", "confidence_based")
        
        if strategy == "confidence_based":
            # Start with the highest confidence path
            root_path = sorted_paths[0]
            join_tree = {
                "root": root_path["source_entity"],
                "joins": [
                    {
                        "from_entity": root_path["source_entity"],
                        "to_entity": root_path["target_entity"],
                        "join_path": root_path["join_path"],
                        "confidence": root_path["confidence"]
                    }
                ]
            }
            
            # Track which entities are already in the tree
            entities_in_tree = {root_path["source_entity"], root_path["target_entity"]}
            
            # Add remaining entities to the tree
            remaining_entities = set(tables.keys()) - entities_in_tree
            
            while remaining_entities and valid_paths:
                # Find best path connecting any entity in tree to any entity not in tree
                best_extension = None
                best_confidence = 0.0
                
                for path in sorted_paths:
                    source = path["source_entity"]
                    target = path["target_entity"]
                    
                    # Check if this path connects an entity in the tree to one outside
                    if (source in entities_in_tree and target not in entities_in_tree):
                        if path["confidence"] > best_confidence:
                            best_extension = {
                                "from_entity": source,
                                "to_entity": target,
                                "join_path": path["join_path"],
                                "confidence": path["confidence"]
                            }
                            best_confidence = path["confidence"]
                    
                    # Check the reverse direction too
                    elif (target in entities_in_tree and source not in entities_in_tree):
                        if path["confidence"] > best_confidence:
                            best_extension = {
                                "from_entity": target,
                                "to_entity": source,
                                "join_path": path["join_path"],  # This should be reversed in a real implementation
                                "confidence": path["confidence"]
                            }
                            best_confidence = path["confidence"]
                
                # If we found a valid extension, add it to the tree
                if best_extension:
                    join_tree["joins"].append(best_extension)
                    entities_in_tree.add(best_extension["to_entity"])
                    remaining_entities.remove(best_extension["to_entity"])
                else:
                    # No valid extension found, break
                    break
        
        elif strategy == "star_schema":
            # Find the entity that connects to the most other entities
            entity_connections = {}
            for path in valid_paths.values():
                source = path["source_entity"]
                target = path["target_entity"]
                
                entity_connections[source] = entity_connections.get(source, 0) + 1
                entity_connections[target] = entity_connections.get(target, 0) + 1
            
            # Find entity with most connections
            hub_entity = max(entity_connections.items(), key=lambda x: x[1])[0]
            
            # Create joins from hub entity to all others
            join_tree = {
                "root": hub_entity,
                "joins": []
            }
            
            for path in valid_paths.values():
                source = path["source_entity"]
                target = path["target_entity"]
                
                if source == hub_entity:
                    join_tree["joins"].append({
                        "from_entity": source,
                        "to_entity": target,
                        "join_path": path["join_path"],
                        "confidence": path["confidence"]
                    })
                elif target == hub_entity:
                    join_tree["joins"].append({
                        "from_entity": target,
                        "to_entity": source,
                        "join_path": path["join_path"],  # This should be reversed in a real implementation
                        "confidence": path["confidence"]
                    })
        
        # Calculate overall confidence
        if join_tree and join_tree["joins"]:
            join_tree["confidence"] = sum(j["confidence"] for j in join_tree["joins"]) / len(join_tree["joins"])
        else:
            join_tree = None
            
        return join_tree
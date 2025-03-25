"""
Relationship Discovery Agent for transparent text2sql reasoning.

This agent is responsible for discovering relationships between entities
and determining optimal join paths for query execution.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent, ResolutionStrategy
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider


class JoinPathStrategy(ResolutionStrategy):
    """Base class for join path resolution strategies."""
    
    def __init__(self, graph_context_provider: SemanticGraphContextProvider):
        """
        Initialize join path strategy.
        
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
        return "Base join path resolution strategy"


class DirectForeignKeyStrategy(JoinPathStrategy):
    """Strategy for finding direct foreign key relationships."""
    
    @property
    def description(self) -> str:
        return "Discovers direct foreign key relationships between tables"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find direct foreign key relationships.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Get direct foreign key relationships from the graph
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.7,
            strategy="direct_fk"
        )
        
        if join_paths:
            # Select the most direct path (fewest joins)
            join_paths.sort(key=lambda p: len(p.get("path", [])))
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": 0.9,  # High confidence for direct FK relationships
                "strategy": self.strategy_name,
                "alternative_paths": join_paths[1:3] if len(join_paths) > 1 else []
            }
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }


class CommonColumnStrategy(JoinPathStrategy):
    """Strategy for finding relationships through common column names."""
    
    @property
    def description(self) -> str:
        return "Discovers relationships through common column names and patterns"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships through common column names.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Get paths based on common column naming patterns
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.5,
            strategy="common_columns"
        )
        
        if join_paths:
            # Sort by confidence
            join_paths.sort(key=lambda p: p.get("confidence", 0), reverse=True)
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": min(0.8, best_path.get("confidence", 0.6)),  # Cap at 0.8
                "strategy": self.strategy_name,
                "alternative_paths": join_paths[1:3] if len(join_paths) > 1 else []
            }
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }


class ConceptBasedJoinStrategy(JoinPathStrategy):
    """Strategy for finding relationships through semantic concepts."""
    
    @property
    def description(self) -> str:
        return "Discovers relationships using semantic concepts in the knowledge graph"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships through semantic concepts.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        
        # Look for concepts that relate these tables
        join_paths = self.graph_context.get_join_paths(
            tenant_id=tenant_id, 
            source_table=source_table,
            target_table=target_table,
            min_confidence=0.4,
            strategy="semantic_concepts"
        )
        
        if join_paths:
            # Sort by confidence and path length
            join_paths.sort(key=lambda p: (p.get("confidence", 0), -len(p.get("path", []))), reverse=True)
            best_path = join_paths[0]
            
            return {
                "source_table": source_table,
                "target_table": target_table,
                "join_path": best_path,
                "confidence": min(0.85, best_path.get("confidence", 0.7)),  # Cap at 0.85
                "strategy": self.strategy_name,
                "alternative_paths": join_paths[1:3] if len(join_paths) > 1 else [],
                "concept": best_path.get("concept_info", {})
            }
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }


class LLMBasedJoinStrategy(JoinPathStrategy):
    """Strategy for finding relationships using LLM."""
    
    def __init__(self, graph_context_provider: SemanticGraphContextProvider, 
                llm_client: LLMClient, prompt_loader: PromptLoader):
        """
        Initialize LLM-based join strategy.
        
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
        return "Uses language model to discover potential join paths based on schema semantics"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find relationships using LLM.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            context: Resolution context
            
        Returns:
            Resolution result with join paths
        """
        tenant_id = context.get("tenant_id", "default")
        query = context.get("query", "")
        
        # Get schema context for tables
        schema_context = self.graph_context.get_schema_context(tenant_id)
        
        # Format tables info for the prompt
        tables_info = self._format_tables_info(schema_context, source_table, target_table)
        
        # Create prompt for LLM resolution
        prompt = self.prompt_loader.format_prompt(
            "relationship_discovery",
            query=query,
            source_table=source_table,
            target_table=target_table,
            tables_info=tables_info
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "join_path": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "from_table": {"type": "string"},
                            "from_column": {"type": "string"},
                            "to_table": {"type": "string"},
                            "to_column": {"type": "string"},
                            "relationship_type": {"type": "string"}
                        }
                    }
                },
                "confidence": {"type": "number"},
                "reasoning": {"type": "string"}
            }
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "join_path" in response and response["join_path"]:
                join_path = {
                    "path": response["join_path"],
                    "confidence": response.get("confidence", 0.6),
                    "reasoning": response.get("reasoning", "")
                }
                
                return {
                    "source_table": source_table,
                    "target_table": target_table,
                    "join_path": join_path,
                    "confidence": join_path["confidence"],
                    "strategy": self.strategy_name,
                    "alternative_paths": [],
                    "reasoning": join_path["reasoning"]
                }
        except Exception as e:
            self.logger.error(f"Error in LLM join resolution: {e}")
        
        return {
            "source_table": source_table,
            "target_table": target_table,
            "join_path": None,
            "confidence": 0.0,
            "strategy": self.strategy_name,
            "alternative_paths": []
        }
    
    def _format_tables_info(self, schema_context: Dict[str, Any], source_table: str, target_table: str) -> str:
        """
        Format tables information for LLM prompts.
        
        Args:
            schema_context: Schema context information
            source_table: Source table name
            target_table: Target table name
            
        Returns:
            Formatted string with tables information
        """
        tables_info = []
        tables = schema_context.get("tables", {})
        
        # Add source table info
        if source_table in tables:
            source_info = tables[source_table]
            columns = source_info.get("columns", [])
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in columns
            ])
            
            tables_info.append(f"SOURCE TABLE: {source_table}\n  Description: {source_info.get('description', 'No description')}\n  Columns:\n    {column_str}")
        
        # Add target table info
        if target_table in tables:
            target_info = tables[target_table]
            columns = target_info.get("columns", [])
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in columns
            ])
            
            tables_info.append(f"TARGET TABLE: {target_table}\n  Description: {target_info.get('description', 'No description')}\n  Columns:\n    {column_str}")
        
        # Add intermediate tables that might be relevant
        # Look for tables with foreign keys to both source and target
        intermediate_tables = []
        for table_name, table_info in tables.items():
            if table_name not in [source_table, target_table]:
                fks = table_info.get("foreign_keys", [])
                refs_source = any(fk.get("referenced_table") == source_table for fk in fks)
                refs_target = any(fk.get("referenced_table") == target_table for fk in fks)
                
                if refs_source or refs_target:
                    intermediate_tables.append(table_name)
        
        # Add up to 3 intermediate tables
        for table_name in intermediate_tables[:3]:
            table_info = tables[table_name]
            columns = table_info.get("columns", [])
            
            key_columns = [col for col in columns if "key" in col.get("name", "").lower() or "id" in col.get("name", "").lower()]
            
            column_str = "\n    ".join([
                f"- {col.get('name')} ({col.get('data_type', 'unknown')}): {col.get('description', '')}" 
                for col in key_columns[:5]  # Only include key columns
            ])
            
            tables_info.append(f"INTERMEDIATE TABLE: {table_name}\n  Description: {table_info.get('description', 'No description')}\n  Key Columns:\n    {column_str}")
        
        return "\n\n".join(tables_info)


class RelationshipAgent(Agent):
    """Agent for discovering relationships between entities with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader):
        """
        Initialize relationship agent.
        
        Args:
            llm_client: LLM client for text generation
            graph_context_provider: Provider for semantic graph context
            prompt_loader: Loader for prompt templates
        """
        super().__init__(prompt_loader)
        self.llm_client = llm_client
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(__name__)
        
        # Initialize join path strategies
        self.join_strategies = [
            DirectForeignKeyStrategy(graph_context_provider),
            CommonColumnStrategy(graph_context_provider),
            ConceptBasedJoinStrategy(graph_context_provider),
            LLMBasedJoinStrategy(graph_context_provider, llm_client, prompt_loader)
        ]
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "relationship"
    
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
        for entity_name, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                tables.append(entity_info["resolved_to"])
        
        # Step 1: Analyze entity relationships from query
        relationship_hints = await self._extract_relationship_hints(query, entities)
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
    
    async def _extract_relationship_hints(self, query: str, entities: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract relationship hints from the query.
        
        Args:
            query: Natural language query
            entities: Resolved entities
            
        Returns:
            Dictionary with relationship hints
        """
        # Get entity names
        entity_names = list(entities.keys())
        
        # Look for common relationship patterns
        patterns = {
            "joins": ["join", "with", "and", "between", "related to", "linked to"],
            "ownership": ["has", "have", "owns", "own", "belonging to", "possessed by"],
            "membership": ["in", "contains", "member of", "part of", "within"]
        }
        
        relationship_types = {}
        confidence = 0.5  # Default confidence
        
        # Check for each entity pair
        query_lower = query.lower()
        for i, entity1 in enumerate(entity_names):
            for entity2 in entity_names[i+1:]:
                rel_type = None
                rel_confidence = 0.0
                
                # Check proximity of entities in query
                if entity1.lower() in query_lower and entity2.lower() in query_lower:
                    words_between = self._count_words_between(query_lower, entity1.lower(), entity2.lower())
                    proximity_score = max(0, 1.0 - (words_between / 10))  # 0 words = 1.0, 10+ words = 0.0
                    
                    # Check for relationship patterns
                    for rel_pattern, keywords in patterns.items():
                        for keyword in keywords:
                            if keyword in query_lower:
                                # Check if keyword is between entities
                                if self._is_keyword_between(query_lower, entity1.lower(), entity2.lower(), keyword):
                                    rel_type = rel_pattern
                                    # Higher confidence for explicit relationship keywords between entities
                                    rel_confidence = 0.7 + (proximity_score * 0.2)  # 0.7-0.9 range
                                    break
                    
                    # If no specific pattern matched but entities are close, infer general relationship
                    if not rel_type and proximity_score > 0.5:
                        rel_type = "general"
                        rel_confidence = 0.6
                
                if rel_type:
                    relationship_types[f"{entity1}_to_{entity2}"] = {
                        "type": rel_type,
                        "confidence": rel_confidence,
                        "entities": [entity1, entity2]
                    }
        
        # Calculate overall confidence
        overall_confidence = max([r["confidence"] for r in relationship_types.values()]) if relationship_types else 0.5
        
        return {
            "relationship_types": relationship_types,
            "confidence": overall_confidence
        }
    
    def _count_words_between(self, query: str, entity1: str, entity2: str) -> int:
        """Count words between two entities in the query."""
        idx1 = query.find(entity1)
        idx2 = query.find(entity2)
        
        if idx1 == -1 or idx2 == -1:
            return 100  # Large number if not found
        
        # Get text between entities
        start_idx = idx1 + len(entity1) if idx1 < idx2 else idx2 + len(entity2)
        end_idx = idx2 if idx1 < idx2 else idx1
        
        if start_idx >= end_idx:
            return 0  # Adjacent entities
        
        between_text = query[start_idx:end_idx].strip()
        return len(between_text.split())
    
    def _is_keyword_between(self, query: str, entity1: str, entity2: str, keyword: str) -> bool:
        """Check if a keyword appears between two entities."""
        idx1 = query.find(entity1)
        idx2 = query.find(entity2)
        keyword_idx = query.find(keyword)
        
        if idx1 == -1 or idx2 == -1 or keyword_idx == -1:
            return False
        
        # Ensure keyword is between entities
        if idx1 < idx2:
            return idx1 + len(entity1) <= keyword_idx < idx2
        else:
            return idx2 + len(entity2) <= keyword_idx < idx1
    
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
        # This is a simplified version of join tree optimization
        # In a real implementation, we would use more sophisticated algorithms

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
        
        # Calculate overall confidence
        if join_tree["joins"]:
            join_tree["confidence"] = sum(j["confidence"] for j in join_tree["joins"]) / len(join_tree["joins"])
        else:
            join_tree["confidence"] = 0.0
            
        return join_tree
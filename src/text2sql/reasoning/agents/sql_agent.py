"""
SQL Generation Agent for transparent text2sql reasoning.

This agent is responsible for generating SQL queries based on the information
gathered by previous agents in the pipeline, using LLM for generation.
"""

import logging
import json
from typing import Dict, List, Any, Optional

from src.llm.client import LLMClient
from src.text2sql.reasoning.agents.base import Agent
from src.text2sql.reasoning.models import ReasoningStream, Alternative
from src.text2sql.reasoning.knowledge_boundary import BoundaryType, KnowledgeBoundary, BoundaryRegistry
from src.text2sql.reasoning.agents.attribute.base import AttributeType
from src.text2sql.utils.prompt_loader import PromptLoader
from src.text2sql.utils.graph_context_provider import SemanticGraphContextProvider


class SQLAgent(Agent):
    """Agent for SQL generation with knowledge boundary awareness."""
    
    def __init__(self, llm_client: LLMClient, 
                graph_context_provider: SemanticGraphContextProvider,
                prompt_loader: PromptLoader):
        """
        Initialize SQL agent.
        
        Args:
            llm_client: LLM client for text generation
            graph_context_provider: Provider for semantic graph context
            prompt_loader: Loader for prompt templates
        """
        super().__init__(prompt_loader)
        self.llm_client = llm_client
        self.graph_context = graph_context_provider
        self.logger = logging.getLogger(__name__)
    
    def _get_agent_type(self) -> str:
        """Return agent type for prompt naming."""
        return "sql"
    
    async def process(self, context: Dict[str, Any], reasoning_stream: ReasoningStream) -> Dict[str, Any]:
        """
        Process query for SQL generation.
        
        Args:
            context: Context with query and previous agent results
            reasoning_stream: Reasoning stream to update
            
        Returns:
            SQL generation results
        """
        query = context["query"]
        intent = context.get("intent", {})
        entities = context.get("entities", {})
        relationships = context.get("relationships", {})
        attributes = context.get("attributes", {})
        boundary_registry = context.get("boundary_registry", BoundaryRegistry())
        tenant_id = context.get("tenant_id", "default")
        
        # Start SQL generation stage
        stage = reasoning_stream.start_stage(
            name="SQL Generation",
            description="Generating SQL query from intent, entities, and relationships"
        )
        
        # Check if we have required information to generate SQL
        if not entities:
            boundary = KnowledgeBoundary(
                boundary_type=BoundaryType.UNMAPPABLE_CONCEPT,
                component="sql_generation",
                confidence=0.2,
                explanation="Cannot generate SQL without resolved entities",
                suggestions=[
                    "Try reformulating your query with more specific entity names",
                    "Refer to tables by their business names"
                ]
            )
            boundary_registry.add_boundary(boundary)
            
            reasoning_stream.add_step(
                description="Checking for required information",
                evidence={
                    "has_intent": bool(intent),
                    "has_entities": bool(entities),
                    "requires_joins": relationships.get("requires_joins", False) if relationships else False,
                    "has_boundaries": len(boundary_registry.get_all_boundaries()) > 0,
                    "missing_entities": True
                },
                confidence=0.9
            )
            
            reasoning_stream.conclude_stage(
                conclusion="Cannot generate SQL without resolved entities",
                final_output={"sql": "", "explanation": "Could not generate SQL due to missing entities"}
            )
            
            context["boundary_registry"] = boundary_registry
            return {"sql": "", "explanation": "Could not generate SQL due to missing entities"}
        
        # Step 1: Prepare schema context
        schema_context = await self._prepare_schema_context(tenant_id, entities, relationships)
        reasoning_stream.add_step(
            description="Preparing schema context for SQL generation",
            evidence={
                "tables_included": list(schema_context["tables"].keys()),
                "has_joins": relationships and relationships.get("requires_joins", False)
            },
            confidence=0.9
        )
        
        # Step 2: Generate SQL using LLM
        sql_generation = await self._generate_sql(query, intent, entities, relationships, schema_context)
        reasoning_stream.add_step(
            description="Generating SQL query using language model",
            evidence={
                "intent_type": intent.get("intent_type", "unknown"),
                "entity_count": len(entities),
                "needs_joins": relationships.get("requires_joins", False) if relationships else False,
                "sql_approach": sql_generation["approach"]
            },
            confidence=sql_generation["confidence"]
        )
        
        # Step 3: Validate SQL
        validation_result = await self._validate_sql(sql_generation["sql"], schema_context)
        reasoning_stream.add_step(
            description="Validating generated SQL",
            evidence={
                "validation_method": validation_result["method"],
                "is_valid": validation_result["is_valid"],
                "issues": validation_result["issues"]
            },
            confidence=validation_result["confidence"]
        )
        
        # If SQL is invalid and has issues, create boundary
        if not validation_result["is_valid"]:
            boundary = KnowledgeBoundary(
                boundary_type=BoundaryType.COMPLEX_IMPLEMENTATION,
                component="sql_validation",
                confidence=0.3,
                explanation=f"Generated SQL has issues: {', '.join(validation_result['issues'])}",
                suggestions=[
                    "Try simplifying your query",
                    "Provide more context about table relationships"
                ]
            )
            boundary_registry.add_boundary(boundary)
            
            # Try to generate a simpler alternative
            simplified_sql = await self._generate_simplified_sql(query, intent, entities, schema_context)
            if simplified_sql["sql"]:
                sql_generation["alternatives"] = [simplified_sql]
                sql_generation["has_simpler_alternative"] = True
        
        # Step 4: Generate alternatives if needed
        alternatives = []
        if sql_generation["confidence"] < 0.8 or intent.get("confidence", 1.0) < 0.8:
            alternatives = await self._generate_alternatives(query, intent, entities, relationships, schema_context)
            
            reasoning_stream.add_step(
                description="Generating alternative SQL interpretations",
                evidence={
                    "alternatives_count": len(alternatives),
                    "alternative_approaches": [alt["approach"] for alt in alternatives]
                },
                confidence=0.85
            )
        
        # Conclude the reasoning stage
        sql_result = {
            "sql": sql_generation["sql"],
            "explanation": sql_generation["explanation"],
            "confidence": sql_generation["confidence"],
            "approach": sql_generation["approach"],
            "is_valid": validation_result["is_valid"],
            "alternatives": [alt for alt in alternatives if alt["sql"] != sql_generation["sql"]]
        }
        
        conclusion = "Generated SQL based on intent and entities"
        if relationships and relationships.get("requires_joins", False):
            conclusion += " with necessary joins"
        
        reasoning_stream.conclude_stage(
            conclusion=conclusion,
            final_output=sql_result,
            alternatives=[Alternative(
                description=alt["explanation"],
                confidence=alt["confidence"],
                reason=alt["approach"]
            ) for alt in sql_result["alternatives"]]
        )
        
        # Update context with boundary registry
        context["boundary_registry"] = boundary_registry
        context["sql_result"] = sql_result
        
        return sql_result
    
    async def _prepare_schema_context(self, tenant_id: str, 
                                   entities: Dict[str, Dict[str, Any]],
                                   relationships: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Prepare schema context for SQL generation.
        
        Args:
            tenant_id: Tenant ID
            entities: Resolved entities
            relationships: Relationship information
            
        Returns:
            Schema context for SQL generation
        """
        # Get full schema context
        full_context = self.graph_context.get_schema_context(tenant_id)
        
        # Filter to only include tables needed for this query
        entity_tables = set()
        for _, entity_info in entities.items():
            if "resolved_to" in entity_info and entity_info["resolved_to"]:
                entity_tables.add(entity_info["resolved_to"])
        
        # Add intermediate tables for joins if needed
        if relationships and relationships.get("requires_joins", False):
            for _, rel_info in relationships.get("relationships", {}).items():
                if "join_path" in rel_info and rel_info["join_path"]:
                    path = rel_info["join_path"].get("path", [])
                    for step in path:
                        if "from_table" in step:
                            entity_tables.add(step["from_table"])
                        if "to_table" in step:
                            entity_tables.add(step["to_table"])
        
        # Create filtered context
        filtered_tables = {}
        all_tables = full_context.get("tables", {})
        
        for table_name in entity_tables:
            if table_name in all_tables:
                filtered_tables[table_name] = all_tables[table_name]
        
        # Add table relationship information
        table_relationships = []
        if relationships and relationships.get("requires_joins", False):
            for rel_key, rel_info in relationships.get("relationships", {}).items():
                if "join_path" in rel_info and rel_info["join_path"]:
                    table_relationships.append({
                        "source_table": rel_info.get("source_table", ""),
                        "target_table": rel_info.get("target_table", ""),
                        "join_path": rel_info["join_path"].get("path", [])
                    })
        
        return {
            "tables": filtered_tables,
            "relationships": table_relationships,
            "glossary_terms": {
                term: info for term, info in full_context.get("glossary_terms", {}).items()
                if any(table in entity_tables for table in info.get("mapped_tables", []))
            }
        }
    
    async def _generate_sql(self, query: str, intent: Dict[str, Any], 
                         entities: Dict[str, Dict[str, Any]],
                         relationships: Optional[Dict[str, Any]],
                         schema_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate SQL using LLM.
        
        Args:
            query: Natural language query
            intent: Intent information
            entities: Resolved entities
            relationships: Relationship information
            schema_context: Schema context
            
        Returns:
            SQL generation result
        """
        # Format schema context for the prompt
        schema_json = json.dumps({
            "tables": {
                name: {
                    "description": info.get("description", ""),
                    "columns": [
                        {
                            "name": col.get("name", ""),
                            "data_type": col.get("data_type", ""),
                            "description": col.get("description", "")
                        }
                        for col in info.get("columns", [])
                    ]
                }
                for name, info in schema_context["tables"].items()
            }
        }, indent=2)
        
        # Format relationships for the prompt
        entity_mapping = json.dumps({
            entity_name: info["resolved_to"]
            for entity_name, info in entities.items()
            if "resolved_to" in info
        }, indent=2)
        
        relationships_json = "null"
        if relationships and relationships.get("requires_joins", False) and relationships.get("relationships"):
            rel_json = {}
            for rel_key, rel_info in relationships["relationships"].items():
                if rel_info.get("join_path"):
                    rel_json[rel_key] = {
                        "source_entity": rel_info["source_entity"],
                        "target_entity": rel_info["target_entity"],
                        "source_table": rel_info["source_table"],
                        "target_table": rel_info["target_table"],
                        "join_path": rel_info["join_path"].get("path", [])
                    }
            relationships_json = json.dumps(rel_json, indent=2)
        
        # Format attributes for the prompt
        attributes_json = "null"
        if attributes:
            # Extract SQL components from attributes
            attr_components = {}
            
            if AttributeType.FILTER in attributes:
                attr_components["filters"] = [
                    {"sql": attr.get("resolved_to"), "confidence": attr.get("confidence", 0.0)}
                    for attr in attributes.get(AttributeType.FILTER, [])
                    if attr.get("resolved_to")
                ]
            
            if AttributeType.AGGREGATION in attributes:
                attr_components["aggregations"] = [
                    {"sql": attr.get("resolved_to"), "confidence": attr.get("confidence", 0.0)}
                    for attr in attributes.get(AttributeType.AGGREGATION, [])
                    if attr.get("resolved_to")
                ]
            
            if AttributeType.GROUPING in attributes:
                attr_components["groupings"] = [
                    {"sql": attr.get("resolved_to"), "confidence": attr.get("confidence", 0.0)}
                    for attr in attributes.get(AttributeType.GROUPING, [])
                    if attr.get("resolved_to")
                ]
            
            if AttributeType.SORTING in attributes:
                attr_components["sortings"] = [
                    {"sql": attr.get("resolved_to"), "confidence": attr.get("confidence", 0.0)}
                    for attr in attributes.get(AttributeType.SORTING, [])
                    if attr.get("resolved_to")
                ]
            
            if AttributeType.LIMIT in attributes:
                attr_components["limits"] = [
                    {"sql": attr.get("resolved_to"), "confidence": attr.get("confidence", 0.0)}
                    for attr in attributes.get(AttributeType.LIMIT, [])
                    if attr.get("resolved_to")
                ]
            
            attributes_json = json.dumps(attr_components, indent=2)
        
        # Create prompt for LLM
        prompt = self.prompt_loader.format_prompt(
            "sql_generation",
            query=query,
            intent_type=intent.get("intent_type", "selection"),
            intent_description=intent.get("explanation", ""),
            entity_mapping=entity_mapping,
            schema=schema_json,
            relationships=relationships_json,
            attributes=attributes_json
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "explanation": {"type": "string"},
                "confidence": {"type": "number"},
                "approach": {"type": "string"}
            },
            "required": ["sql", "explanation", "confidence"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "sql" in response:
                return {
                    "sql": response["sql"],
                    "explanation": response.get("explanation", "Generated SQL based on natural language query"),
                    "confidence": response.get("confidence", 0.7),
                    "approach": response.get("approach", "LLM-based SQL generation")
                }
        except Exception as e:
            self.logger.error(f"Error generating SQL: {e}")
        
        # Fallback if generation fails
        return {
            "sql": f"SELECT * FROM {next(iter(schema_context['tables'].keys()))} LIMIT 10 -- Generation failed",
            "explanation": "Failed to generate SQL due to an error",
            "confidence": 0.1,
            "approach": "Fallback generation"
        }
    
    async def _validate_sql(self, sql: str, schema_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate generated SQL.
        
        Args:
            sql: Generated SQL
            schema_context: Schema context
            
        Returns:
            Validation result
        """
        if not sql:
            return {
                "is_valid": False,
                "confidence": 0.9,
                "issues": ["Empty SQL query"],
                "method": "basic_validation"
            }
        
        # Simple validation for SQL syntax - using LLM for validation
        prompt = self.prompt_loader.format_prompt(
            "sql_validation",
            sql=sql,
            schema=json.dumps(schema_context["tables"], indent=2)
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "is_valid": {"type": "boolean"},
                "confidence": {"type": "number"},
                "issues": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "suggestions": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["is_valid", "confidence"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "is_valid" in response:
                return {
                    "is_valid": response["is_valid"],
                    "confidence": response.get("confidence", 0.7),
                    "issues": response.get("issues", []),
                    "suggestions": response.get("suggestions", []),
                    "method": "llm_validation"
                }
        except Exception as e:
            self.logger.error(f"Error validating SQL: {e}")
        
        # Basic fallback validation if LLM validation fails
        basic_validation = self._basic_sql_validation(sql)
        return {
            "is_valid": basic_validation["is_valid"],
            "confidence": 0.6,  # Lower confidence for basic validation
            "issues": basic_validation["issues"],
            "method": "basic_validation"
        }
    
    def _basic_sql_validation(self, sql: str) -> Dict[str, Any]:
        """
        Perform basic SQL validation checks.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Basic validation result
        """
        issues = []
        
        # Check for required SQL components
        if "SELECT" not in sql.upper():
            issues.append("Missing SELECT statement")
        
        if "FROM" not in sql.upper():
            issues.append("Missing FROM clause")
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            issues.append("Unbalanced parentheses")
        
        # Check for common SQL injection patterns (basic check)
        sql_injection_patterns = ["--", ";", "DROP", "DELETE FROM", "TRUNCATE"]
        for pattern in sql_injection_patterns:
            if pattern in sql.upper() and pattern != "FROM":
                issues.append(f"Potential SQL injection pattern: {pattern}")
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues
        }
    
    async def _generate_simplified_sql(self, query: str, intent: Dict[str, Any], 
                                    entities: Dict[str, Dict[str, Any]],
                                    schema_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate simplified SQL for fallback.
        
        Args:
            query: Natural language query
            intent: Intent information
            entities: Resolved entities
            schema_context: Schema context
            
        Returns:
            Simplified SQL generation result
        """
        # Create simplified prompt for LLM
        prompt = self.prompt_loader.format_prompt(
            "simplified_sql_generation",
            query=query,
            intent_type=intent.get("intent_type", "selection"),
            entities=", ".join([f"{name} ({info['resolved_to']})" 
                              for name, info in entities.items() 
                              if "resolved_to" in info]),
            schema=json.dumps({
                name: {"columns": [col["name"] for col in info.get("columns", [])]}
                for name, info in schema_context["tables"].items()
            }, indent=2)
        )
        
        # Define schema for structured response
        schema = {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "explanation": {"type": "string"},
                "confidence": {"type": "number"}
            },
            "required": ["sql"]
        }
        
        try:
            # Generate structured response
            response = await self.llm_client.generate_structured(prompt, schema)
            
            if response and "sql" in response:
                return {
                    "sql": response["sql"],
                    "explanation": response.get("explanation", "Simplified SQL query"),
                    "confidence": response.get("confidence", 0.6),
                    "approach": "Simplified generation"
                }
        except Exception as e:
            self.logger.error(f"Error generating simplified SQL: {e}")
        
        # Extreme fallback - very basic SQL
        table_name = next(iter(schema_context["tables"].keys()))
        return {
            "sql": f"SELECT * FROM {table_name} LIMIT 10",
            "explanation": "Basic fallback query to show sample data",
            "confidence": 0.3,
            "approach": "Basic fallback"
        }
    
    async def _generate_alternatives(self, query: str, intent: Dict[str, Any], 
                                  entities: Dict[str, Dict[str, Any]],
                                  relationships: Optional[Dict[str, Any]],
                                  schema_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate alternative SQL interpretations.
        
        Args:
            query: Natural language query
            intent: Intent information
            entities: Resolved entities
            relationships: Relationship information
            schema_context: Schema context
            
        Returns:
            List of alternative SQL interpretations
        """
        # Try alternative approach - using different intent interpretation
        alternatives = []
        
        # If intent confidence is low, try alternative intent types
        if intent.get("confidence", 1.0) < 0.8:
            alternative_intents = [
                "selection" if intent.get("intent_type") != "selection" else "aggregation",
                "comparison" if intent.get("intent_type") not in ["selection", "comparison"] else "trend"
            ]
            
            for alt_intent in alternative_intents:
                # Create alternative prompt
                alt_prompt = self.prompt_loader.format_prompt(
                    "sql_generation_alternative",
                    query=query,
                    intent_type=alt_intent,
                    original_intent=intent.get("intent_type", "selection"),
                    entity_mapping=json.dumps({
                        entity_name: info["resolved_to"]
                        for entity_name, info in entities.items()
                        if "resolved_to" in info
                    }, indent=2),
                    schema=json.dumps({
                        name: {
                            "columns": [col["name"] for col in info.get("columns", [])]
                        }
                        for name, info in schema_context["tables"].items()
                    }, indent=2)
                )
                
                # Define schema for structured response
                alt_schema = {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "explanation": {"type": "string"},
                        "confidence": {"type": "number"},
                        "approach": {"type": "string"}
                    },
                    "required": ["sql", "explanation"]
                }
                
                try:
                    # Generate structured response
                    alt_response = await self.llm_client.generate_structured(alt_prompt, alt_schema)
                    
                    if alt_response and "sql" in alt_response:
                        alternatives.append({
                            "sql": alt_response["sql"],
                            "explanation": alt_response.get("explanation", f"Alternative interpretation with {alt_intent} intent"),
                            "confidence": alt_response.get("confidence", 0.5),
                            "approach": alt_response.get("approach", f"Alternative {alt_intent} intent")
                        })
                except Exception as e:
                    self.logger.error(f"Error generating alternative SQL: {e}")
        
        # If we need joins but confidence is low, try a simpler interpretation without some joins
        if (relationships and relationships.get("requires_joins", False) and 
            len(schema_context["tables"]) > 2):
            
            # Try with fewer tables
            main_tables = [
                next(iter(entities.values()))["resolved_to"],
                next(iter(reversed(list(entities.values()))))["resolved_to"]
            ]
            
            simplified_schema = {
                name: info for name, info in schema_context["tables"].items()
                if name in main_tables
            }
            
            alt_prompt = self.prompt_loader.format_prompt(
                "sql_generation_simplified",
                query=query,
                intent_type=intent.get("intent_type", "selection"),
                tables_included=", ".join(main_tables),
                schema=json.dumps(simplified_schema, indent=2)
            )
            
            # Define schema for structured response
            alt_schema = {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "explanation": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["sql", "explanation"]
            }
            
            try:
                # Generate structured response
                alt_response = await self.llm_client.generate_structured(alt_prompt, alt_schema)
                
                if alt_response and "sql" in alt_response:
                    alternatives.append({
                        "sql": alt_response["sql"],
                        "explanation": alt_response.get("explanation", "Simplified query with fewer joins"),
                        "confidence": alt_response.get("confidence", 0.5),
                        "approach": "Simplified joins"
                    })
            except Exception as e:
                self.logger.error(f"Error generating simplified SQL: {e}")
                
        return alternatives
# The Agentic Query Journey: Inside the Text2SQL System

## Executive Summary

This document provides a detailed walkthrough of how a natural language query travels through our transparent, agentic Text2SQL architecture. It explains the step-by-step journey from user input to SQL output, highlighting the specialized roles of each agent, the reasoning process, and how knowledge boundaries are handled explicitly.

Our multi-agent architecture breaks down the complex Text2SQL process into specialized components that work together in a pipeline, each contributing its expertise while maintaining full transparency of the reasoning process. This approach significantly improves performance on complex queries and edge cases while providing unprecedented visibility into the system's decision-making.

---

## 1. The Agentic Architecture: Birds-Eye View

![Architecture Overview](architecture_overview_diagram.png)

Our Text2SQL system is built on a multi-agent architecture with five specialized agents:

1. **IntentAgent**: Determines the query's primary purpose (selection, aggregation, comparison, etc.)
2. **EntityAgent**: Identifies and resolves database entities mentioned in the query
3. **RelationshipAgent**: Discovers optimal join paths between entities
4. **AttributeAgent**: Identifies and resolves query attributes like filters, aggregations, and groupings
5. **SQLAgent**: Generates the final SQL query with proper syntax and structure

These agents work within a framework of shared components:

- **ReasoningStream**: Captures the complete chain of thought during processing
- **Knowledge Boundaries**: Explicitly identifies and handles system limitations
- **Configuration System**: Allows customization of agent behavior
- **Strategy Registry**: Enables pluggable resolution strategies

The system processes queries through a pipeline, with each agent building on the results of previous agents while contributing to a shared, transparent reasoning stream.

---

## 2. Detailed Query Journey: Following the Path

To illustrate how the system works, let's follow a sample query through the entire processing journey:

> "Show me the top 5 customers with the most expensive orders in Q2 2023"

### 2.1 User Query Submission

The journey begins when the user submits this natural language query. The TransparentQueryEngine initializes a `ReasoningStream` to track the entire reasoning process and creates a `BoundaryRegistry` to capture any knowledge limitations.

```python
# TransparentQueryEngine initialization
reasoning_stream = ReasoningStream(
    query_id=str(uuid.uuid4()),
    query_text="Show me the top 5 customers with the most expensive orders in Q2 2023"
)

processing_context = {
    "query": "Show me the top 5 customers with the most expensive orders in Q2 2023",
    "tenant_id": tenant_id,
    "context": {},
    "boundary_registry": BoundaryRegistry()
}
```

### 2.2 Intent Analysis Stage

The first agent engaged is the **IntentAgent**, which must determine the primary purpose of the query.

#### Stage Initialization

```
📋 STAGE: Intent Analysis
📝 DESCRIPTION: Determining the primary purpose of the query
```

#### Step 1: Pattern Analysis

The IntentAgent first analyzes query patterns looking for intent signals:

```
🔍 STEP: Analyzing query structure and patterns
🧩 EVIDENCE:
  - pattern_matches:
    - selection: 2 matches (show, top)
    - aggregation: 0 matches
    - comparison: 0 matches
    - trend: 0 matches
  - most_likely_type: selection
  - match_strength: 2
  - confidence: 0.73
```

#### Step 2: LLM Intent Classification

The agent then uses a language model for more sophisticated intent analysis:

```
🔍 STEP: Classifying intent using language model
🧩 EVIDENCE:
  - intent_type: selection
  - subtype: ranked_list
  - confidence: 0.92
  - explanation: "This query is asking to select and display specific customer data, 
                 with ordering (top 5) and filtering (Q2 2023)."
```

#### Stage Conclusion

After processing, the IntentAgent concludes its reasoning:

```
✓ CONCLUSION: Query intent determined to be selection with 0.92 confidence
```

The agent updates the processing context with this result:

```python
processing_context["intent"] = {
    "intent_type": "selection",
    "subtype": "ranked_list",
    "confidence": 0.92,
    "explanation": "This query is asking to select and display specific customer data,
                   with ordering (top 5) and filtering (Q2 2023)."
}
```

### 2.3 Entity Recognition Stage

With intent established, the **EntityAgent** now identifies entities mentioned in the query.

#### Stage Initialization

```
📋 STAGE: Entity Recognition
📝 DESCRIPTION: Identifying and resolving database entities in the query
```

#### Step 1: Entity Extraction

The EntityAgent extracts potential entity mentions using various methods:

```
🔍 STEP: Extracting potential entity mentions from query
🧩 EVIDENCE:
  - extraction_methods: ["capitalization", "noun_phrases"]
  - potential_entities: ["Q2", "customers", "orders"]
  - details:
    - capitalization:
      - candidates: ["Q2"]
    - noun_phrases:
      - candidates: ["customers", "orders"]
  - confidence: 0.78
```

#### Step 2: Intent-Based Filtering

The agent refines entity candidates based on the previously determined intent:

```
🔍 STEP: Filtering candidates based on selection intent
🧩 EVIDENCE:
  - intent_type: selection
  - before_filtering: 3
  - after_filtering: 3
  - filtered_entities: ["Q2", "customers", "orders"]
  - confidence: 0.85
```

#### Step 3: Entity Resolution

The agent then resolves these entity mentions to database tables using multiple strategies:

```
🔍 STEP: Resolving entities using multiple resolution strategies
🧩 EVIDENCE:
  - strategies_used: ["DirectTableMatchStrategy", "GlossaryTermMatchStrategy", 
                     "SemanticConceptMatchStrategy", "LLMBasedResolutionStrategy"]
  - entities_resolved:
    - customers:
      - resolved_to: "customer"
      - confidence: 0.95
      - resolution_type: "table"
      - strategy: "DirectTableMatchStrategy"
    - orders:
      - resolved_to: "orders"
      - confidence: 0.92
      - resolution_type: "table"
      - strategy: "DirectTableMatchStrategy"
    - Q2:
      - resolved_to: null
      - confidence: 0.0
      - resolution_type: null
      - strategy: "none"
  - confidence: 0.95
```

#### Step 4: Knowledge Boundary Creation

The agent identifies that "Q2" is a temporal reference, not an entity, and creates a knowledge boundary:

```
🔍 STEP: Identifying unknown or unmappable entities
🧩 EVIDENCE:
  - unknown_entities: ["Q2"]
  - boundary_count: 1
  - requires_clarification: false
  - confidence: 0.90
```

```
⚠️ KNOWLEDGE BOUNDARY:
  - type: UNMAPPABLE_CONCEPT
  - component: entity_Q2
  - explanation: "Q2 is a time period reference (Q2 2023), not a database entity"
  - confidence: 0.85
  - suggestions: ["Handle Q2 as a filter condition, not an entity"]
```

#### Stage Conclusion

The EntityAgent concludes its reasoning:

```
✓ CONCLUSION: Identified 2 entities with average confidence 0.94 and 1 unmappable concept
```

The agent updates the processing context with the resolved entities:

```python
processing_context["entities"] = {
    "customers": {
        "resolved_to": "customer",
        "confidence": 0.95,
        "resolution_type": "table",
        "strategy": "DirectTableMatchStrategy",
        "metadata": {...}
    },
    "orders": {
        "resolved_to": "orders",
        "confidence": 0.92,
        "resolution_type": "table",
        "strategy": "DirectTableMatchStrategy",
        "metadata": {...}
    }
}
```

### 2.4 Relationship Discovery Stage

With entities identified, the **RelationshipAgent** now discovers how they relate in the database.

#### Stage Initialization

```
📋 STAGE: Relationship Discovery
📝 DESCRIPTION: Discovering relationships between entities for join path determination
```

#### Step 1: Relationship Hint Analysis

The agent first analyzes the query for relationship hints:

```
🔍 STEP: Analyzing query for relationship hints
🧩 EVIDENCE:
  - entity_count: 2
  - relationship_hints:
    - relationship_types:
      - customers_to_orders:
        - type: "ownership"
        - confidence: 0.75
        - entities: ["customers", "orders"]
    - methods: ["keyword_based"]
    - confidence: 0.75
```

#### Step 2: Join Path Discovery

The agent then discovers join paths between the entities using multiple strategies:

```
🔍 STEP: Discovering join paths between entities
🧩 EVIDENCE:
  - join_paths_found:
    - customers_to_orders:
      - source: "customers"
      - target: "orders"
      - confidence: 0.92
      - strategy: "DirectForeignKeyStrategy"
  - total_pairs: 1
  - total_paths_found: 1
  - confidence: 0.92
```

#### Join Path Details

The detailed join path discovered:

```
🔄 JOIN PATH: customers_to_orders
  - path:
    - from_table: "customer"
    - from_column: "id"
    - to_table: "orders"
    - to_column: "customer_id"
  - confidence: 0.92
  - strategy: "DirectForeignKeyStrategy"
```

#### Stage Conclusion

The RelationshipAgent concludes its reasoning:

```
✓ CONCLUSION: Discovered 1 join path between 2 entities
```

The agent updates the processing context with the relationship information:

```python
processing_context["relationships"] = {
    "relationships": {
        "customers_to_orders": {
            "source_entity": "customers",
            "target_entity": "orders",
            "source_table": "customer",
            "target_table": "orders",
            "join_path": {
                "path": [{
                    "from_table": "customer",
                    "from_column": "id",
                    "to_table": "orders",
                    "to_column": "customer_id"
                }],
                "confidence": 0.92
            },
            "confidence": 0.92,
            "strategy": "DirectForeignKeyStrategy"
        }
    },
    "requires_joins": true,
    "join_tree": {...}
}
```

### 2.5 Attribute Processing Stage

With entities and their relationships established, the **AttributeAgent** now identifies and resolves query attributes like filters, aggregations, and groupings.

#### Stage Initialization

```
📋 STAGE: Attribute Processing
📝 DESCRIPTION: Identifying and resolving query attributes like filters, aggregations, and groupings
```

#### Step 1: Attribute Extraction

The AttributeAgent extracts potential attributes using multiple methods:

```
🔍 STEP: Extracting potential query attributes
🧩 EVIDENCE:
  - extraction_methods: ["keyword_based", "nlp_based"]
  - filter_count: 2
  - aggregation_count: 1
  - grouping_count: 0
  - sorting_count: 1
  - limit_count: 1
  - details:
    - keyword_based:
      - filters: ["in Q2 2023", "most expensive"]
      - sortings: ["top 5"]
      - limits: ["top 5"]
    - nlp_based:
      - filters: ["in Q2 2023", "most expensive"]
      - aggregations: ["most expensive"]
      - sortings: ["top 5"]
      - limits: ["top 5"]
  - confidence: 0.82
```

#### Step 2: Attribute Resolution

The agent resolves these attributes to specific SQL components using multiple strategies:

```
🔍 STEP: Resolving attributes to SQL components
🧩 EVIDENCE:
  - strategies_used: ["ColumnBasedResolutionStrategy", "SemanticConceptResolutionStrategy", "LLMBasedResolutionStrategy"]
  - attributes_resolved:
    - filters: 2
    - aggregations: 1
    - groupings: 0
    - sortings: 1
    - limits: 1
  - resolution_details:
    - filter_in_Q2_2023:
      - strategy_used: "ColumnBasedResolutionStrategy"
      - resolved_to: "orders.order_date BETWEEN '2023-04-01' AND '2023-06-30'"
      - confidence: 0.88
    - filter_most_expensive:
      - strategy_used: "SemanticConceptResolutionStrategy"
      - resolved_to: "orders.total_amount"
      - confidence: 0.75
    - aggregation_most_expensive:
      - strategy_used: "LLMBasedResolutionStrategy"
      - resolved_to: "SUM(orders.total_amount)"
      - confidence: 0.82
    - sorting_top_5:
      - strategy_used: "ColumnBasedResolutionStrategy"
      - resolved_to: "order_total DESC"
      - confidence: 0.85
    - limit_top_5:
      - strategy_used: "ColumnBasedResolutionStrategy"
      - resolved_to: "LIMIT 5"
      - confidence: 0.95
  - confidence: 0.85
```

#### Stage Conclusion

The AttributeAgent concludes its reasoning:

```
✓ CONCLUSION: Processed 5 query attributes
```

The agent updates the processing context with the attribute information:

```python
processing_context["attributes"] = {
    "filters": [
        {
            "attribute_type": "filter",
            "attribute_value": "in Q2 2023",
            "resolved_to": "orders.order_date BETWEEN '2023-04-01' AND '2023-06-30'",
            "confidence": 0.88,
            "strategy": "ColumnBasedResolutionStrategy",
            "metadata": {...}
        },
        {
            "attribute_type": "filter",
            "attribute_value": "most expensive",
            "resolved_to": "orders.total_amount",
            "confidence": 0.75,
            "strategy": "SemanticConceptResolutionStrategy",
            "metadata": {...}
        }
    ],
    "aggregations": [
        {
            "attribute_type": "aggregation",
            "attribute_value": "most expensive",
            "resolved_to": "SUM(orders.total_amount)",
            "confidence": 0.82,
            "strategy": "LLMBasedResolutionStrategy",
            "metadata": {...}
        }
    ],
    "sortings": [
        {
            "attribute_type": "sorting",
            "attribute_value": "top 5",
            "resolved_to": "order_total DESC",
            "confidence": 0.85,
            "strategy": "ColumnBasedResolutionStrategy",
            "metadata": {...}
        }
    ],
    "limits": [
        {
            "attribute_type": "limit",
            "attribute_value": "top 5",
            "resolved_to": "LIMIT 5",
            "confidence": 0.95,
            "strategy": "ColumnBasedResolutionStrategy",
            "metadata": {...}
        }
    ],
    "confidence": 0.85
}
```

### 2.6 SQL Generation Stage

With intent, entities, relationships, and attributes established, the **SQLAgent** can now generate the final SQL query.

#### Stage Initialization

```
📋 STAGE: SQL Generation
📝 DESCRIPTION: Generating SQL query from intent, entities, and relationships
```

#### Step 1: Schema Context Preparation

The agent first prepares the schema context for SQL generation:

```
🔍 STEP: Preparing schema context for SQL generation
🧩 EVIDENCE:
  - tables_included: ["customer", "orders"]
  - has_joins: true
  - confidence: 0.90
```

#### Step 2: SQL Generation

The agent then generates the SQL query using LLM with comprehensive context:

```
🔍 STEP: Generating SQL query using language model
🧩 EVIDENCE:
  - intent_type: "selection"
  - entity_count: 2
  - needs_joins: true
  - sql_approach: "Join-based selection with aggregation and ranking"
  - confidence: 0.88
```

#### Step 3: SQL Validation

The agent validates the generated SQL against the schema:

```
🔍 STEP: Validating generated SQL
🧩 EVIDENCE:
  - validation_method: "llm_validation"
  - is_valid: true
  - issues: []
  - confidence: 0.93
```

#### Generated SQL

The final SQL query generated:

```sql
SELECT 
    c.name AS customer_name,
    SUM(o.total_amount) AS order_total
FROM 
    customer c
JOIN 
    orders o ON c.id = o.customer_id
WHERE 
    o.order_date BETWEEN '2023-04-01' AND '2023-06-30'  -- Q2 2023
GROUP BY 
    c.name
ORDER BY 
    order_total DESC
LIMIT 5
```

#### Stage Conclusion

The SQLAgent concludes its reasoning:

```
✓ CONCLUSION: Generated SQL based on intent and entities with necessary joins
```

The agent updates the processing context with the SQL result:

```python
processing_context["sql_result"] = {
    "sql": "SELECT c.name AS customer_name, SUM(o.total_amount) AS order_total FROM customer c...",
    "explanation": "This query joins customers and orders, filters to Q2 2023...",
    "confidence": 0.88,
    "approach": "Join-based selection with aggregation and ranking",
    "is_valid": true
}
```

### 2.6 Response Assembly

Finally, the TransparentQueryEngine assembles all the information into a comprehensive response:

```python
response = Text2SQLResponse(
    original_query="Show me the top 5 customers with the most expensive orders in Q2 2023",
    interpreted_as="Query with intent: selection (ranked_list)",
    ambiguity_level=0.12,  # 1.0 - 0.88
    sql_results=[
        SQLResult(
            sql="SELECT c.name AS customer_name, SUM(o.total_amount) AS order_total...",
            explanation="This query joins customers and orders, filters to Q2 2023...",
            approach="Join-based selection with aggregation and ranking",
            is_primary=True
        )
    ],
    primary_interpretation=primary_sql,
    multiple_interpretations=False,
    entities_resolved={
        "customers": "customer",
        "orders": "orders"
    },
    metadata={
        "reasoning_stream": reasoning_stream.dict(),
        "intent": intent_result,
        "entities": entities_result,
        "relationships": relationships_result,
        "attributes": attributes_result,
        "sql_generation": sql_result,
        "knowledge_boundaries": boundary_registry.to_dict(),
        "requires_clarification": False,
        "transparent_engine": True,
        "agent_based": True
    }
)
```

---

## 3. Agent Deep-Dive: Understanding the Specialized Roles

### 3.1 IntentAgent: The Query Purpose Analyzer

The IntentAgent classifies the query's primary intent, essential for guiding subsequent processing. It implements multiple analysis techniques:

#### Pattern-Based Analysis

The agent first analyzes lexical patterns in the query using regular expressions to identify intent signals:

```python
patterns = {
    "selection": ["show", "list", "get", "find", "display"],
    "aggregation": ["how many", "count", "sum", "total", "average"],
    "comparison": ["compare", "difference", "contrast", "versus"],
    "trend": ["trend", "change", "over time", "growth"]
}
```

#### LLM-Based Classification

The agent then uses a language model for more sophisticated intent analysis:

```
PROMPT: [intent_classification]
You are classifying the intent of a natural language query.

Query: "Show me the top 5 customers with the most expensive orders in Q2 2023"

Please analyze this query and determine its primary intent. Consider the following intent types:
- selection: Retrieving specific records or information
- aggregation: Calculating counts, sums, averages, etc.
- comparison: Comparing different groups or categories
- trend: Analyzing changes over time
...
```

#### Multiple Intent Detection

For complex queries, the agent checks if multiple intents are present:

```python
def _might_have_multiple_intents(self, query: str) -> bool:
    """Check if query might have multiple intents based on heuristics."""
    # Check for conjunction indicators
    conjunction_patterns = [
        r"(and|also|additionally)",
        r"(as well as)",
        r"(;|,\s*and)",
        r"(\.|\?)\s*[A-Z]"  # Multiple sentences
    ]
    
    # Check if any patterns match
    for pattern in conjunction_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            return True
    
    return False
```

#### Alternative Generation

For ambiguous queries, the agent generates alternative interpretations:

```python
async def _generate_alternatives(self, query: str, primary_intent: str) -> List[Dict[str, Any]]:
    """Generate alternative intent interpretations."""
    prompt = self.prompt_loader.format_prompt(
        "intent_alternatives", 
        query=query,
        primary_intent=primary_intent
    )
    
    # Generate structured response using LLM
    result = await self.llm_client.generate_structured(prompt, schema)
    return result.get("alternatives", [])
```

### 3.2 EntityAgent: The Entity Resolution Specialist

The EntityAgent identifies and resolves entities mentioned in the query to database tables and concepts using multiple strategies.

#### Entity Extraction Methods

The agent extracts potential entities using multiple extraction methods:

1. **Capitalization-Based**: Extracts words that begin with capital letters
2. **Keyword-Based**: Extracts terms following SQL-like keywords
3. **Noun Phrase-Based**: Extracts common noun phrases typically used as entities

#### Resolution Strategies

The EntityAgent employs multiple strategies for resolving entities to database tables:

1. **DirectTableMatchStrategy**: Matches entity names directly to table names with high confidence
2. **GlossaryTermMatchStrategy**: Resolves entities via business glossary terms that map to tables
3. **SemanticConceptMatchStrategy**: Uses semantic concepts in the knowledge graph to resolve entities
4. **LLMBasedResolutionStrategy**: Uses a language model when other strategies fail

Each strategy independently attempts to resolve entities, and the agent selects the resolution with highest confidence:

```python
async def _resolve_entities(self, candidates: List[str], context: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve entity candidates using all available strategies."""
    resolved_entities = {}
    
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
    
    return {
        "entities": resolved_entities,
        "confidence": max([e["confidence"] for e in resolved_entities.values()]) if resolved_entities else 0.0
    }
```

#### Knowledge Boundary Creation

The EntityAgent explicitly identifies entities it cannot resolve:

```python
# Handle unknown entities as knowledge boundaries
unknown_entities = [
    entity for entity in filtered_candidates 
    if entity not in resolved_entities["entities"] or 
       resolved_entities["entities"][entity]["confidence"] < 0.4
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
                "Try using a more specific business term"
            ]
        )
        boundary_registry.add_boundary(boundary)
```

### 3.3 RelationshipAgent: The Join Path Discoverer

The RelationshipAgent discovers how entities are related in the database schema and determines optimal join paths.

#### Relationship Hint Extraction

The agent first extracts relationship hints from the query:

```python
# Look for common relationship patterns
patterns = {
    "joins": ["join", "with", "and", "between", "related to"],
    "ownership": ["has", "have", "owns", "own", "belonging to"],
    "membership": ["in", "contains", "member of", "part of"]
}

# Check for each entity pair
for i, entity1 in enumerate(entities):
    for entity2 in entities[i+1:]:
        # Check proximity of entities in query
        if entity1.lower() in query_lower and entity2.lower() in query_lower:
            words_between = self._count_words_between(query_lower, entity1.lower(), entity2.lower())
            proximity_score = max(0, 1.0 - (words_between / 10))
            
            # Check for relationship patterns
            for rel_pattern, keywords in patterns.items():
                for keyword in keywords:
                    if keyword in query_lower:
                        # Check if keyword is between entities
                        if self._is_keyword_between(query_lower, entity1.lower(), entity2.lower(), keyword):
                            rel_type = rel_pattern
                            # Higher confidence for explicit relationship keywords between entities
                            rel_confidence = 0.7 + (proximity_score * 0.2)
```

#### Join Path Strategies

The RelationshipAgent employs multiple strategies for discovering join paths:

1. **DirectForeignKeyStrategy**: Uses direct foreign key relationships defined in the schema
2. **CommonColumnStrategy**: Infers joins based on common column naming patterns
3. **ConceptBasedJoinStrategy**: Uses semantic concepts to discover complex join paths
4. **LLMBasedJoinStrategy**: Uses a language model for complex cases

Each strategy independently attempts to find join paths, and the agent selects the path with highest confidence:

```python
async def _discover_join_path(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Discover join paths between two tables using all available strategies."""
    best_result = None
    best_confidence = 0.0
    
    # Try each join strategy
    for strategy in self.join_strategies:
        result = await strategy.resolve(source_table, target_table, context)
        
        # If strategy produced a result with confidence
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
            "strategy": "none"
        }
    
    return best_result
```

#### Join Tree Optimization

For queries involving multiple tables, the agent determines an optimal join tree:

```python
def _determine_optimal_join_tree(self, entities: Dict[str, Dict[str, Any]], join_paths: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Determine optimal join tree for multiple entities."""
    # Strategy selection from configuration
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
                # Check if this path connects an entity in the tree to one outside
                if (path["source_entity"] in entities_in_tree and 
                    path["target_entity"] not in entities_in_tree):
                    if path["confidence"] > best_confidence:
                        best_extension = path
                        best_confidence = path["confidence"]
            
            # If we found a valid extension, add it to the tree
            if best_extension:
                join_tree["joins"].append({
                    "from_entity": best_extension["source_entity"],
                    "to_entity": best_extension["target_entity"],
                    "join_path": best_extension["join_path"],
                    "confidence": best_extension["confidence"]
                })
                entities_in_tree.add(best_extension["target_entity"])
                remaining_entities.remove(best_extension["target_entity"])
```

### 3.4 AttributeAgent: The Attribute Resolution Specialist

The AttributeAgent identifies and resolves query attributes like filters, aggregations, groupings, sorting conditions, and limits.

#### Attribute Types

The agent classifies attributes into five main types:

```python
class AttributeType:
    """Enumeration of attribute types."""
    FILTER = "filter"        # WHERE clause components
    AGGREGATION = "aggregation"  # Aggregate functions (SUM, COUNT, etc.)
    GROUPING = "grouping"    # GROUP BY clause components
    SORTING = "sorting"      # ORDER BY clause components
    LIMIT = "limit"          # LIMIT clause components
```

#### Extraction Methods

The AttributeAgent uses multiple extraction methods:

1. **Keyword-Based**: Extracts attributes using common keywords and patterns
2. **NLP-Based**: Uses natural language processing to identify attribute patterns
3. **LLM-Based**: Uses language models for more complex attribute identification

```python
def _extract_filters(self, query: str, entity_context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract filter attributes from query."""
    filters = []
    
    # Pattern for value comparisons (greater than, less than, equal to, etc.)
    comparison_patterns = [
        (r"(greater|more|higher|larger|bigger) than (\d+)", "greater_than", r"\2"),
        (r"(less|lower|smaller|fewer) than (\d+)", "less_than", r"\2"),
        (r"equal to (\d+)", "equal_to", r"\1"),
        # more patterns...
    ]
    
    for pattern, operator, value_pattern in comparison_patterns:
        for match in re.finditer(pattern, query):
            # Extract the value from the match
            value = re.sub(pattern, value_pattern, match.group(0))
            
            # Determine which column this applies to
            query_before = query[:match.start()].lower()
            words_before = query_before.split()[-5:] if len(query_before.split()) > 5 else query_before.split()
            column_hint = " ".join(words_before)
            
            filters.append({
                "attribute_value": match.group(0),
                "operator": operator,
                "value": {"value": value},
                "column_hint": column_hint,
                "confidence": 0.8
            })
    
    # More extraction logic...
    return filters
```

#### Resolution Strategies

The AttributeAgent uses multiple strategies to map attributes to SQL components:

1. **ColumnBasedResolutionStrategy**: Maps attributes to columns based on name matching
2. **SemanticConceptResolutionStrategy**: Uses semantic concepts from the knowledge graph
3. **LLMBasedResolutionStrategy**: Uses language models for complex mapping

```python
async def _resolve_attributes(self, attributes: Dict[str, List[Dict[str, Any]]], context: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve attribute candidates using all available strategies."""
    resolved_attributes = {
        AttributeType.FILTER: [],
        AttributeType.AGGREGATION: [],
        AttributeType.GROUPING: [],
        AttributeType.SORTING: [],
        AttributeType.LIMIT: [],
        "details": {}
    }
    
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
    
    return resolved_attributes
```

### 3.5 SQLAgent: The Query Generator

The SQLAgent synthesizes the final SQL query based on the intent, entities, relationships, and attributes identified by previous agents.

#### Schema Context Preparation

The agent first prepares a comprehensive schema context:

```python
def _prepare_schema_context(self, tenant_id: str, entities: Dict[str, Dict[str, Any]], relationships: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare schema context for SQL generation."""
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
        "glossary_terms": {term: info for term, info in full_context.get("glossary_terms", {}).items()
                          if any(table in entity_tables for table in info.get("mapped_tables", []))}
    }
```

#### LLM-Based SQL Generation

The agent then uses a language model to generate the SQL query:

```
PROMPT: [sql_generation]
You are a SQL expert that translates natural language questions into SQL queries.

Query: "Show me the top 5 customers with the most expensive orders in Q2 2023"

Intent: selection
Intent Description: This query is asking to select and display specific customer data, with ordering (top 5) and filtering (Q2 2023).

Entities mentioned in the query have been mapped to tables as follows:
{
  "customers": "customer",
  "orders": "orders"
}

Database Schema:
{
  "tables": {
    "customer": {
      "description": "Customer information",
      "columns": [
        {"name": "id", "data_type": "integer", "description": "Primary key"},
        {"name": "name", "data_type": "string", "description": "Customer name"},
        {"name": "email", "data_type": "string", "description": "Customer email address"}
      ]
    },
    "orders": {
      "description": "Customer orders",
      "columns": [
        {"name": "id", "data_type": "integer", "description": "Primary key"},
        {"name": "customer_id", "data_type": "integer", "description": "Foreign key to customer"},
        {"name": "order_date", "data_type": "date", "description": "Date of order"},
        {"name": "total_amount", "data_type": "decimal", "description": "Total order amount"}
      ]
    }
  }
}

Relationships between tables:
[
  {
    "source_table": "customer",
    "target_table": "orders",
    "join_path": [
      {
        "from_table": "customer",
        "from_column": "id",
        "to_table": "orders",
        "to_column": "customer_id"
      }
    ]
  }
]

Your task is to generate a SQL query that correctly answers the user's question.
...
```

#### SQL Validation

The agent validates the generated SQL to ensure it's syntactically correct and matches the schema:

```python
async def _validate_sql(self, sql: str, schema_context: Dict[str, Any]) -> Dict[str, Any]:
    """Validate generated SQL."""
    # Create validation prompt
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
            "issues": {"type": "array", "items": {"type": "string"}},
            "suggestions": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["is_valid", "confidence"]
    }
    
    # Generate structured response
    response = await self.llm_client.generate_structured(prompt, schema)
    
    return {
        "is_valid": response["is_valid"],
        "confidence": response.get("confidence", 0.7),
        "issues": response.get("issues", []),
        "suggestions": response.get("suggestions", []),
        "method": "llm_validation"
    }
```

#### Alternative Generation

For uncertain cases, the agent generates alternative SQL interpretations:

```python
async def _generate_alternatives(self, query: str, intent: Dict[str, Any], entities: Dict[str, Dict[str, Any]], relationships: Dict[str, Any], schema_context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate alternative SQL interpretations."""
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
                    name: {"columns": [col["name"] for col in info.get("columns", [])]}
                    for name, info in schema_context["tables"].items()
                }, indent=2)
            )
            
            # Generate structured response
            alt_response = await self.llm_client.generate_structured(alt_prompt, schema)
            
            if alt_response and "sql" in alt_response:
                alternatives.append({
                    "sql": alt_response["sql"],
                    "explanation": alt_response.get("explanation", f"Alternative interpretation with {alt_intent} intent"),
                    "confidence": alt_response.get("confidence", 0.5),
                    "approach": alt_response.get("approach", f"Alternative {alt_intent} intent")
                })
    
    return alternatives
```

---

## 4. Key Architecture Benefits

### 4.1 Full Transparency

The agentic architecture provides full transparency into every step of the query processing:

- **Detailed Reasoning**: Every decision is captured with evidence and confidence
- **Step-by-Step Analysis**: The full chain of thought is preserved and exposed
- **Uncertainty Quantification**: Confidence scores reflect system uncertainty

### 4.2 Explicit Knowledge Boundaries

The system explicitly identifies what it doesn't know:

- **Unmappable Entities**: Terms that can't be mapped to database objects
- **Missing Relationships**: Tables that can't be joined reliably
- **Ambiguous Intents**: Queries with multiple possible interpretations
- **Invalid SQL**: SQL that doesn't validate against the schema

For each knowledge boundary, the system provides:
- Clear explanation of the limitation
- Confidence assessment
- Suggestions for resolution

### 4.3 Modular, Extensible Design

The architecture is designed for easy extension:

- **Strategy Pattern**: New resolution strategies can be added without changing agent code
- **Registration System**: Strategies register themselves via decorators
- **Configuration-Driven**: Agent behavior can be customized via configuration
- **Loose Coupling**: Agents communicate through a well-defined context object

### 4.4 Improved Performance on Edge Cases

The multi-strategy approach significantly improves performance on edge cases:

- **Multiple Approaches**: Different strategies excel in different scenarios
- **Graceful Degradation**: If one strategy fails, others can succeed
- **LLM Fallbacks**: Language models provide reasonable results when pattern-based approaches fail
- **Alternative Generation**: The system provides alternatives for ambiguous cases

---

## 5. Technical Implementation Details

### 5.1 High-Performance Strategy Execution

To optimize performance, the system:

- Uses parallel execution of strategies when possible
- Employs caching for expensive operations (schema retrieval, etc.)
- Prioritizes faster strategies first

### 5.2 Advanced Configuration

The system supports fine-grained configuration:

```json
{
  "strategies": [
    {
      "type": "direct_table_match",
      "enabled": true,
      "priority": 1
    },
    {
      "type": "glossary_term_match",
      "enabled": true,
      "priority": 2
    }
  ],
  "extractors": [
    {
      "type": "capitalization",
      "enabled": true
    }
  ]
}
```

### 5.3 Dynamic Strategy Registration

Strategies are registered dynamically using Python decorators:

```python
@StrategyRegistry.register("direct_table_match")
class DirectTableMatchStrategy(EntityResolutionStrategy):
    # Strategy implementation
```

### 5.4 Central Knowledge Registry

All knowledge boundaries are tracked in a central registry:

```python
boundary_registry = BoundaryRegistry()

# Adding a boundary
boundary = KnowledgeBoundary(
    boundary_type=BoundaryType.UNKNOWN_ENTITY,
    component="entity_Q2",
    confidence=0.2,
    explanation="Could not map 'Q2' to any database entity",
    suggestions=["Q2 appears to be a time period reference"]
)
boundary_registry.add_boundary(boundary)

# Getting boundaries by type
unknown_entities = boundary_registry.get_boundaries_by_type(BoundaryType.UNKNOWN_ENTITY)
```

---

## 6. Future Directions and Extensions

### 6.1 Additional Specialized Agents

The architecture can be extended with additional specialized agents:

- **ExplanationAgent**: For generating natural language explanations of SQL
- **FeedbackAgent**: For learning from user feedback
- **OptimizationAgent**: For optimizing SQL query performance

### 6.2 Advanced SQL Generation

Future enhancements to SQL generation include:

- **Query Optimization**: Generate optimized SQL for complex queries
- **Advanced Joins**: Support for more complex join types and conditions
- **Temporal Logic**: Better handling of time-based filtering and analysis

### 6.3 Learning and Adaptation

The system can be enhanced with learning capabilities:

- **User Feedback Incorporation**: Learn from user corrections
- **Entity Resolution Learning**: Improve resolution based on historical patterns
- **Strategy Performance Tracking**: Monitor and adjust strategy priorities based on success rates

---

## 7. Conclusion

The transparent, agentic architecture of our Text2SQL system represents a significant advancement in natural language to SQL translation. By breaking down the complex process into specialized agents with transparent reasoning, we've created a system that:

1. **Provides unprecedented visibility** into the query translation process
2. **Explicitly communicates limitations** when they occur
3. **Offers improved performance** through specialized, modular components
4. **Supports easy extension** with new strategies and agents

This architecture enables both high performance and high transparency, making the system more reliable, explainable, and trustworthy for end users.

---

## Appendices

### Appendix A: Knowledge Boundary Types

| Boundary Type | Description | Example |
|---------------|-------------|---------|
| UNKNOWN_ENTITY | Terms that can't be mapped to database objects | "Sales region" when no region table exists |
| UNMAPPABLE_CONCEPT | Concepts that aren't directly representable | "Q2 2023" as a time reference |
| AMBIGUOUS_INTENT | Queries with multiple possible intents | "Show sales and calculate average" |
| MISSING_RELATIONSHIP | Tables that can't be joined | Products can't be joined to employees |
| UNCERTAIN_ATTRIBUTE | Attributes with uncertain mapping | "Size" could mean physical size or storage size |
| COMPLEX_IMPLEMENTATION | SQL that requires advanced features | Window functions not supported |

### Appendix B: Reasoning Stream Structure

The reasoning stream is structured as follows:

```python
ReasoningStream:
    query_id: str
    query_text: str
    stages: List[ReasoningStage]
    complete: bool
    
ReasoningStage:
    name: str  # e.g., "Intent Analysis"
    description: str
    steps: List[ReasoningStep]
    conclusion: str
    final_output: Any
    alternatives: List[Alternative]
    completed: bool
    
ReasoningStep:
    description: str
    evidence: Dict[str, Any]
    confidence: float
    timestamp: str
    
Alternative:
    description: str
    confidence: float
    reason: str
```

### Appendix C: Agent Registry Structure

The strategy registry is structured as follows:

```python
StrategyRegistry:
    _strategies: Dict[str, Type]  # Maps strategy names to strategy classes
    
    @classmethod
    def register(cls, strategy_type: str) -> Callable:
        """Register a strategy class with the given type."""
        
    @classmethod
    def get_strategy(cls, strategy_type: str) -> Type:
        """Get a strategy class by type."""
```
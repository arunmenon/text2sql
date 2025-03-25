# Relationship Agent Refactoring

This document describes the refactored Relationship Agent architecture, which follows software engineering best practices for modularity, extensibility, and maintainability.

## Architecture Overview

The refactored RelationshipAgent uses a plugin-based architecture with strategy and registry patterns to allow for easy extension and modification:

```
src/text2sql/reasoning/
  ├── agents/
  │   └── relationship/               # Modular relationship agent package
  │       ├── __init__.py             # Clean public API
  │       ├── relationship_agent.py   # Main coordinator
  │       ├── base.py                 # Base interfaces
  │       ├── extractors/             # Relationship hint extraction methods
  │       │   ├── __init__.py
  │       │   └── keyword_based.py
  │       └── strategies/             # Join path strategies 
  │           ├── __init__.py
  │           ├── direct_foreign_key.py
  │           ├── common_column.py
  │           ├── concept_based.py
  │           └── llm_based.py
  ├── registry.py                     # Shared strategy registry system
  └── config.py                       # Configuration management
```

## Key Design Patterns

### 1. Strategy Pattern

Each join path resolution approach is implemented as a separate strategy class that follows a common interface:

```python
class JoinPathStrategy(ABC):
    @abstractmethod
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve join path between tables."""
        pass
```

This allows different strategies to be implemented, tested, and maintained independently.

### 2. Registry Pattern

Strategies register themselves with a central registry:

```python
@StrategyRegistry.register("direct_foreign_key")
class DirectForeignKeyStrategy(JoinPathStrategy):
    # Implementation...
```

This enables dynamic discovery and loading of strategies without hardcoding dependencies.

### 3. Configuration-Driven Approach

The agent loads strategies based on configuration, allowing for runtime customization:

```json
{
  "strategies": [
    {
      "type": "direct_foreign_key",
      "enabled": true,
      "priority": 1
    },
    ...
  ]
}
```

### 4. Protocol-Based Interfaces

The system uses Protocol classes to define interfaces, which improves type safety while maintaining flexibility:

```python
class RelationshipExtractor(Protocol):
    @property
    def name(self) -> str:
        pass
    
    def extract(self, query: str, entities: List[str]) -> Dict[str, Any]:
        pass
```

## Benefits of Refactoring

1. **Reduced File Size**: Each component is now in its own file, vastly reducing individual file sizes.

2. **Improved Extensibility**: Adding a new strategy requires creating a new file that implements the JoinPathStrategy interface and registering it with the StrategyRegistry.

3. **Better Testability**: Each strategy can be tested in isolation.

4. **Configuration-Driven**: Strategies can be enabled/disabled and prioritized via configuration without code changes.

5. **Loose Coupling**: Strategies are not aware of each other, improving maintainability.

6. **Focused Responsibility**: Each class has a single, well-defined responsibility.

## Join Tree Optimization

The refactored RelationshipAgent supports multiple join tree optimization strategies:

1. **Confidence-Based**: Build the join tree starting with highest confidence paths first.
2. **Star Schema**: Identify a central hub entity and build joins radiating from it.

These strategies can be selected via configuration:

```json
{
  "join_tree": {
    "strategy": "confidence_based",
    "max_path_length": 3
  }
}
```

## Example: Adding a New Strategy

To add a new join path strategy:

1. Create a new file: `src/text2sql/reasoning/agents/relationship/strategies/my_new_strategy.py`

2. Implement the strategy:

```python
@StrategyRegistry.register("my_new_strategy")
class MyNewJoinStrategy(JoinPathStrategy):
    @property
    def description(self) -> str:
        return "My new awesome join strategy"
    
    async def resolve(self, source_table: str, target_table: str, context: Dict[str, Any]) -> Dict[str, Any]:
        # Implement new join path discovery logic
        ...
```

3. Add to configuration:

```json
{
  "strategies": [
    ...
    {
      "type": "my_new_strategy",
      "enabled": true,
      "priority": 5
    }
  ]
}
```

No changes to the main RelationshipAgent class are required!

## Future Improvements

1. **Custom Join Tree Algorithms**: Support for more complex join tree optimization algorithms
2. **Multiple Passes**: Implement multi-pass join path discovery for complex cases
3. **Cardinality Awareness**: Consider table sizes when determining join order
4. **Query-Specific Optimization**: Customize join paths based on query intent
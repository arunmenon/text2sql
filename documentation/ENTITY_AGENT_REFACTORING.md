# Entity Agent Refactoring

This document describes the refactored Entity Agent architecture, which follows software engineering best practices for modularity, extensibility, and maintainability.

## Architecture Overview

The refactored EntityAgent uses a plugin-based architecture with strategy and registry patterns to allow for easy extension and modification:

```
src/text2sql/reasoning/
  ├── agents/
  │   └── entity/                     # Modular entity agent package
  │       ├── __init__.py             # Clean public API
  │       ├── entity_agent.py         # Main coordinator (slim!)
  │       ├── base.py                 # Base interfaces
  │       ├── extractors/             # Entity extraction methods
  │       │   ├── __init__.py
  │       │   ├── capitalization.py
  │       │   ├── keyword_based.py
  │       │   └── noun_phrase.py
  │       └── strategies/             # Resolution strategies 
  │           ├── __init__.py
  │           ├── direct_table_match.py
  │           ├── glossary_term_match.py
  │           ├── semantic_concept_match.py
  │           └── llm_based_resolution.py
  └── registry.py                     # Strategy registry system
```

## Key Design Patterns

### 1. Strategy Pattern

Each resolution approach is implemented as a separate strategy class that follows a common interface:

```python
class EntityResolutionStrategy(ABC):
    @abstractmethod
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve entity using strategy."""
        pass
```

This allows different strategies to be implemented, tested, and maintained independently.

### 2. Registry Pattern

Strategies register themselves with a central registry:

```python
@StrategyRegistry.register("direct_table_match")
class DirectTableMatchStrategy(EntityResolutionStrategy):
    # Implementation...
```

This enables dynamic discovery and loading of strategies without hardcoding dependencies.

### 3. Configuration-Driven Approach

The agent loads strategies based on configuration, allowing for runtime customization:

```json
{
  "strategies": [
    {
      "type": "direct_table_match",
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
class EntityExtractor(Protocol):
    @property
    def name(self) -> str:
        pass
    
    def extract(self, query: str) -> List[str]:
        pass
```

## Benefits of Refactoring

1. **Reduced File Size**: Each component is now in its own file, vastly reducing individual file sizes.

2. **Improved Extensibility**: Adding a new strategy only requires:
   - Creating a new strategy class file
   - Implementing the EntityResolutionStrategy interface
   - Registering with the StrategyRegistry

3. **Better Testability**: Each strategy can be tested in isolation.

4. **Configuration-Driven**: Strategies can be enabled/disabled and prioritized via configuration without code changes.

5. **Loose Coupling**: Strategies are not aware of each other, improving maintainability.

6. **Focused Responsibility**: Each class has a single, well-defined responsibility.

## Example: Adding a New Strategy

To add a new strategy:

1. Create a new file: `src/text2sql/reasoning/agents/entity/strategies/my_new_strategy.py`

2. Implement the strategy:

```python
@StrategyRegistry.register("my_new_strategy")
class MyNewStrategy(EntityResolutionStrategy):
    @property
    def description(self) -> str:
        return "My new awesome strategy"
    
    async def resolve(self, entity: str, context: Dict[str, Any]) -> Dict[str, Any]:
        # Implement new resolution logic
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

No changes to the main EntityAgent class are required!

## Future Improvements

1. **Plugin System**: Enhance with a formal plugin system for third-party extensions
2. **Dynamic Loading**: Support loading strategies from external packages
3. **Metrics Tracking**: Add performance metrics for each strategy
4. **Strategy Combinations**: Allow strategies to be combined or chained
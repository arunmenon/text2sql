# Attribute Agent

The AttributeAgent is a crucial component in the text2sql transparent reasoning architecture, responsible for handling non-entity query components like filters, aggregations, groupings, sortings, and limits.

## Purpose

The AttributeAgent bridges the gap between entity/relationship identification and SQL generation by:

1. Identifying query attributes that modify or constrain the requested data
2. Resolving these attributes to specific SQL components (WHERE, GROUP BY, ORDER BY, etc.)
3. Providing transparency in how each attribute is interpreted and resolved
4. Handling ambiguity with multiple resolution strategies and alternatives

## Architecture

The AttributeAgent follows the same modular, extensible architecture as other agents in the system:

### Core Components

- **AttributeAgent**: Main coordinator that manages the extraction and resolution process
- **AttributeExtractors**: Extract potential attributes from the query using different techniques
- **AttributeResolutionStrategies**: Resolve extracted attributes to SQL components
- **AttributeType**: Enumeration of different attribute types (filter, aggregation, grouping, etc.)

### Processing Flow

1. **Attribute Extraction**: Identify potential attributes in the query using multiple extraction methods
2. **Attribute Resolution**: Map attributes to specific SQL components using multiple resolution strategies
3. **Knowledge Boundary Handling**: Identify unmappable attributes and create knowledge boundaries
4. **Alternative Generation**: Generate alternative interpretations for ambiguous attributes

## Resolution Strategies

The AttributeAgent employs multiple resolution strategies:

1. **Column-Based Strategy**: Maps attributes to table columns based on name matching
2. **Semantic Concept Strategy**: Maps attributes to columns using business glossary and semantic concepts
3. **LLM-Based Strategy**: Uses language models to resolve complex or ambiguous attributes

## Extensibility

The AttributeAgent can be extended with:

1. **New Extractors**: Adding specialized extraction techniques
2. **New Resolution Strategies**: Implementing additional mapping approaches
3. **Configuration-Driven Operation**: Customizing behavior via configuration files

## Integration with SQL Generation

The AttributeAgent provides resolved attributes to the SQLAgent, which uses them to construct the final SQL query. Each attribute resolution includes:

- The SQL component it resolves to
- Confidence score
- Original query text
- Metadata about the resolution process

## Example

For a query like "Show me sales over $1000 grouped by region from last month, sorted by amount", the AttributeAgent would identify:

- **Filter**: "over $1000" → `sales.amount > 1000`
- **Filter**: "from last month" → `sales.date >= '2025-02-01' AND sales.date <= '2025-02-28'`
- **Grouping**: "grouped by region" → `sales.region`
- **Sorting**: "sorted by amount" → `sales.amount DESC`

These components are then used by the SQLAgent to construct the final SQL:

```sql
SELECT region, SUM(amount) as total_sales
FROM sales
WHERE amount > 1000 AND (date >= '2025-02-01' AND date <= '2025-02-28')
GROUP BY region
ORDER BY amount DESC
```

## Knowledge Boundaries

When attributes cannot be mapped (e.g., "by quality" when no column represents quality), the AttributeAgent creates knowledge boundaries that:

1. Acknowledge the limitation
2. Explain why the attribute cannot be resolved
3. Provide suggestions for clarification

## Configuration

The AttributeAgent is configured through the `attribute_agent.json` file, which specifies:

- Enabled extractors and their priorities
- Enabled resolution strategies and their configurations
- Priority ordering for strategies
# Project Reorganization Documentation

This document describes the reorganization of the GraphAlchemy (Text2SQL) project structure, specifically the migration of scripts from the project root and the `tools/` directory to a more organized `scripts/` directory structure.

## Summary of Changes

1. **Organization of Scripts**:
   - Moved scattered Python files from the root directory into categorical subdirectories
   - Organized scripts into logical categories: schema, relationships, glossary, database, enhancement, api, cli

2. **Compatibility Layer**:
   - Created wrapper scripts in the root directory (cli.py, text2sql_cli.py, run_api.py)
   - Set up symbolic links in the old locations to point to new script locations
   - Updated all script-to-script references to use the new paths

3. **Documentation**:
   - Added detailed README files for each script category explaining purpose and usage
   - Created this REORGANIZATION.md file to document the changes

## Directory Structure Changes

### Before:
```
/text2sql/
  ├── cli.py
  ├── text2sql_cli.py
  ├── run_api.py
  ├── extract_schema.py
  ├── simulate_schema.py
  ├── infer_relationships.py
  ├── simulate_relationships.py
  ├── generate_glossary.py
  ├── normalize_glossary.py
  ├── generate_concept_tags.py
  ├── run_direct_enhancement.py
  ├── run_enhancement.py
  ├── check_graph.py
  ├── check_neo4j.py
  ├── clear_and_reload.py
  ├── run_walmart_discovery.py
  └── tools/
      ├── schema/
      │   ├── load_schema.py
      │   └── detect_relationships.py
      ├── enhance/
      │   ├── generate_glossary.py
      │   ├── normalize_glossary.py
      │   ├── generate_concept_tags.py
      │   ├── run_direct_enhancement.py
      │   └── run_enhancement.py
      ├── check/
      │   ├── check_database.py
      │   └── check_graph.py
      └── utils/
          └── clear_and_reload.py
```

### After:
```
/text2sql/
  ├── cli.py                  # Wrapper script that forwards to scripts/cli/cli.py
  ├── text2sql_cli.py         # Wrapper script that forwards to scripts/cli/text2sql_cli.py
  ├── run_api.py              # Wrapper script that forwards to scripts/api/run_api.py
  ├── scripts/
  │   ├── schema/              # Schema extraction and manipulation
  │   │   ├── README.md
  │   │   ├── extract_schema.py
  │   │   ├── load_schema_demo.py
  │   │   └── simulate_schema.py
  │   ├── relationships/        # Relationship inference and management
  │   │   ├── README.md
  │   │   ├── infer_relationships.py
  │   │   └── simulate_relationships.py
  │   ├── glossary/            # Business glossary generation and management
  │   │   ├── README.md
  │   │   ├── generate_glossary.py
  │   │   ├── normalize_glossary.py
  │   │   └── generate_concept_tags.py
  │   ├── database/            # Database utilities and checking
  │   │   ├── README.md
  │   │   ├── check_graph.py
  │   │   ├── check_neo4j.py
  │   │   └── clear_and_reload.py
  │   ├── enhancement/         # Metadata enhancement pipelines
  │   │   ├── README.md
  │   │   ├── run_direct_enhancement.py
  │   │   ├── run_enhancement.py
  │   │   └── run_walmart_discovery.py
  │   ├── api/                 # API server scripts
  │   │   ├── README.md
  │   │   └── run_api.py
  │   └── cli/                 # Command-line interfaces
  │       ├── README.md
  │       ├── cli.py
  │       └── text2sql_cli.py
  └── tools/                  # Legacy compatibility directory with symlinks
      ├── README.md           # Explains the compatibility layer
      ├── schema/
      │   ├── load_schema.py  # -> ../../scripts/schema/load_schema_demo.py
      │   └── detect_relationships.py  # -> ../../scripts/relationships/simulate_relationships.py
      ├── enhance/
      │   ├── generate_glossary.py  # -> ../../scripts/glossary/generate_glossary.py
      │   ├── normalize_glossary.py  # -> ../../scripts/glossary/normalize_glossary.py
      │   ├── generate_concept_tags.py  # -> ../../scripts/glossary/generate_concept_tags.py
      │   ├── run_direct_enhancement.py  # -> ../../scripts/enhancement/run_direct_enhancement.py
      │   └── run_enhancement.py  # -> ../../scripts/enhancement/run_enhancement.py
      ├── check/
      │   ├── check_database.py  # -> ../../scripts/database/check_neo4j.py
      │   └── check_graph.py  # -> ../../scripts/database/check_graph.py
      └── utils/
          └── clear_and_reload.py  # -> ../../scripts/database/clear_and_reload.py
```

## Key Path Updates

1. **Main CLI Entry Point**:
   - Before: `./cli.py schema load --tenant-id <tenant_id>`
   - After: `./cli.py schema load --tenant-id <tenant_id>` (wrapper forwards to scripts/cli/cli.py)

2. **Script-to-Script References**:
   - Before: `python simulate_schema.py --tenant-id <tenant_id>`
   - After: `python scripts/schema/simulate_schema.py --tenant-id <tenant_id>`

## Ensuring Compatibility

To ensure functionality is not impacted by this reorganization, we've implemented several compatibility measures:

1. **Root-Level Wrapper Scripts**:
   - Maintain original entry points like cli.py, text2sql_cli.py, run_api.py
   - These wrappers forward all commands to the new script locations

2. **Symbolic Links**:
   - Created symlinks in the original tools/ directory structure
   - These point to the new script locations, allowing existing code to work without changes

3. **Path Updates**:
   - Updated all script-to-script references in the codebase
   - Modified subprocess calls to point to the new script locations

## Testing Recommendation

After this reorganization, please test the following key workflows:

1. **CLI Commands**:
   ```bash
   ./cli.py schema load --tenant-id test_tenant
   ./cli.py enhance run --tenant-id test_tenant --direct
   ./cli.py check database --tenant-id test_tenant
   ```

2. **Text2SQL Interface**:
   ```bash
   ./text2sql_cli.py --query "Show me all products" --tenant-id test_tenant
   ```

3. **API Server**:
   ```bash
   ./run_api.py
   ```

## Future Plans

1. **Phase Out Compatibility Layer**:
   - The tools/ directory with symlinks is a temporary compatibility measure
   - Future versions will remove this layer once all code has been updated

2. **Documentation Updates**:
   - Additional README updates will be made as needed
   - API documentation will be expanded

## Questions or Issues?

If you encounter any issues with the reorganized structure, please:
1. Check that all scripts are executable (`chmod +x <script_path>`)
2. Ensure you're running scripts from the project root directory
3. Verify that the symlinks in the tools/ directory are correctly pointing to their targets
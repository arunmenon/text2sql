#!/usr/bin/env python3
"""
Build Semantic Graph - Main Entry Point

This is a simple wrapper script that calls the main semantic graph builder
orchestration script. It provides a convenient entry point from the root directory.

Usage examples:
    # Run full pipeline
    python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --schema-file schemas/walmart_facilities_complete.sql --csv-dir data/datasets
    
    # Only enhance metadata with LLM (using existing schema)
    python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --only-enhance-metadata --use-llm
    
    # Only generate business glossary
    python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --only-generate-glossary --use-llm
    
    # Skip schema loading and just update relationships
    python build_semantic_graph.py --tenant-id YOUR_TENANT_ID --skip-schema-load --csv-dir data/datasets
"""

import sys
import os
from src.semantic_graph_builder.scripts.build_semantic_graph import main

if __name__ == "__main__":
    # Make the script executable
    os.chmod(__file__, 0o755)
    
    # Run the main function
    import asyncio
    asyncio.run(main())
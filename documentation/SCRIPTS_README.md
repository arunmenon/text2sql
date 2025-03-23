# GraphAlchemy Scripts

This directory contains all the executable scripts for the GraphAlchemy system, organized by function.

## Directory Structure

```
scripts/
├── schema/       # Schema extraction and manipulation
├── relationships/ # Relationship inference and management
├── glossary/     # Business glossary generation and management
├── database/     # Database utilities and checking
├── enhancement/  # Metadata enhancement pipelines
├── api/          # API server scripts
└── cli/          # Command-line interfaces
```

## Script Categories

### Schema Management (`schema/`)
Scripts for extracting, loading, and simulating database schemas.

### Relationship Management (`relationships/`)
Scripts for inferring and simulating relationships between database entities.

### Business Glossary (`glossary/`)
Scripts for generating and managing business glossaries and concept taxonomies.

### Database Tools (`database/`)
Utilities for checking database health, clearing data, and reloading schemas.

### Enhancement Pipelines (`enhancement/`)
Workflows for enhancing metadata using LLMs and other techniques.

### API (`api/`)
Scripts for running API servers and services.

### CLI (`cli/`)
Command-line interfaces for interacting with the system.

## Getting Started

Each subdirectory contains its own README with detailed instructions for running the scripts in that category. For most scripts, you'll need:

1. A running Neo4j instance (see main README for setup)
2. Appropriate environment variables set (see `.env.example` in the root directory)
3. Python dependencies installed via `pip install -r requirements.txt`

For general usage of the system, see the main README in the project root directory.
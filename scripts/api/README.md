# API Server Scripts

This directory contains scripts for running API servers and services in the GraphAlchemy system.

## Available Scripts

### run_api.py
Starts the Text2SQL API server using FastAPI and Uvicorn.

**Usage:**
```bash
python run_api.py
```

**Features:**
- Starts a FastAPI server for Text2SQL functionality
- Configurable host and port via environment variables
- Supports hot reloading for development
- Exposes endpoints for natural language to SQL conversion

**Configuration:**
The server can be configured using the following environment variables in your `.env` file:
- `API_HOST`: The host address to bind to (default: 127.0.0.1)
- `API_PORT`: The port to listen on (default: 8000)

**Prerequisites:**
- Neo4j running with credentials in .env file
- OpenAI API key in .env file
- Python dependencies installed (FastAPI, Uvicorn)

## API Endpoints

The API server exposes the following endpoints:

### `POST /api/text2sql`
Converts natural language to SQL.

**Request Body:**
```json
{
  "query": "Show me all products with low inventory",
  "tenant_id": "your_tenant_id"
}
```

**Response:**
```json
{
  "query": "Show me all products with low inventory",
  "primary_interpretation": {
    "sql": "SELECT p.product_name, i.quantity FROM products p JOIN inventory i ON p.product_id = i.product_id WHERE i.quantity < 10",
    "explanation": "This query finds all products with inventory quantity less than 10, which is considered low.",
    "confidence": 0.95,
    "tables_used": ["products", "inventory"]
  },
  "alternative_interpretations": [
    {
      "sql": "SELECT p.product_name, i.quantity FROM products p JOIN inventory i ON p.product_id = i.product_id ORDER BY i.quantity ASC LIMIT 10",
      "explanation": "Alternative interpretation showing the 10 products with the lowest inventory levels.",
      "confidence": 0.85,
      "tables_used": ["products", "inventory"]
    }
  ],
  "query_metadata": {
    "processing_time_ms": 352,
    "tenant_id": "your_tenant_id",
    "model_used": "gpt-4o"
  }
}
```

## Running in Production

For production deployments, consider:

1. Using a process manager like Supervisor or systemd
2. Setting up HTTPS with a reverse proxy (Nginx, Apache)
3. Implementing proper authentication middleware
4. Setting appropriate concurrency limits

Example production start command:
```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4
```
"""
Run the Text2SQL API server.
"""
import os
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Host and port
host = os.getenv("API_HOST", "127.0.0.1")
port = int(os.getenv("API_PORT", "8000"))

if __name__ == "__main__":
    print(f"Starting Text2SQL API on http://{host}:{port}")
    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)
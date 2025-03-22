FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
COPY README.md .

# Install project dependencies
RUN pip install --no-cache-dir -e .

# Copy source code
COPY src/ src/

# Create credentials directory
RUN mkdir -p /app/credentials

# Expose port
EXPOSE 8000

# Start API server
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
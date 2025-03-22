"""
Setup script for the text2sql package.
"""
from setuptools import setup, find_packages

setup(
    name="text2sql",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery>=3.11.0",
        "neo4j>=5.8.0",
        "sqlglot>=19.0.0",
        "fastapi>=0.95.0",
        "uvicorn>=0.22.0",
        "pydantic>=2.0.0",
        "python-levenshtein>=0.21.0",
        "python-dotenv>=1.0.0",
        "networkx>=3.1",
    ],
)
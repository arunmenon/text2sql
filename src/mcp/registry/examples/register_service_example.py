#!/usr/bin/env python3
"""
Example of programmatically registering a service with the Metadata Registry.

This script demonstrates three ways to register a service:
1. Via the HTTP API
2. By creating a YAML file in the watched directory
3. Through direct API calls to the registry
"""
import os
import sys
import json
import yaml
import argparse
import requests
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = str(Path(__file__).parent.parent.parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import registry API if available
try:
    from src.mcp.registry.metadata_registry import get_metadata_registry, ServiceMetadata
    REGISTRY_AVAILABLE = True
except ImportError:
    REGISTRY_AVAILABLE = False

# Sample service definition
SAMPLE_SERVICE = {
    "service_id": "sample-weather-service",
    "name": "weather_service",
    "description": "Weather data service providing current and forecast weather",
    "service_type": "rest",
    "version": "1.0.0",
    "endpoints": {
        "uri": "${WEATHER_API_URL:-https://api.weather.example}"
    },
    "metadata": {
        "api_key": "${WEATHER_API_KEY:-demo_key}",
        "verification": {
            "type": "api_key",
            "key_name": "x-api-key"
        }
    },
    "tags": [
        "weather",
        "forecast",
        "meteorology"
    ],
    "operations": [
        {
            "name": "get_current_weather",
            "description": "Get current weather for a location",
            "schema": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City name or ZIP code"
                    },
                    "units": {
                        "type": "string",
                        "description": "Temperature units (metric or imperial)",
                        "enum": ["metric", "imperial"],
                        "default": "metric"
                    }
                },
                "required": ["location"]
            }
        },
        {
            "name": "get_forecast",
            "description": "Get weather forecast for a location",
            "schema": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City name or ZIP code"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days to forecast",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 5
                    },
                    "units": {
                        "type": "string",
                        "description": "Temperature units (metric or imperial)",
                        "enum": ["metric", "imperial"],
                        "default": "metric"
                    }
                },
                "required": ["location"]
            }
        }
    ]
}

def register_via_http_api(api_url, service_data):
    """Register a service via the HTTP API."""
    print(f"Registering service via HTTP API at {api_url}...")
    
    try:
        response = requests.post(
            f"{api_url}/services",
            json=service_data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 201:
            print("Service registered successfully!")
            print(f"Response: {response.json()}")
            return True
        else:
            print(f"Failed to register service: {response.status_code}")
            print(f"Error: {response.text}")
            return False
    
    except Exception as e:
        print(f"Error connecting to API: {str(e)}")
        return False

def register_via_file(directory, service_data):
    """Register a service by creating a YAML file in the watched directory."""
    print(f"Registering service via file in {directory}...")
    
    # Ensure directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Create file path
    file_path = os.path.join(directory, f"{service_data['service_id']}.yaml")
    
    try:
        # Write service definition to file
        with open(file_path, 'w') as f:
            yaml.dump(service_data, f, default_flow_style=False)
        
        print(f"Service definition written to {file_path}")
        print("The registry will automatically discover this file if watching the directory.")
        return True
    
    except Exception as e:
        print(f"Error writing service definition: {str(e)}")
        return False

def register_via_direct_api(service_data):
    """Register a service directly with the registry API."""
    if not REGISTRY_AVAILABLE:
        print("Registry API not available for direct registration.")
        return False
    
    print("Registering service directly with registry API...")
    
    try:
        # Get registry instance
        registry = get_metadata_registry()
        
        # Create service metadata
        service = ServiceMetadata.from_dict(service_data)
        service.source = "direct_api"
        
        # Register service
        if registry.register_service(service):
            print(f"Service {service_data['service_id']} registered successfully!")
            return True
        else:
            print(f"Failed to register service {service_data['service_id']}")
            return False
    
    except Exception as e:
        print(f"Error registering service: {str(e)}")
        return False

def main():
    """Main function to demonstrate service registration."""
    parser = argparse.ArgumentParser(description="Service Registration Example")
    parser.add_argument(
        "--method",
        choices=["http", "file", "direct", "all"],
        default="all",
        help="Registration method to use"
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8235",
        help="URL of the Metadata Registry API"
    )
    parser.add_argument(
        "--services-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data", "services"),
        help="Directory for service definition files"
    )
    parser.add_argument(
        "--service-id",
        help="Override the service ID"
    )
    
    args = parser.parse_args()
    
    # Copy service data to avoid modifying the original
    service_data = dict(SAMPLE_SERVICE)
    
    # Override service ID if specified
    if args.service_id:
        service_data["service_id"] = args.service_id
    
    # Register via specified method(s)
    if args.method in ["http", "all"]:
        register_via_http_api(args.api_url, service_data)
        print()
    
    if args.method in ["file", "all"]:
        register_via_file(args.services_dir, service_data)
        print()
    
    if args.method in ["direct", "all"] and REGISTRY_AVAILABLE:
        register_via_direct_api(service_data)

if __name__ == "__main__":
    main()
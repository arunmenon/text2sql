"""
Service API for the Metadata Registry

This module provides API endpoints for services to register themselves
with the Metadata Registry.
"""
import os
import logging
import json
from typing import Dict, List, Any, Optional
from flask import Flask, request, jsonify

from ..metadata_registry import get_metadata_registry, ServiceMetadata

logger = logging.getLogger(__name__)

app = Flask(__name__)
registry = None

@app.route('/services', methods=['GET'])
def list_services():
    """List all registered services."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    services = []
    for service_id, service in registry.services.items():
        services.append(service.to_dict())
    
    return jsonify({"services": services})

@app.route('/services/<service_id>', methods=['GET'])
def get_service(service_id):
    """Get a specific service by ID."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    service = registry.get_service(service_id)
    if not service:
        return jsonify({"error": f"Service {service_id} not found"}), 404
    
    return jsonify(service.to_dict())

@app.route('/services', methods=['POST'])
def register_service():
    """Register a new service."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    try:
        # Get service data from request
        service_data = request.json
        if not service_data:
            return jsonify({"error": "No service data provided"}), 400
        
        # Add service to registry
        service_id = registry.add_service_from_dict(service_data)
        if not service_id:
            return jsonify({"error": "Failed to register service"}), 400
        
        return jsonify({
            "status": "success",
            "message": f"Service registered with ID: {service_id}",
            "service_id": service_id
        }), 201
    
    except Exception as e:
        logger.error(f"Error registering service: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/services/<service_id>', methods=['PUT'])
def update_service(service_id):
    """Update an existing service."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    # Check if service exists
    service = registry.get_service(service_id)
    if not service:
        return jsonify({"error": f"Service {service_id} not found"}), 404
    
    try:
        # Get service data from request
        service_data = request.json
        if not service_data:
            return jsonify({"error": "No service data provided"}), 400
        
        # Ensure service_id matches
        service_data["service_id"] = service_id
        
        # Remove old service
        registry.unregister_service(service_id)
        
        # Add updated service
        registry.add_service_from_dict(service_data)
        
        return jsonify({
            "status": "success",
            "message": f"Service {service_id} updated"
        })
    
    except Exception as e:
        logger.error(f"Error updating service: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/services/<service_id>', methods=['DELETE'])
def unregister_service(service_id):
    """Unregister a service."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    # Check if service exists
    service = registry.get_service(service_id)
    if not service:
        return jsonify({"error": f"Service {service_id} not found"}), 404
    
    # Unregister service
    registry.unregister_service(service_id)
    
    return jsonify({
        "status": "success",
        "message": f"Service {service_id} unregistered"
    })

@app.route('/discover', methods=['POST'])
def trigger_discovery():
    """Trigger service discovery manually."""
    if not registry:
        return jsonify({"error": "Registry not initialized"}), 500
    
    # Refresh discovery
    registry.refresh_discovery()
    
    return jsonify({
        "status": "success",
        "message": "Service discovery triggered"
    })

def init_app(registry_instance=None):
    """Initialize the Flask app with the registry."""
    global registry
    registry = registry_instance or get_metadata_registry()
    return app

def run_api(host="0.0.0.0", port=8235, registry_instance=None):
    """Run the API server."""
    init_app(registry_instance)
    logger.info(f"Starting Service API on {host}:{port}")
    app.run(host=host, port=port)
# External Service Definitions

This directory contains external service definitions discovered by the Metadata Registry. Unlike the built-in services in `src/mcp/registry/services/`, these definitions can be added, modified, or removed without changing the application code.

## Adding New Services

To add a new service:

1. Create a YAML file with the service definition following this template:

```yaml
service_id: unique-service-id
name: service_name
description: "Service description"
service_type: "service_type"
version: "1.0.0"
endpoints:
  uri: ${ENV_VAR:-default_value}
metadata:
  # Any metadata needed by the service
operations:
  - name: operation_name
    description: "Operation description"
    schema:
      # OpenAPI-style schema for the operation
```

2. Save the file in one of these locations:
   - `{data_dir}/services/` - For local services
   - Any directory configured as a discovery path in `config/registry.yaml`
   - Any HTTP endpoint configured in `config/registry.yaml`

The registry will automatically discover the service through the appropriate feed.

## Dynamic Service Registration

Services can register themselves at runtime via:

1. **File-based**: Drop a YAML definition in the watched directory
2. **HTTP-based**: POST to the registry's registration endpoint
3. **Plugin-based**: Create a Python module with service definition

## Service Versioning

When a service definition changes, the registry follows these rules:

1. If a new version of an existing service is found, it replaces the older version
2. Services are identified by their `service_id`
3. Version comparison uses semver conventions
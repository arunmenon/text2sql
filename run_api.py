#!/usr/bin/env python3
"""
Run the Text2SQL API server.

This is a compatibility wrapper that forwards execution to the new API location.
"""
import os
import sys

if __name__ == "__main__":
    # Add executable permission to the actual API script if needed
    os.chmod("scripts/api/run_api.py", 0o755)
    
    # Forward all arguments to the actual API script
    exit_code = os.system(f"python3 scripts/api/run_api.py")
    
    # Exit with the same exit code
    sys.exit(exit_code >> 8)  # Extract the actual exit code from the system call
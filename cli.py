#!/usr/bin/env python3
"""
Text2SQL CLI - Main entry point for text2sql commands

This is a compatibility wrapper that forwards all commands to the new CLI location.
"""
import os
import sys

if __name__ == "__main__":
    # Add executable permission to the actual CLI script if needed
    os.chmod("scripts/cli/cli.py", 0o755)
    
    # Forward all arguments to the actual CLI script
    args = " ".join(sys.argv[1:])
    exit_code = os.system(f"python3 scripts/cli/cli.py {args}")
    
    # Exit with the same exit code
    sys.exit(exit_code >> 8)  # Extract the actual exit code from the system call
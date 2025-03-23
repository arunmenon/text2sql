# Legacy Compatibility Directory

This directory contains symbolic links to the actual scripts in the /scripts directory.
It exists to maintain backward compatibility with code that references the old paths.

Please update your code to use the new paths directly:

- tools/schema/* → scripts/schema/*
- tools/enhance/* → scripts/glossary/* or scripts/enhancement/*
- tools/check/* → scripts/database/*
- tools/utils/* → scripts/database/*

This compatibility layer will be removed in a future update.


#!/bin/bash
# Serve MkDocs documentation locally
# Usage: ./serve-docs.sh

set -e

echo "🚀 Starting MkDocs development server..."
echo "📚 Documentation will be available at: http://127.0.0.1:8000"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Serve documentation
uv run mkdocs serve --livereload

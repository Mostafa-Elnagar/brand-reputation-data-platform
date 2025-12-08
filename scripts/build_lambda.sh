#!/bin/bash
set -e

# Build script for Reddit ingestion Lambda package
# Creates a deployable ZIP file with all dependencies

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/build/reddit_ingestion_lambda"
OUTPUT_ZIP="$BUILD_DIR/reddit_ingestion_lambda.zip"

echo "=== Building Reddit Ingestion Lambda Package ==="
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"

# Clean and create build directory
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/package"

# Install dependencies
echo "Installing dependencies..."
pip install \
    --target "$BUILD_DIR/package" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    -r "$PROJECT_ROOT/lambda/ingestion/requirements.txt" \
    --quiet

# Copy Lambda handler
echo "Copying Lambda handler..."
cp "$PROJECT_ROOT/lambda/ingestion/handler.py" "$BUILD_DIR/package/"

# Copy modules
echo "Copying modules..."
cp -r "$PROJECT_ROOT/modules" "$BUILD_DIR/package/"

# Create ZIP package
echo "Creating ZIP package..."
cd "$BUILD_DIR/package"
zip -r "$OUTPUT_ZIP" . -q

# Cleanup
rm -rf "$BUILD_DIR/package"

# Report
ZIP_SIZE=$(du -h "$OUTPUT_ZIP" | cut -f1)
echo ""
echo "=== Build Complete ==="
echo "Output: $OUTPUT_ZIP"
echo "Size: $ZIP_SIZE"

#!/bin/bash
set -e

echo "🧪 Running integration tests without Docker dependencies..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Run mock/unit integration tests
echo -e "${BLUE}Running S3 mock integration tests...${NC}"
RUST_LOG=info cargo test --test s3_integration_test

echo -e "${BLUE}Running HDFS mock integration tests...${NC}" 
RUST_LOG=info cargo test --test hdfs_integration_test

echo -e "${BLUE}Running argument-based reader tests...${NC}"
RUST_LOG=info cargo test --test argument_based_reader_test

echo -e "${BLUE}Running general integration tests...${NC}"
RUST_LOG=info cargo test --test integration_test

echo -e "${GREEN}✅ All mock integration tests passed!${NC}"
echo ""
echo -e "${YELLOW}💡 To test with real services:${NC}"
echo -e "  1. Start Docker Desktop"
echo -e "  2. Run: ./scripts/setup-dev-environment.sh"
echo -e "  3. Run: ./scripts/verify-integrations.sh"
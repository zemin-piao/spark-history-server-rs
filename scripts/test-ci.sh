#!/bin/bash
set -e

echo "🧪 Running CI test suite (no Docker dependencies)..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Run tests excluding Docker-dependent integration tests
echo -e "${BLUE}Running unit tests and mock integration tests...${NC}"
cargo test --all-targets

echo -e "${BLUE}Skipping performance tests (run manually with --features performance-tests)...${NC}"
# cargo test --features performance-tests

echo -e "${GREEN}✅ All CI tests passed!${NC}"
echo ""
echo -e "${BLUE}💡 To run Docker-dependent integration tests locally:${NC}"
echo -e "  ./scripts/test-local.sh"
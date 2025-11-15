#!/bin/bash
set -e

echo "🧪 Running CI test suite (no Docker dependencies)..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Run unit tests first
echo -e "${BLUE}Running unit tests...${NC}"
cargo test --lib --verbose

# Run integration tests excluding large-scale tests
echo -e "${BLUE}Running integration tests (excluding large-scale tests)...${NC}"
cargo test --test integration_test
cargo test --test analytics_api_test
cargo test --test hdfs_integration_test
cargo test --test incremental_scan_test
cargo test --test batch_writer_debug_test
cargo test --test argument_based_reader_test

echo -e "${YELLOW}Skipping large-scale tests (run with [run-large-scale] in commit message)${NC}"
echo -e "${YELLOW}Skipping performance tests (run manually with --features performance-tests)${NC}"

echo -e "${GREEN}✅ All CI tests passed!${NC}"
echo ""
echo -e "${BLUE}💡 To run additional tests locally:${NC}"
echo -e "  Docker integration tests:  ./scripts/test-local.sh"
echo -e "  Large-scale tests:         cargo test --release --test large_scale_test"
echo -e "  Performance tests:         cargo test --features performance-tests"
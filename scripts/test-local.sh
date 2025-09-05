#!/bin/bash
set -e

echo "🧪 Running full local test suite (including Docker integration tests)..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Run CI tests first
echo -e "${BLUE}Running CI test suite...${NC}"
./scripts/test-ci.sh

# Start Docker services if not running
echo -e "${BLUE}Checking Docker services...${NC}"
if ! docker ps | grep -q spark-history-minio; then
    echo -e "${YELLOW}Starting MinIO...${NC}"
    docker-compose -f docker-compose.minio.yml up -d
    sleep 5
fi

if ! docker ps | grep -q spark-history-hdfs-namenode; then
    echo -e "${YELLOW}Starting HDFS cluster...${NC}"
    docker-compose -f docker-compose.hdfs.yml up -d
    sleep 30
fi

# Run integration tests with Docker containers
echo -e "${BLUE}Running Docker-dependent integration tests...${NC}"
export S3_TEST_BUCKET=spark-events
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_REGION=us-east-1
export HDFS_NAMENODE_URL=hdfs://localhost:8020

cargo test --features integration-tests

echo -e "${GREEN}🎉 All tests passed (CI + Docker integration)!${NC}"
echo ""
echo -e "${YELLOW}📋 Test Summary:${NC}"
echo -e "  ✅ Unit tests and mock integration tests"
echo -e "  ✅ Performance tests"  
echo -e "  ✅ Real S3 (MinIO) integration tests"
echo -e "  ✅ Real HDFS integration tests"
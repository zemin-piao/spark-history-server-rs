#!/bin/bash
set -e

echo "🚀 Setting up MinIO (S3) local development environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo -e "${BLUE}Checking prerequisites...${NC}"

if ! command_exists docker; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command_exists docker-compose; then
    echo -e "${RED}❌ Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker and Docker Compose are installed${NC}"

# Start MinIO service
echo -e "${BLUE}Starting MinIO service...${NC}"
docker-compose -f docker-compose.minio.yml up -d

# Wait for MinIO to be ready
echo -e "${YELLOW}⏳ Waiting for MinIO to be ready...${NC}"
timeout=60
count=0
while ! curl -s http://localhost:9000/minio/health/live > /dev/null; do
    if [ $count -ge $timeout ]; then
        echo -e "${RED}❌ MinIO failed to start within $timeout seconds${NC}"
        exit 1
    fi
    sleep 2
    count=$((count + 2))
    echo -n "."
done
echo -e "\n${GREEN}✅ MinIO is ready at http://localhost:9000${NC}"

# Initialize MinIO buckets and upload test data
echo -e "${BLUE}Setting up MinIO buckets and test data...${NC}"
./scripts/upload-test-data.sh

echo -e "${GREEN}🎉 MinIO development environment is ready!${NC}"
echo ""
echo -e "${YELLOW}📋 Service URLs:${NC}"
echo -e "  • MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo -e "  • MinIO API: http://localhost:9000"
echo ""
echo -e "${YELLOW}🧪 Run S3 tests:${NC}"
echo -e "  • S3_TEST_BUCKET=spark-events AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin123 cargo test test_real_s3 --ignored"
echo ""
echo -e "${YELLOW}🔧 Run application with MinIO:${NC}"
echo -e "  • cargo run -- --s3 --s3-bucket spark-events --s3-endpoint http://localhost:9000 --s3-access-key minioadmin --s3-secret-key minioadmin123"
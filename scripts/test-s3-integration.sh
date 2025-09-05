#!/bin/bash
set -e

echo "🧪 Testing S3 integration with MinIO..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Start MinIO
echo -e "${BLUE}Starting MinIO...${NC}"
docker-compose -f docker-compose.minio.yml up -d

# Wait for MinIO
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
echo -e "\n${GREEN}✅ MinIO is ready${NC}"

# Set up environment for tests
export S3_TEST_BUCKET=spark-events
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_REGION=us-east-1

echo -e "${BLUE}Running real S3 integration tests...${NC}"

# Run the real S3 tests
if cargo test test_real_s3_health_check -- --ignored; then
    echo -e "${GREEN}✅ S3 health check test passed${NC}"
else
    echo -e "${RED}❌ S3 health check test failed${NC}"
fi

if cargo test test_real_s3_list_directory -- --ignored; then
    echo -e "${GREEN}✅ S3 directory listing test passed${NC}"
else
    echo -e "${RED}❌ S3 directory listing test failed${NC}"
fi

if cargo test test_real_s3_file_exists -- --ignored; then
    echo -e "${GREEN}✅ S3 file exists test passed${NC}"
else
    echo -e "${RED}❌ S3 file exists test failed${NC}"
fi

echo -e "${BLUE}Testing application startup with S3 backend...${NC}"
# Use gtimeout on macOS or timeout on Linux
TIMEOUT_CMD="timeout"
if command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_CMD="gtimeout"
fi

if command -v $TIMEOUT_CMD >/dev/null 2>&1; then
    $TIMEOUT_CMD 10s cargo run -- \
        --s3 \
        --s3-bucket spark-events \
        --s3-endpoint http://localhost:9000 \
        --aws-access-key-id minioadmin \
        --aws-secret-access-key minioadmin123 \
        --port 18081 || test $? -eq 124
else
    echo -e "${YELLOW}⚠️ timeout command not available, testing basic compilation only${NC}"
    cargo check
fi

if [ $? -eq 124 ]; then
    echo -e "${GREEN}✅ Application started successfully with S3 backend${NC}"
else
    echo -e "${RED}❌ Application failed to start with S3 backend${NC}"
fi

echo -e "${GREEN}🎉 S3 integration testing complete!${NC}"
echo -e "${YELLOW}💡 MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)${NC}"
echo -e "${YELLOW}💡 MinIO API: http://localhost:9000${NC}"
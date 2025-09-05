#!/bin/bash
set -e

echo "🔍 Verifying S3 and HDFS integrations..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Track test results
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local test_name="$1"
    local test_command="$2"
    
    echo -e "${BLUE}Running: $test_name${NC}"
    
    if eval "$test_command"; then
        echo -e "${GREEN}✅ $test_name PASSED${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}❌ $test_name FAILED${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
    echo ""
}

echo -e "${YELLOW}📋 Running integration verification tests...${NC}"
echo ""

# Test 1: Basic S3 integration tests
run_test "S3 Integration Tests" "cargo test s3_integration_test"

# Test 2: Basic HDFS integration tests  
run_test "HDFS Integration Tests" "cargo test hdfs_integration_test"

# Test 3: Real MinIO S3 tests
echo -e "${BLUE}Setting up environment for MinIO tests...${NC}"
export S3_TEST_BUCKET=spark-events
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_REGION=us-east-1

run_test "MinIO Health Check Test" "cargo test test_real_s3_health_check --ignored"
run_test "MinIO Directory Listing Test" "cargo test test_real_s3_list_directory --ignored"
run_test "MinIO File Exists Test" "cargo test test_real_s3_file_exists --ignored"

# Test 4: Real HDFS tests
echo -e "${BLUE}Setting up environment for HDFS tests...${NC}"
export HDFS_NAMENODE_URL=hdfs://localhost:8020
export SPARK_EVENTS_DIR=/spark-events

run_test "HDFS Real Integration Test" "cargo test test_real_hdfs --ignored"

# Test 5: Application startup tests with different backends
echo -e "${BLUE}Testing application startup with different backends...${NC}"

# Test S3 mode startup (quick test)
run_test "Application S3 Mode Startup" "timeout 10s cargo run -- --s3 --s3-bucket spark-events --s3-endpoint http://localhost:9000 --s3-access-key minioadmin --s3-secret-key minioadmin123 --port 18081 || test \$? -eq 124"

# Test HDFS mode startup (quick test)  
run_test "Application HDFS Mode Startup" "timeout 10s cargo run -- --hdfs --hdfs-namenode hdfs://localhost:8020 --log-directory /spark-events --port 18082 || test \$? -eq 124"

# Summary
echo -e "${YELLOW}📊 Test Results Summary:${NC}"
echo -e "  ${GREEN}✅ Tests Passed: $TESTS_PASSED${NC}"
echo -e "  ${RED}❌ Tests Failed: $TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All integration tests passed!${NC}"
    exit 0
else
    echo -e "${RED}💥 Some tests failed. Check the output above for details.${NC}"
    exit 1
fi
#!/bin/bash
set -e

echo "📁 Uploading test data to MinIO and HDFS..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# MinIO client configuration
MINIO_ALIAS="local-minio"
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin123"
BUCKET_NAME="spark-events"

# Check if mc (MinIO client) is available
MC_CMD="mc"
if ! command -v mc >/dev/null 2>&1; then
    echo -e "${BLUE}Installing MinIO client (mc)...${NC}"
    
    # Detect OS and architecture
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    
    case $ARCH in
        x86_64) ARCH="amd64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac
    
    # Download mc to current directory instead of system-wide installation
    curl -O "https://dl.min.io/client/mc/release/${OS}-${ARCH}/mc"
    chmod +x mc
    # Use local mc instead of system installation
    MC_CMD="./mc"
    echo -e "${GREEN}✅ MinIO client installed${NC}"
fi

# Configure MinIO client
echo -e "${BLUE}Configuring MinIO client...${NC}"
$MC_CMD alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Create bucket
echo -e "${BLUE}Creating S3 bucket: $BUCKET_NAME...${NC}"
$MC_CMD mb $MINIO_ALIAS/$BUCKET_NAME --ignore-existing

# Upload test data to MinIO if exists
if [ -d "test-data/spark-events" ]; then
    echo -e "${BLUE}Uploading test data to S3 bucket...${NC}"
    $MC_CMD cp --recursive test-data/spark-events/ $MINIO_ALIAS/$BUCKET_NAME/
    echo -e "${GREEN}✅ Test data uploaded to MinIO${NC}"
else
    echo -e "${YELLOW}⚠️ No test-data/spark-events directory found, creating sample data...${NC}"
    
    # Create sample event data
    mkdir -p test-data/spark-events/application_1234567890_0001
    cat > test-data/spark-events/application_1234567890_0001/events << EOF
{"Event":"SparkListenerApplicationStart","App Name":"Sample Spark App","App ID":"application_1234567890_0001","Timestamp":1640995200000,"User":"spark"}
{"Event":"SparkListenerJobStart","Job ID":0,"Stage Infos":[],"Properties":{},"Timestamp":1640995201000}
{"Event":"SparkListenerJobEnd","Job ID":0,"Job Result":"JobSucceeded","Timestamp":1640995210000}
{"Event":"SparkListenerApplicationEnd","Timestamp":1640995220000}
EOF
    
    echo -e "${GREEN}✅ Sample test data created${NC}"
    $MC_CMD cp --recursive test-data/spark-events/ $MINIO_ALIAS/$BUCKET_NAME/
    echo -e "${GREEN}✅ Sample data uploaded to MinIO${NC}"
fi

# Upload test data to HDFS
echo -e "${BLUE}Uploading test data to HDFS...${NC}"
if [ -d "test-data/spark-events" ]; then
    # Copy test data into HDFS container and then to HDFS
    docker cp test-data/spark-events spark-history-hdfs-namenode:/tmp/spark-events
    docker exec spark-history-hdfs-namenode hdfs dfs -put -f /tmp/spark-events/* /spark-events/
    echo -e "${GREEN}✅ Test data uploaded to HDFS${NC}"
else
    echo -e "${YELLOW}⚠️ Using sample data for HDFS${NC}"
    docker exec spark-history-hdfs-namenode hdfs dfs -mkdir -p /spark-events/application_1234567890_0001
    
    # Create sample data inside container
    docker exec spark-history-hdfs-namenode bash -c 'cat > /tmp/events << EOF
{"Event":"SparkListenerApplicationStart","App Name":"Sample HDFS App","App ID":"application_1234567890_0001","Timestamp":1640995200000,"User":"spark"}
{"Event":"SparkListenerJobStart","Job ID":0,"Stage Infos":[],"Properties":{},"Timestamp":1640995201000}
{"Event":"SparkListenerJobEnd","Job ID":0,"Job Result":"JobSucceeded","Timestamp":1640995210000}
{"Event":"SparkListenerApplicationEnd","Timestamp":1640995220000}
EOF'
    
    docker exec spark-history-hdfs-namenode hdfs dfs -put /tmp/events /spark-events/application_1234567890_0001/
    echo -e "${GREEN}✅ Sample data uploaded to HDFS${NC}"
fi

# Verify uploads
echo -e "${BLUE}Verifying uploads...${NC}"

echo -e "${YELLOW}MinIO contents:${NC}"
$MC_CMD ls --recursive $MINIO_ALIAS/$BUCKET_NAME

echo -e "${YELLOW}HDFS contents:${NC}"
docker exec spark-history-hdfs-namenode hdfs dfs -ls -R /spark-events

echo -e "${GREEN}🎉 Test data setup complete!${NC}"
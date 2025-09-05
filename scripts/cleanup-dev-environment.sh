#!/bin/bash
set -e

echo "🧹 Cleaning up local development environment..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Stop and remove containers
echo -e "${BLUE}Stopping Docker containers...${NC}"
docker-compose down

# Remove volumes if requested
if [ "$1" = "--volumes" ] || [ "$1" = "-v" ]; then
    echo -e "${YELLOW}Removing Docker volumes (this will delete all data)...${NC}"
    docker-compose down -v
    echo -e "${GREEN}✅ Docker volumes removed${NC}"
fi

# Remove any test data artifacts
if [ "$1" = "--all" ]; then
    echo -e "${YELLOW}Removing test data artifacts...${NC}"
    rm -rf test-data/spark-events/application_1234567890_0001 || true
    echo -e "${GREEN}✅ Test artifacts cleaned${NC}"
fi

echo -e "${GREEN}🎉 Environment cleanup complete!${NC}"
echo ""
echo -e "${YELLOW}💡 Usage examples:${NC}"
echo -e "  • Basic cleanup: ./scripts/cleanup-dev-environment.sh"
echo -e "  • Remove volumes: ./scripts/cleanup-dev-environment.sh --volumes"
echo -e "  • Full cleanup: ./scripts/cleanup-dev-environment.sh --all"
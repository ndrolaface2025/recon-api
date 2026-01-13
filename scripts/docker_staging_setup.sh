#!/bin/bash
# Docker Staging Setup Script for Hostinger
# This script helps you run commands inside your Docker container on Hostinger

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐳 Docker Staging Database Setup${NC}"
echo "================================="
echo ""

# Configuration
CONTAINER_NAME="recon-api-backend"  # Change to your container name
DB_HOST="postgres"  # If postgres is in separate container
DB_PORT="5432"
DB_NAME="recon_staging_db"
DB_USER="staging_user"

# Function to run command in container
run_in_container() {
    echo -e "${YELLOW}Running in container: $1${NC}"
    docker exec -it $CONTAINER_NAME bash -c "$1"
}

# Check if running on Hostinger (remote) or locally
if [ "$1" == "remote" ]; then
    echo -e "${BLUE}📡 Remote Mode: Connecting to Hostinger${NC}"
    SSH_HOST="${2:-your-server@hostinger}"
    
    echo "Usage for remote:"
    echo "  ./scripts/docker_staging_setup.sh remote user@your-hostinger-ip"
    echo ""
    
    # Run commands via SSH
    ssh $SSH_HOST << 'ENDSSH'
        cd /path/to/your/project
        
        # List containers
        echo "📦 Available containers:"
        docker ps
        
        echo ""
        echo "To run migrations:"
        echo "  docker exec -it <container-name> alembic upgrade head"
        
        echo ""
        echo "To seed database:"
        echo "  docker exec -it <container-name> python scripts/seed_staging_database.py --host postgres --password <pwd>"
ENDSSH
else
    # Local mode - assumes you're SSH'd into Hostinger
    echo -e "${BLUE}🏠 Local Mode: Running commands directly${NC}"
    echo ""
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker not found. Make sure you're on the server with Docker installed.${NC}"
        exit 1
    fi
    
    # List running containers
    echo -e "${GREEN}📦 Running containers:${NC}"
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
    echo ""
    
    # Menu
    echo "What would you like to do?"
    echo "1. Run migrations (alembic upgrade head)"
    echo "2. Seed database with initial data"
    echo "3. Check database connection"
    echo "4. Reset match status (clear all matches)"
    echo "5. Enter container shell"
    echo "6. View logs"
    echo "0. Exit"
    echo ""
    
    read -p "Select option: " option
    
    case $option in
        1)
            echo -e "${GREEN}🔄 Running migrations...${NC}"
            run_in_container "alembic upgrade head"
            ;;
        2)
            read -p "Enter database password: " -s DB_PASS
            echo ""
            echo -e "${GREEN}🌱 Seeding database...${NC}"
            run_in_container "python scripts/seed_staging_database.py --host $DB_HOST --password $DB_PASS --database $DB_NAME --user $DB_USER"
            ;;
        3)
            echo -e "${GREEN}🔌 Checking database connection...${NC}"
            run_in_container "python -c \"from app.db.session import AsyncSessionLocal; import asyncio; async def test(): async with AsyncSessionLocal() as db: result = await db.execute('SELECT 1'); print('✅ Connection successful!'); asyncio.run(test())\""
            ;;
        4)
            echo -e "${YELLOW}⚠️  This will reset all match statuses. Are you sure? (y/n)${NC}"
            read -p "" confirm
            if [ "$confirm" == "y" ]; then
                echo -e "${GREEN}🔄 Resetting match status...${NC}"
                run_in_container "python scripts/reset_match_status.py"
            fi
            ;;
        5)
            echo -e "${GREEN}🐚 Entering container shell...${NC}"
            docker exec -it $CONTAINER_NAME bash
            ;;
        6)
            echo -e "${GREEN}📋 Viewing logs (Ctrl+C to exit)...${NC}"
            docker logs -f $CONTAINER_NAME
            ;;
        0)
            echo "Goodbye!"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            exit 1
            ;;
    esac
fi

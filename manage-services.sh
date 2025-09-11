#!/bin/bash

# File Processing Service Management Script

set -e

case "$1" in
    start)
        echo "🚀 Starting all services..."
        docker-compose up -d
        echo "✅ All services started"
        ;;
    stop)
        echo "🛑 Stopping all services..."
        docker-compose down
        echo "✅ All services stopped"
        ;;
    restart)
        echo "🔄 Restarting all services..."
        docker-compose down
        docker-compose up -d
        echo "✅ All services restarted"
        ;;
    restart-processor)
        echo "🔄 Restarting file processor..."
        docker-compose restart file-processor
        echo "✅ File processor restarted"
        ;;
    logs)
        echo "📋 Showing logs for all services..."
        docker-compose logs -f
        ;;
    logs-processor)
        echo "📋 Showing file processor logs..."
        docker-compose logs -f file-processor
        ;;
    status)
        echo "📊 Service status:"
        docker-compose ps
        ;;
    health)
        echo "🏥 Health status:"
        echo "File Processor:"
        docker inspect --format='{{json .State.Health}}' file-processor | jq
        ;;
    build)
        echo "🏗️ Building services..."
        docker-compose build
        echo "✅ Build complete"
        ;;
    build-processor)
        echo "🏗️ Building file processor..."
        docker-compose build file-processor
        echo "✅ File processor build complete"
        ;;
    shell-processor)
        echo "🐚 Opening shell in file processor container..."
        docker exec -it file-processor bash
        ;;
    clean)
        echo "🧹 Cleaning up unused Docker resources..."
        docker system prune -f
        docker volume prune -f
        echo "✅ Cleanup complete"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|restart-processor|logs|logs-processor|status|health|build|build-processor|shell-processor|clean}"
        echo ""
        echo "Commands:"
        echo "  start              - Start all services"
        echo "  stop               - Stop all services"
        echo "  restart            - Restart all services"
        echo "  restart-processor  - Restart only file processor"
        echo "  logs               - Show logs for all services"
        echo "  logs-processor     - Show logs for file processor only"
        echo "  status             - Show service status"
        echo "  health             - Show health status"
        echo "  build              - Build all services"
        echo "  build-processor    - Build file processor only"
        echo "  shell-processor    - Open shell in processor container"
        echo "  clean              - Clean up Docker resources"
        exit 1
        ;;
esac
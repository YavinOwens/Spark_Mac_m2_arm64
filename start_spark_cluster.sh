#!/bin/bash

echo "============================================================"
echo "STARTING SPARK DOCKER CLUSTER"
echo "============================================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Stop any existing containers
echo "🛑 Stopping any existing Spark containers..."
docker-compose down

# Start the cluster
echo "🚀 Starting Spark cluster..."
docker-compose up -d

# Wait for containers to be ready
echo "⏳ Waiting for containers to be ready..."
sleep 10

# Check container status
echo "📊 Container status:"
docker-compose ps

# Check if containers are running
if docker-compose ps | grep -q "Up"; then
    echo "✅ Spark cluster is running!"
    echo ""
    echo "🌐 Access URLs:"
    echo "  - Spark Master UI: http://localhost:8080"
    echo "  - Spark Worker UI: http://localhost:8081"
    echo ""
    echo "🔧 To test cluster mode, run:"
    echo "  python -m pyspark_cluster.session_comparison"
    echo ""
    echo "🛑 To stop the cluster:"
    echo "  docker-compose down"
else
    echo "❌ Failed to start Spark cluster. Check the logs:"
    echo "  docker-compose logs"
fi 
#!/bin/bash

set -e

echo "============================================================"
echo "STARTING LOCAL SPARK CLUSTER"
echo "============================================================"

# Stop any running Spark master/worker
$SPARK_HOME/sbin/stop-worker.sh || true
$SPARK_HOME/sbin/stop-master.sh || true

# Start Spark master
$SPARK_HOME/sbin/start-master.sh

# Start Spark worker (connects to master at localhost:7077)
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

sleep 2

echo ""
echo "🌐 Spark Master UI: http://localhost:8080"
echo "🌐 Spark Worker UI: http://localhost:8081"
echo ""
echo "To stop the cluster, run:"
echo "  $SPARK_HOME/sbin/stop-worker.sh && $SPARK_HOME/sbin/stop-master.sh" 
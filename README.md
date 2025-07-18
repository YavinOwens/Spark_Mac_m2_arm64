# Spark Cluster with Jupyter Lab

A complete Apache Spark cluster setup with Jupyter Lab for interactive data processing and analysis.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB of available RAM
- Ports 8080, 8081, 18080, and 8888 available

### Startup Instructions

1. **Clone or navigate to the project directory:**
   ```bash
   cd work_database_cleanup
   ```

2. **Start the Spark cluster:**
   ```bash
   docker compose up -d
   ```

3. **Wait for all services to start (about 30-60 seconds)**

4. **Access the services using the URLs below**

## 🔗 Access URLs & Credentials

### Jupyter Lab
- **URL:** http://localhost:8888
- **Token:** `7631a38004d31b08a26dcad9d6b015309d0d96c536bbbc59`
- **Direct Access:** http://localhost:8888/lab?token=7631a38004d31b08a26dcad9d6b015309d0d96c536bbbc59

### Spark Master UI
- **URL:** http://localhost:8080
- **Purpose:** Monitor Spark cluster, view applications, and manage workers

### Spark Worker UI
- **URL:** http://localhost:8081
- **Purpose:** Monitor individual worker node status and resources

### Spark History Server
- **URL:** http://localhost:18080
- **Purpose:** View completed Spark jobs and their execution details

## 📊 Services Overview

| Service | Port | Status | Purpose |
|---------|------|--------|---------|
| Spark Master | 8080 | ✅ Running | Cluster management and coordination |
| Spark Worker | 8081 | ✅ Running | Data processing nodes |
| Spark History Server | 18080 | ✅ Running | Job monitoring and history |
| Jupyter Lab | 8888 | ✅ Running | Interactive development environment |

## 🛠️ Development with PySpark

### Connecting to Spark from Jupyter

1. **Open Jupyter Lab** using the URL and token above
2. **Create a new Python notebook**
3. **Connect to Spark cluster:**

```python
from pyspark.sql import SparkSession

# Create Spark session connected to the cluster
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Your PySpark code here
df = spark.read.csv("/path/to/data.csv")
df.show()
```

### Example PySpark Code

```python
# Read data
df = spark.read.csv("/home/jovyan/data/sample.csv", header=True)

# Transform data
result = df.groupBy("column_name").count()

# Show results
result.show()

# Save results
result.write.mode("overwrite").parquet("/home/jovyan/data/output")
```

## 📁 Directory Structure

```
work_database_cleanup/
├── docker-compose.yml          # Cluster configuration
├── Dockerfile                  # Custom Spark image
├── notebooks/                  # Jupyter notebooks (mounted)
├── data/                       # Data files (mounted)
├── jars/                       # Spark JAR files
└── README.md                   # This file
```

## 🔧 Configuration

### Resource Limits
- **Spark Master:** 1 CPU, 2GB RAM
- **Spark Worker:** 1 CPU, 2GB RAM
- **Jupyter:** No limits (uses host resources)

### Volumes
- `./notebooks` → `/home/jovyan/work` (Jupyter notebooks)
- `./data` → `/home/jovyan/data` (Data files)
- `spark-work` → `/opt/bitnami/spark/work` (Spark worker data)
- `spark-events` → `/tmp/spark-events` (Spark history)

## 🛑 Stopping the Cluster

```bash
# Stop all services
docker compose down

# Stop and remove volumes (WARNING: deletes data)
docker compose down -v
```

## 🔍 Troubleshooting

### Check Service Status
```bash
# View all running containers
docker ps

# Check specific service logs
docker logs spark-master
docker logs spark-worker
docker logs jupyter
docker logs spark-history-server
```

### Common Issues

1. **Port conflicts:** Ensure ports 8080, 8081, 18080, 8888 are available
2. **Memory issues:** Increase Docker memory allocation in Docker Desktop
3. **Container name conflicts:** Remove existing containers with `docker rm <container-name>`

### Reset Everything
```bash
# Stop and remove everything
docker compose down -v
docker system prune -a --volumes

# Restart fresh
docker compose up -d
```

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Jupyter Lab Documentation](https://jupyterlab.readthedocs.io/)

## 🔐 Security Notes

- The Jupyter token is generated automatically and changes on each restart
- To get the current token: `docker logs jupyter | grep token`
- Consider setting up authentication for production use
- All services are accessible only from localhost by default

## 📈 Monitoring

- **Cluster Health:** Check Spark Master UI at http://localhost:8080
- **Job History:** View completed jobs at http://localhost:18080
- **Worker Status:** Monitor workers at http://localhost:8081
- **Container Logs:** Use `docker logs <container-name>` for debugging

---

**Happy Sparking! 🚀** 
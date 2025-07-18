## Spark UI Access

- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
- **Spark Worker UI:** [http://localhost:8081](http://localhost:8081)

These URLs allow you to monitor the status of your Spark cluster, jobs, and workers.

## Using Spark with Jupyter Lab

1. **Start Jupyter Lab:**
   ```sh
   source spark_env/bin/activate
   jupyter lab
   ```
   (Or, to ensure no environment variable issues:)
   ```sh
   JAVA_TOOL_OPTIONS= jupyter lab
   ```

2. **Access Jupyter Lab:**
   - Open your browser to the URL printed in the terminal (e.g., [http://localhost:8888/lab](http://localhost:8888/lab) or [http://localhost:8889/lab](http://localhost:8889/lab)).

3. **Run Spark Scripts:**
   - Open a new notebook and run:
     ```python
     %run spark_demo_full.py
     ```
   - Or copy-paste code into notebook cells and execute.

4. **Cluster Mode:**
   - The demo script is configured to connect to the Spark master at `spark://localhost:7077`.
   - Ensure your Spark master and worker are running locally (see above URLs).

5. **Troubleshooting:**
   - If you see errors about `JAVA_TOOL_OPTIONS`, make sure to start Jupyter Lab with `JAVA_TOOL_OPTIONS=` as shown above.
   - For import errors, ensure your project root is in `PYTHONPATH` or run from the project directory. 

## Using Spark and Jupyter Lab with Docker

1. **Build Docker Images:**
   ```sh
   docker-compose build
   ```

2. **Start the Spark Cluster and Jupyter Lab:**
   ```sh
   ./start_spark_cluster.sh
   ```
   This will start Spark master, worker(s), and Jupyter Lab in containers.

3. **Access Spark UIs:**
   - **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
   - **Spark Worker UI:** [http://localhost:8081](http://localhost:8081)

4. **Access Jupyter Lab:**
   - Open your browser to the URL printed in the terminal, e.g.:
     - [http://localhost:8888/lab?token=...](http://localhost:8888/lab?token=...)
   - If prompted for a token, copy it from the terminal output (look for `token=...` in the URL).

5. **Run Spark Scripts in Jupyter:**
   - Open a new notebook and run:
     ```python
     %run spark_demo_full.py
     ```
   - Or copy-paste code into notebook cells and execute.

6. **Cluster Mode:**
   - The demo script is configured to connect to the Spark master at `spark://spark-master:7077` (inside Docker).
   - Ensure your Spark master and worker containers are running (see above URLs).

7. **Stopping the Cluster:**
   ```sh
   docker-compose down
   ```

8. **Troubleshooting:**
   - If you see errors about `JAVA_TOOL_OPTIONS`, ensure your Docker images are rebuilt after any environment changes.
   - If you cannot access Jupyter Lab, check the terminal for the correct port and token.
   - For import errors, ensure your code is mounted into the Jupyter container (see `docker-compose.yml`). 
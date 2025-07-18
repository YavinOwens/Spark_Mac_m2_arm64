FROM bitnami/spark:3.5.0

USER root

# NOTE: This image uses Python 3.11.9
# Your host Python version should match for cluster mode
# Check with: python3 --version

# Install pip and verify Python version
RUN install_packages python3-pip curl && \
    python3 --version && \
    python3 -m pip install --upgrade pip

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt

# Copy application code
COPY pyspark_cluster/ /opt/pyspark_cluster/
COPY *.py /opt/
COPY *.ipynb /opt/
COPY *.sh /opt/

# Set Python path to include the application code
ENV PYTHONPATH=/opt:/opt/pyspark_cluster:$PYTHONPATH
# Set PySpark Python executables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set JAVA_HOME for Spark and PySpark
ENV JAVA_HOME=/opt/bitnami/java

# Set working directory
WORKDIR /opt

USER 1001 
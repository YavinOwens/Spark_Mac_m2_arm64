#!/bin/bash

# =============================================================
# Interactive Spark + PySpark + Docker Environment Setup Script
# For Mac (Intel/Apple Silicon) and Windows (WSL/Native)
# =============================================================
# This script will:
#   - Prompt for an install directory
#   - Download and extract Apache Spark
#   - Install Python requirements (via pip)
#   - Install Docker Desktop
#   - Clone/download the project repo
#   - Guide the user through any manual steps
#   - Annotate each step for clarity
#
# NOTE: This script is safe to run and will NOT overwrite existing files without confirmation.
#       It is interactive and will prompt before each major action.
# =============================================================
# this section was written by AI replicating the steps i performed on my system.  please go through this script and ensure it is correct before using it.
# if you see any issues, please fix them.  
# if you see any improvements, please make them.
# if you see any questions, please ask me.
# if you see any other changes, please make them.
# if you see any other issues, please fix them.

set -e

# --------- Helper Functions ---------
function prompt_continue() {
    read -p "Press Enter to continue or Ctrl+C to abort..."
}

function prompt_path() {
    echo "\nWhere would you like to install/download everything?"
    read -e -p "Enter full path (default: current directory): " INSTALL_PATH
    # If no input, use current directory
    if [ -z "$INSTALL_PATH" ]; then
        INSTALL_PATH="$(pwd)"
        echo "No path provided. Using current directory: $INSTALL_PATH"
    else
        mkdir -p "$INSTALL_PATH"
        echo "Using install path: $INSTALL_PATH"
    fi
}

function detect_os() {
    OS="unknown"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS="mac"
    elif [[ "$OSTYPE" == "linux"* ]]; then
        if grep -qi microsoft /proc/version 2>/dev/null; then
            OS="wsl"
        else
            OS="linux"
        fi
    elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* ]]; then
        OS="windows"
    fi
    echo "Detected OS: $OS"
}

# --------- Main Script ---------
echo "============================================================"
echo "  Spark + PySpark + Docker Environment Setup"
echo "============================================================"
echo "This script will help you set up everything needed for a Spark/PySpark workflow on Mac or Windows."
echo "You will be prompted before each major step."
echo "============================================================"

prompt_path
cd "$INSTALL_PATH"

detect_os

# --------- Step 1: Download Apache Spark ---------
echo "\n[1/5] Downloading Apache Spark..."
echo "You can choose the Spark version. Default is 3.5.1 (recommended for most workflows)."
read -e -p "Enter Spark version (default: 3.5.1): " SPARK_VERSION
SPARK_VERSION=${SPARK_VERSION:-3.5.1}
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="${SPARK_PACKAGE}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

if [ -d "$INSTALL_PATH/$SPARK_PACKAGE" ]; then
    echo "Spark already downloaded at $INSTALL_PATH/$SPARK_PACKAGE"
else
    echo "Downloading Spark from $SPARK_URL ..."
    curl -L -o "$SPARK_TGZ" "$SPARK_URL"
    echo "Extracting..."
    tar -xzf "$SPARK_TGZ"
    rm "$SPARK_TGZ"
    echo "Spark extracted to $INSTALL_PATH/$SPARK_PACKAGE"
fi
prompt_continue

# --------- Step 2: Install Python & Requirements ---------
echo "\n[2/5] Installing Python requirements..."
echo "This will use pip to install requirements.txt."
if [ -f "$INSTALL_PATH/requirements.txt" ]; then
    REQ_PATH="$INSTALL_PATH/requirements.txt"
else
    REQ_PATH="requirements.txt"
fi
if [ ! -f "$REQ_PATH" ]; then
    echo "requirements.txt not found. Downloading from project repo..."
    curl -O https://raw.githubusercontent.com/YavinOwens/Spark_Mac_m2_arm64/main/requirements.txt
    REQ_PATH="requirements.txt"
fi
python3 -m pip install --upgrade pip
python3 -m pip install -r "$REQ_PATH"
prompt_continue

# --------- Step 3: Install Docker Desktop ---------
echo "\n[3/5] Installing Docker Desktop..."
if [[ "$OS" == "mac" ]]; then
    echo "Opening Docker Desktop download page for Mac..."
    open https://www.docker.com/products/docker-desktop/
    echo "Please download and install Docker Desktop manually if not already installed."
elif [[ "$OS" == "windows" || "$OS" == "wsl" ]]; then
    echo "Opening Docker Desktop download page for Windows..."
    if command -v explorer.exe >/dev/null; then
        explorer.exe https://www.docker.com/products/docker-desktop/
    else
        echo "Please open https://www.docker.com/products/docker-desktop/ in your browser."
    fi
else
    echo "Please download Docker Desktop for your platform from https://www.docker.com/products/docker-desktop/"
fi
echo "After installing Docker Desktop, start it and ensure it is running."
prompt_continue

# --------- Step 4: Clone Project Repository ---------
echo "\n[4/5] Downloading project files..."
REPO_URL="https://github.com/YavinOwens/Spark_Mac_m2_arm64.git"
if [ -d "$INSTALL_PATH/Spark_Mac_m2_arm64" ]; then
    echo "Project already cloned at $INSTALL_PATH/Spark_Mac_m2_arm64"
else
    git clone "$REPO_URL"
    echo "Project cloned to $INSTALL_PATH/Spark_Mac_m2_arm64"
fi
prompt_continue

# --------- Step 5: Final Instructions ---------
echo "\n[5/5] Setup complete!"
echo "To start the Spark cluster and Jupyter Lab, run:"
echo "  cd $INSTALL_PATH/Spark_Mac_m2_arm64"
echo "  docker-compose up -d"
echo "  # Then open http://localhost:8888 in your browser for Jupyter Lab"
echo "  # Spark UI: http://localhost:8080 (master), http://localhost:8081 (worker)"
echo "\nTo run a demo script inside the Jupyter container:"
echo "  docker exec -it jupyter bash"
echo "  cd /home/jovyan/work"
echo "  python3 spark_demo_full_cluster.py"
echo "\nFor more details, see SPARK_README.md in the project folder."
echo "============================================================"
echo "Setup script finished!"
echo "============================================================" 
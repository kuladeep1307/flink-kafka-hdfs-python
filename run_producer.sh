#!/bin/bash

set -e  # Exit on any error

VENV_DIR="./venv"

# Check if venv exists; if not, create it
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "$VENV_DIR"
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Check if confluent-kafka is installed; install if not
if ! pip show confluent-kafka &>/dev/null; then
  echo "Installing confluent-kafka package..."
  pip install --upgrade pip
  pip install confluent-kafka
fi

# Run the Python producer script
echo "Starting Kafka producer..."
python3 kafka_high_throughput_producer.py

# Deactivate venv on exit (optional)
deactivate

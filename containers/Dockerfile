# Declare the Python version as an argument
ARG py_version=3.9

# Use the specified Python version as the base image
FROM python:${py_version}-slim

# Install system prerequisites
RUN apt-get update && \
    apt-get install -y --no-install-recommends rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file
COPY containers/requirements.txt /containers/requirements.txt
COPY jobs/direct_runner_transactions.py /jobs/direct_runner_transactions.py
COPY jobs/unit_test_transactions.py /jobs/unit_test_transactions.py
COPY jobs/run_in_docker.sh /jobs/run_in_docker.sh
RUN ls -l /jobs/
RUN chmod +x /jobs/run_in_docker.sh



# Install Apache Beam with GCP extras
RUN pip install --no-cache-dir -r /containers/requirements.txt

# Define entry point
CMD ["/jobs/run_in_docker.sh"]
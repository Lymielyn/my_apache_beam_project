# Apache Beam pipeline using Direct Runner
A repository to store all the materials of a Apache Beam pipeline

## Table of Contents
- [Installation](#installation)
- [Cloning the repository](#cloning)
- [Usage](#usage)

## Installation

If you are intending to run this in using a Docker container, please ensure that you have Docker Desktop installed in your local machine. For more information on how to install Docker, please [click this link](https://www.docker.com/products/docker-desktop/)

Windows users: please setup WSL and a local Ubuntu Virtual machine following `[the instructions here](https://documentation.ubuntu.com/wsl/en/latest/#1-overview)`. Install the above prerequisites on your ubuntu terminal; if you have trouble installing docker, follow `[the steps here](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-22-04#step-1-installing-docker)` (only Step 1 is necessary). Please install the make command with sudo apt install make -y (if its not already present).

## Cloning the repository:
   ```bash
   git clone https://github.com/Lymielyn/my_apache_beam_project.git
   cd my_apache_beam_project
   ```

## Usage
### Run pipeline locally

Users can run the pipeline locally by executing the run_me.sh file which will install the required modules and libraries and then execute the Apache Beam pipeline
   ```bash
   cd jobs
   run_me.sh
   ```
### Run pipeline in Docker container

Open a terminal in your Docker Desktop and use the command below to run the pipeline.
   ```
   cd my_apache_beam_project
   docker build -t beam_transactions_pipeline -f containers/Dockerfile . # Build the Docker image
   docker image ls # View image
   docker run beam_transactions_pipeline # Run pipeline
   ```

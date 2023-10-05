# TDT4225 - Very Large, Distributed Data Volumes: Assignment 2

## Introduction
This repository contains the code and resources for Assignment 2 of the TDT4225 course at NTNU. It focuses on working with very large, distributed data volumes using MySQL and Python.

## Prerequisites

### Software
- make
- docker

### Data
- Dataset: Ensure the required dataset is placed in the project folder

## Getting started
### Clone the Repository
```bash
git clone https://github.com/Kem0sabe/TDT4225-assigment-2.git
```

### Navigate to Project Directory
```bash
cd TDT4225-assigment-2
```

### Run Setup and Start Services
We use a Makefile for automating various tasks like setting up the virtual environment, starting the database, and running the queries.

Before proceeding, ensure that Docker is up and running.

To run the entire setup and start all services:
```bash
make setup
```

### Running Queries After Initial Setup
If you have already run the initial setup and wish to run queries, you can do so with the following command:
```bash
make queries
```
Note: The initial setup must be completed before running queries.

### Shutting Down and Cleanup
To shut down the generated Docker container and remove any automatically generated Python files, use:
```bash
make down
```

## Tasks
- Part 1: Data Cleaning and Insertion
- Part 2: Queries and Analysis
- Part 3: Report

For more details on each part and the tasks involved, please refer to the [assignment PDF]([http://example.com/path/to/your/assignment.pdf](https://github.com/Kem0sabe/TDT4225-assignment-2/blob/main/Assignment-2-2023.pdf)).



### Makefile Commands
- make create-env: Create a Python virtual environment
- make remove-env: Remove the Python virtual environment
- make queries: Run main.py to execute queries
- make db: Start the database using Docker Compose
- make install: Install requirements into the virtual environment
- make down: Tear down the Docker containers and remove Python venv
- make init_env_and_db: Run the init script
- make setup: Start all services

For a complete list of available commands, run make help.




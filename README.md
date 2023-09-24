# TDT4225 - Very Large, Distributed Data Volumes: Assignment 2

## Introduction
This repository contains the code and resources for Assignment 2 of the TDT4225 course at NTNU. It focuses on working with very large, distributed data volumes using MySQL and Python.

## Prerequisites
- make
- docker

## Getting started
### Clone the Repository
```bash
git clone https://github.com/Kem0sabe/TDT4225-project1.git
```

### Navigate to Project Directory
```bash
cd TDT4225-project1
```

### Run Setup and Start Services
We use a Makefile for automating various tasks like setting up the virtual environment, starting the database, and running the queries.

To run the entire setup and start all services:
```bash
make start
```

### Running Queries After Initial Setup
If you have already run the initial setup and wish to run queries, you can do so with the following command:
```bash
make queries
```
Note: This will perform the setup if it hasn't been done already.

## Tasks
- Part 1: Data Cleaning and Insertion
- Part 2: Queries and Analysis
- Part 3: Report

For more details on each part and the tasks involved, please refer to the assignment PDF.

### Makefile Commands
- make create-env: Create a Python virtual environment
- make remove-env: Remove the Python virtual environment
- make queries: Run main.py to execute queries
- make db: Start the database using Docker Compose
- make install: Install project requirements into the virtual environment
- make down: Tear down the Docker containers
- make setup: Run the setup script
- make start: Start all services

For a complete list of available commands, run make help.




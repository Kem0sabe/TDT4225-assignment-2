# Makefile for managing project tasks

# List of phony targets
.PHONY: create-env clean remove-env queries db pip_install down init_db setup help

VENV_NAME=myenv

ifeq ($(OS),Windows_NT)
    SYSTEM=Windows_NT
    VENV_ACTIVATE=.\\$(VENV_NAME)\\Scripts\\activate
    PYTHON=$(VENV_NAME)\\Scripts\\python.exe
    PIP=$(VENV_NAME)\\Scripts\\pip.exe
else
    SYSTEM=$(shell uname -s)
    VENV_ACTIVATE=./$(VENV_NAME)/bin/activate
    PYTHON=$(VENV_NAME)/bin/python3
    PIP=$(VENV_NAME)/bin/pip3
endif

# Create a Python virtual environment
create-env: ## Create a Python virtual environment
	@echo "Creating virtual environment..."
	test -d $(VENV_NAME) || python3 -m venv $(VENV_NAME)

# Cleanup
clean: ## Remove __pycache__ directory
	ifeq ($(SYSTEM),Windows_NT)
		@echo "Removing byte-compiled python files..."
		if exist "__pycache__" rd /s /q "__pycache__"
	else
		@echo "Removing byte-compiled python files..."
		rm -rf __pycache__
	endif

# Remove the Python virtual environment
remove-env: clean ## Remove the Python virtual environment
	@echo "Removing virtual environment..."
	rm -rf $(VENV_NAME)

# Run main.py
queries: ## Run the queries
	@echo "Running main.py..."
	$(VENV_NAME)/bin/python3 main.py

# Start the database using Docker Compose
db: ## Start the database using Docker Compose
	@echo "Starting the database..."
	docker-compose up -d

# Install project requirements into virtual environment
pip_install: create-env ## Install project requirements
	@echo "Installing project requirements..."
	$(VENV_NAME)/bin/pip3 install -r requirements.txt

# Tear down the Docker containers
down: remove-env ## Tear down Docker containers
	@echo "Tearing down the Docker containers..."
	docker-compose down

# Run init using main.py
init_db: create-env ## Run the init script
	@echo "Running init..."
	$(VENV_NAME)/bin/python3 DbMaker.py
	@echo "Init completed."

# Start all services
setup: db pip_install ## Start all services
	@echo "Pausing for the database to initialize..."
	sleep 10
	$(MAKE) init_db
	@echo "Starting all services..."

# List all available make commands
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# Makefile for managing project tasks

VENV_NAME=myenv

# Create a Python virtual environment
create-env: ## Create a Python virtual environment
	@echo "Creating virtual environment..."
	test -d $(VENV_NAME) || python3 -m venv $(VENV_NAME)

# Remove the Python virtual environment
remove-env: ## Remove the Python virtual environment
	@echo "Removing virtual environment..."
	rm -rf $(VENV_NAME)

# Run main.py
queries: ## Run the main script
	@echo "Running main.py..."
	$(VENV_NAME)/bin/python3 main.py

# Start the database using Docker Compose
db: ## Start the database using Docker Compose
	@echo "Starting the database..."
	docker-compose up -d

# Install project requirements into virtual environment
install: create-env ## Install project requirements
	@echo "Installing project requirements..."
	$(VENV_NAME)/bin/pip3 install -r requirements.txt

# Tear down the Docker containers
down: ## Tear down Docker containers
	@echo "Tearing down the Docker containers..."
	docker-compose down

# Run setup using main.py
setup: create-env ## Run the setup script
	@echo "Running setup..."
	$(VENV_NAME)/bin/python3 DbMaker.py
	@echo "Setup completed."

# Start all services
start: db ## Start all services
	@echo "Pausing for the database to initialize..."
	sleep 10 # Wait for 10 seconds
	$(MAKE) install
	$(MAKE) setup
	@echo "Starting all services..."

# List all available make commands
help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

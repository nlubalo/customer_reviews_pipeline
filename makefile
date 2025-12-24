# Variables
COMPOSE=docker compose
WEB=airflow-webserver
SCHEDULER=airflow-scheduler
DB=postgres
JUPYTER_PORT=8888
AIRFLOW_HOME=/opt/airflow

.PHONY: help init up down bash jupyter logs
help:
	@echo "Makefile commands:"
	@echo "  init          Initialize the Airflow database and create admin user"
	@echo "  up            Start the Airflow services"
	@echo "  down          Stop the Airflow services"
	@echo "  bash          Open a bash shell in the Airflow webserver container"
	@echo "  jupyter       Start JupyterLab server"
	@echo "  logs          View logs of the Airflow webserver"

init:
	$(COMPOSE) up -d $(DB)
	sleep 10
	$(COMPOSE) run --rm $(WEB) airflow db init
	$(COMPOSE) run --rm $(WEB) airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

# Start all services
up:
	@$(COMPOSE) up -d $(DB)
	@$(COMPOSE) up -d $(WEB) $(SCHEDULER)

# ------------------- Stop all services
down:
	@$(COMPOSE) down

# ----------------------------------------
# Bash into Airflow webserver
bash:
	@$(COMPOSE) exec $(WEB) bash

# ----------------------------------------
# Run Jupyter Lab inside webserver container
jupyter:
	@$(COMPOSE) exec $(WEB) jupyter lab \
		--ip=0.0.0.0 \
		--port=$(JUPYTER_PORT) \
		--no-browser \
		--allow-root

# ----------------------------------------
# Show webserver logs
logs:
	@$(COMPOSE) logs -f $(WEB)

# Update this
reset:
	docker compose down -v
	docker compose build --no-cache
	docker compose up -d
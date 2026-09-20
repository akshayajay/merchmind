.PHONY: install format lint test demo api dashboard compose streaming-test

install:
	pip install -e '.[dev]'

format:
	ruff format .
	ruff check --fix .

lint:
	ruff format --check .
	ruff check .

test:
	pytest

demo:
	merchmind run --transactions 50000 --customers 2500 --products 500

api:
	uvicorn merchmind.api:app --reload

dashboard:
	streamlit run src/merchmind/dashboard.py

compose:
	docker compose up --build

streaming-test:
	docker compose --profile streaming run -T --build --rm streaming-test

.PHONY: airflow airflow-test
airflow:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build airflow

airflow-test:
	docker compose -f docker-compose.yml -f docker-compose.airflow.yml run -T --build --rm airflow-test

.PHONY: retail-demo retail-stop retail-status retail-logs compose-check
RETAIL_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.airflow.yml -f docker-compose.inventory.yml
retail-demo:
	$(RETAIL_COMPOSE) --profile streaming --profile inventory up -d --build api dashboard replay spark refresh inventory-replay inventory-spark stock-refresh airflow

retail-stop:
	$(RETAIL_COMPOSE) --profile streaming --profile inventory stop

retail-status:
	$(RETAIL_COMPOSE) --profile streaming --profile inventory ps

retail-logs:
	$(RETAIL_COMPOSE) logs --tail 100 airflow

compose-check:
	$(RETAIL_COMPOSE) --profile streaming --profile inventory config --quiet

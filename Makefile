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

.PHONY: help setup up down receiver demo test replay report sweep lint fmt clean

PYTHON ?= python3.11
VENV ?= .venv
BIN = $(VENV)/bin

help:
	@echo "Rekon: Streaming Payment Reconciliation Engine"
	@echo "=============================================="
	@echo "Available commands:"
	@echo "  make setup     - Create virtual environment and install dependencies"
	@echo "  make up        - Start Docker services (Redpanda Kafka broker)"
	@echo "  make down      - Stop Docker services"
	@echo "  make receiver  - Start the FastAPI webhook receiver locally on port 8000"
	@echo "  make demo      - Run full end-to-end simulated reconciliation demo"
	@echo "  make test      - Run all unit, integration, and chaos scenario tests"
	@echo "  make replay    - Wipe checkpoints and replay from Bronze to verify idempotency"
	@echo "  make report    - Show status report of Gold order reconciliation & anomalies"
	@echo "  make sweep     - Run periodic reconciliation sweep (window expiry & OPTIMIZE)"
	@echo "  make lint      - Run ruff linter"
	@echo "  make fmt       - Format Python source files"
	@echo "  make clean     - Clean local Delta tables, checkpoints, logs, and caches"

setup:
	@if command -v uv >/dev/null 2>&1; then \
		uv venv --python $(PYTHON) $(VENV); \
		uv pip install --python $(BIN)/python -e ".[dev]"; \
	else \
		$(PYTHON) -m venv $(VENV); \
		$(BIN)/pip install --upgrade pip; \
		$(BIN)/pip install -e ".[dev]"; \
	fi
	@echo "Setup complete. Activate environment with: source $(VENV)/bin/activate"

up:
	docker compose up -d
	@echo "Waiting for Redpanda Kafka broker to become healthy..."
	@until docker compose exec -T redpanda rpk cluster health | grep -i 'healthy:.*true' >/dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "Creating Kafka topics: orders.created, payments.mpesa.raw, payments.bank.raw..."
	docker compose exec -T redpanda rpk topic create orders.created payments.mpesa.raw payments.bank.raw -p 3 -r 1 || true
	@echo "Redpanda Kafka cluster is ready on localhost:9092."

down:
	docker compose down

receiver:
	$(BIN)/uvicorn receiver.app:app --host 0.0.0.0 --port 8000 --reload

demo:
	$(BIN)/python -m simulator.cli demo

test:
	$(BIN)/pytest tests/

replay:
	$(BIN)/python -m streaming.replay

report:
	$(BIN)/python -m streaming.report

sweep:
	$(BIN)/python -m streaming.sweep

lint:
	$(BIN)/ruff check .

fmt:
	$(BIN)/ruff format .

clean:
	rm -rf data/ checkpoints/ spark-warehouse/ metastore_db/ derby.log .pytest_cache/ .ruff_cache/ *.log

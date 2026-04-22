.DEFAULT_GOAL := help
PYTHON        := python3.11
VENV          := .venv
PIP           := $(VENV)/bin/pip
DBT           := $(VENV)/bin/dbt
PYTEST        := $(VENV)/bin/pytest

.PHONY: install
install: ## Create virtual environment and install all dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "\n✅ Environment ready. Activate with: source $(VENV)/bin/activate"

.PHONY: env
env: ## Copy .env.example to .env (only if .env doesn't exist yet)
	@[ -f .env ] && echo "⚠️  .env already exists — skipping." || (cp .env.example .env && echo "✅ .env created. Fill in your credentials.")

.PHONY: run
run: ## Run the ingestion pipeline for yesterday (reads .env automatically)
	@export $$(grep -v '^#' .env | xargs) && \
	$(PYTHON) -m ingestion.pipeline

.PHONY: run-date
run-date: ## Run pipeline for a specific date: make run-date DATE=2024-06-01
	@[ -n "$(DATE)" ] || (echo "❌ Usage: make run-date DATE=YYYY-MM-DD" && exit 1)
	@export $$(grep -v '^#' .env | xargs) && \
	$(PYTHON) -m ingestion.pipeline --date $(DATE)

.PHONY: dry-run
dry-run: ## Run pipeline without writing to GCS or BigQuery (safe for local testing)
	@export $$(grep -v '^#' .env | xargs) && \
	$(PYTHON) -m ingestion.pipeline --dry-run

.PHONY: test
test: ## Run full test suite with coverage report
	$(PYTEST) tests/ -v --tb=short \
		--cov=ingestion \
		--cov-report=term-missing \
		--cov-report=html:.coverage-report

.PHONY: test-connectors
test-connectors: ## Run only connector tests (fast, no network calls)
	$(PYTEST) tests/ingestion/connectors/ -v --tb=short

.PHONY: test-ci
test-ci: ## Run tests in CI mode (no coverage HTML, fail fast)
	$(PYTEST) tests/ -v --tb=short -x

.PHONY: dbt-deps
dbt-deps: ## Install dbt packages (dbt deps)
	cd dbt_project && $(DBT) deps

.PHONY: dbt-run
dbt-run: ## Run all dbt models (bronze → silver → gold)
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt_project && $(DBT) run --profiles-dir .

.PHONY: dbt-test
dbt-test: ## Run all dbt tests (schema + singular)
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt_project && $(DBT) test --profiles-dir .

.PHONY: dbt-build
dbt-build: ## dbt run + dbt test in a single command
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt_project && $(DBT) build --profiles-dir .

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs on http://localhost:8080
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt_project && $(DBT) docs generate --profiles-dir . && \
	$(DBT) docs serve --port 8080 --profiles-dir .

.PHONY: dbt-lint
dbt-lint: ## Compile all models to catch syntax errors without running them
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt_project && $(DBT) compile --profiles-dir .

.PHONY: docker-build
docker-build: ## Build the Docker image
	docker build -f docker/Dockerfile -t raw-to-gold:local .

.PHONY: docker-run
docker-run: ## Run the pipeline inside Docker (uses local .env file)
	docker run --rm --env-file .env raw-to-gold:local

.PHONY: docker-dry-run
docker-dry-run: ## Run the pipeline inside Docker in dry-run mode
	docker run --rm --env-file .env raw-to-gold:local --dry-run

.PHONY: tf-init
tf-init: ## Initialize Terraform (downloads providers)
	cd terraform && terraform init

.PHONY: tf-plan
tf-plan: ## Show Terraform plan without applying changes
	cd terraform && terraform plan -var="gcp_project_id=$$(grep GCP_PROJECT_ID .env | cut -d= -f2)"

.PHONY: tf-apply
tf-apply: ## Apply Terraform changes (creates GCS buckets and BQ tables)
	cd terraform && terraform apply -var="gcp_project_id=$$(grep GCP_PROJECT_ID .env | cut -d= -f2)"

.PHONY: lint
lint: ## Run ruff linter across the codebase
	$(VENV)/bin/ruff check ingestion/ tests/

.PHONY: format
format: ## Format code with ruff formatter
	$(VENV)/bin/ruff format ingestion/ tests/

.PHONY: typecheck
typecheck: ## Run mypy static type checker
	$(VENV)/bin/mypy ingestion/ --ignore-missing-imports


.PHONY: clean
clean: ## Remove virtual environment, cache files and coverage reports
	rm -rf $(VENV) .coverage .coverage-report __pycache__ .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "raw-to-gold-pipeline — available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36mmake %-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
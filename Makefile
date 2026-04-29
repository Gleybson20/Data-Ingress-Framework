.DEFAULT_GOAL := help
PYTHON  := python
VENV    := .venv
PIP     := $(VENV)/Scripts/pip
DBT     := $(VENV)/Scripts/dbt
PYTEST  := $(VENV)/Scripts/pytest

.PHONY: install
install: ## Cria o ambiente virtual e instala as dependências
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Ambiente pronto. Ative com: .venv\\Scripts\\activate"

.PHONY: env
env: ## Copia .env.example para .env (somente se .env não existir)
	@if not exist .env (copy .env.example .env && echo .env criado.) else (echo .env ja existe.)

.PHONY: run
run: ## Ingere dados de ontem no DuckDB
	$(PYTHON) -m ingestion.pipeline

.PHONY: run-date
run-date: ## Ingere dados de uma data específica: make run-date DATE=2024-06-01
	$(PYTHON) -m ingestion.pipeline --date $(DATE)

.PHONY: dry-run
dry-run: ## Testa os conectores sem salvar nada em disco
	$(PYTHON) -m ingestion.pipeline --dry-run

.PHONY: test
test: ## Roda todos os testes com relatório de cobertura
	$(PYTEST) tests/ -v --tb=short --cov=ingestion --cov-report=term-missing

.PHONY: test-connectors
test-connectors: ## Roda apenas os testes de conectores
	$(PYTEST) tests/ -v --tb=short -k "connector"

.PHONY: test-ci
test-ci: ## Modo CI: para na primeira falha, sem HTML
	$(PYTEST) tests/ -v --tb=short -x

.PHONY: dbt-deps
dbt-deps: ## Instala pacotes dbt
	cd dbt_project && $(DBT) deps

.PHONY: dbt-run
dbt-run: ## Executa todos os modelos dbt (bronze → silver → gold)
	cd dbt_project && $(DBT) run --profiles-dir .

.PHONY: dbt-test
dbt-test: ## Executa todos os testes dbt
	cd dbt_project && $(DBT) test --profiles-dir .

.PHONY: dbt-build
dbt-build: ## dbt run + dbt test em sequência
	cd dbt_project && $(DBT) build --profiles-dir .

.PHONY: dbt-docs
dbt-docs: ## Gera e serve a documentação em http://localhost:8080
	cd dbt_project && $(DBT) docs generate --profiles-dir . && $(DBT) docs serve --port 8080 --profiles-dir .

.PHONY: dbt-lint
dbt-lint: ## Verifica sintaxe dos modelos sem executar
	cd dbt_project && $(DBT) compile --profiles-dir .

.PHONY: query-bronze
query-bronze: ## Mostra os últimos registros do Bronze no terminal
	$(PYTHON) -c "import duckdb; con = duckdb.connect('./data/warehouse.duckdb'); print('\n=== weather_daily ==='); print(con.execute('SELECT * FROM bronze.weather_daily ORDER BY date DESC LIMIT 5').df()); print('\n=== exchange_rates_daily ==='); print(con.execute('SELECT * FROM bronze.exchange_rates_daily ORDER BY date DESC LIMIT 5').df())"

.PHONY: query-silver
query-silver: ## Mostra os últimos registros do Silver no terminal
	$(PYTHON) -c "import duckdb; con = duckdb.connect('./data/warehouse.duckdb'); print('\n=== int_weather_daily ==='); print(con.execute('SELECT * FROM silver.int_weather_daily ORDER BY date DESC LIMIT 5').df()); print('\n=== int_exchange_rates_daily ==='); print(con.execute('SELECT * FROM silver.int_exchange_rates_daily ORDER BY date DESC LIMIT 5').df())"

.PHONY: query-gold
query-gold: ## Mostra os KPIs Gold no terminal
	$(PYTHON) -c "import duckdb; con = duckdb.connect('./data/warehouse.duckdb'); print('\n=== fct_weather_monthly_kpis ==='); print(con.execute('SELECT * FROM gold.fct_weather_monthly_kpis ORDER BY month DESC LIMIT 5').df()); print('\n=== fct_exchange_rates_monthly_kpis ==='); print(con.execute('SELECT * FROM gold.fct_exchange_rates_monthly_kpis ORDER BY month DESC LIMIT 5').df())"

.PHONY: lint
lint: ## Verifica estilo e bugs com ruff
	$(VENV)/Scripts/ruff check ingestion/ tests/

.PHONY: format
format: ## Formata o código automaticamente
	$(VENV)/Scripts/ruff format ingestion/ tests/

.PHONY: typecheck
typecheck: ## Verifica tipos estáticos com mypy
	$(VENV)/Scripts/mypy ingestion/ --ignore-missing-imports

.PHONY: clean
clean: ## Remove ambiente virtual, caches e relatórios
	if exist $(VENV) rmdir /s /q $(VENV)
	if exist .coverage del .coverage
	if exist .coverage-report rmdir /s /q .coverage-report
	for /d /r . %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d"

.PHONY: clean-data
clean-data: ## Apaga todos os dados gerados (NDJSON + DuckDB)
	if exist data rmdir /s /q data
	@echo "Pasta data/ removida."

.PHONY: help
help: ## Mostra esta mensagem de ajuda
	@echo.
	@echo raw-to-gold-pipeline (DuckDB) - comandos disponíveis:
	@echo.
	@grep -E "^[a-zA-Z_-]+:.*?## .*$$" $(MAKEFILE_LIST) | awk "BEGIN {FS = \":.*?## \"}; {printf \"  make %-20s %s\n\", $$1, $$2}"
	@echo.
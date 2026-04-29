"""
ingestion/pipeline.py
----------------------
Orquestrador principal: extrai dados das APIs e armazena localmente.

Fluxo
-----
    1. Para cada conector registrado, chama connector.run(target_date)
    2. Serializa os records em NDJSON
    3. Salva o arquivo localmente em data/bronze/<source>/date=YYYY-MM-DD/
    4. Carrega o arquivo no DuckDB (schema bronze)
    5. Emite um relatório estruturado de execução

Estrutura local gerada
----------------------
    data/
    ├── bronze/
    │   ├── open_meteo/date=YYYY-MM-DD/open_meteo.ndjson
    │   └── exchange_rates/date=YYYY-MM-DD/exchange_rates.ndjson
    └── warehouse.duckdb
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from ingestion.connectors.base_connector import BaseConnector, ConnectorError, ConnectorResult
from ingestion.connectors.exchange_rates import ExchangeRatesConnector
from ingestion.connectors.open_meteo import OpenMeteoConnector

logger = logging.getLogger(__name__)

DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "./data"))
DB_PATH: Path  = Path(os.environ.get("DB_PATH",  "./data/warehouse.duckdb"))

_SOURCE_TO_TABLE: dict[str, str] = {
    "open_meteo":     "weather_daily",
    "exchange_rates": "exchange_rates_daily",
}


@dataclass
class ConnectorRun:
    source: str
    status: str
    records_loaded: int
    file_path: str
    error: str | None = None


@dataclass
class PipelineReport:
    target_date: str
    started_at: str
    finished_at: str = ""
    connector_runs: list[ConnectorRun] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        statuses = {r.status for r in self.connector_runs}
        if "failed" in statuses and len(statuses) == 1:
            return "failed"
        if "failed" in statuses or "partial" in statuses:
            return "partial"
        return "success"

    def to_json(self) -> str:
        data = {
            "target_date":    self.target_date,
            "started_at":     self.started_at,
            "finished_at":    self.finished_at,
            "overall_status": self.overall_status,
            "db_path":        str(DB_PATH.resolve()),
            "connector_runs": [asdict(r) for r in self.connector_runs],
        }
        return json.dumps(data, indent=2, ensure_ascii=False)


class BronzePipeline:
    """Orquestra extração → arquivo NDJSON local → DuckDB Bronze."""

    def __init__(
        self,
        target_date: date,
        data_dir: Path = DATA_DIR,
        db_path: Path  = DB_PATH,
        connectors: list[BaseConnector] | None = None,
        dry_run: bool = False,
    ) -> None:
        self._target_date = target_date
        self._data_dir    = Path(data_dir)
        self._db_path     = Path(db_path)
        self._dry_run     = dry_run
        self._connectors: list[BaseConnector] = connectors or [
            OpenMeteoConnector(),
            ExchangeRatesConnector(),
        ]

        if not dry_run:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_duckdb_schemas()

    def run(self) -> PipelineReport:
        report = PipelineReport(
            target_date=self._target_date.isoformat(),
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info(
            "[Pipeline] Iniciando ingestão para %s (%d conectores, dry_run=%s).",
            self._target_date, len(self._connectors), self._dry_run,
        )
        for connector in self._connectors:
            report.connector_runs.append(self._run_connector(connector))

        report.finished_at = datetime.now(timezone.utc).isoformat()
        logger.info("[Pipeline] Concluído. Status: %s", report.overall_status.upper())
        return report

    def _run_connector(self, connector: BaseConnector) -> ConnectorRun:
        source    = connector.source_name
        file_path = self._build_file_path(source)

        try:
            result: ConnectorResult = connector.run(self._target_date)
        except ConnectorError as exc:
            logger.error("[Pipeline] Conector '%s' falhou: %s", source, exc)
            return ConnectorRun(source=source, status="failed",
                                records_loaded=0, file_path=str(file_path), error=str(exc))

        if result.is_empty():
            logger.warning("[Pipeline] Conector '%s' retornou 0 registros.", source)
            return ConnectorRun(source=source, status="partial",
                                records_loaded=0, file_path=str(file_path),
                                error="Connector returned empty result")

        ndjson_bytes = self._serialize(result)

        if self._dry_run:
            logger.info("[Pipeline][DRY RUN] %d registros seriam salvos em %s",
                        len(result), file_path)
            return ConnectorRun(source=source, status="success",
                                records_loaded=len(result), file_path=str(file_path))

        try:
            self._save_ndjson(ndjson_bytes, file_path)
            self._load_to_duckdb(file_path, source)
        except Exception as exc:
            logger.error("[Pipeline] Falha ao salvar '%s': %s", source, exc, exc_info=True)
            return ConnectorRun(source=source, status="failed",
                                records_loaded=0, file_path=str(file_path), error=str(exc))

        return ConnectorRun(source=source, status="success",
                            records_loaded=len(result), file_path=str(file_path))

    @staticmethod
    def _serialize(result: ConnectorResult) -> bytes:
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in result.records]
        return "\n".join(lines).encode("utf-8")

    def _build_file_path(self, source: str) -> Path:
        date_str = self._target_date.isoformat()
        folder   = self._data_dir / "bronze" / source / f"date={date_str}"
        return folder / f"{source}.ndjson"

    def _save_ndjson(self, data: bytes, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(data)
        logger.info("[Pipeline] Arquivo salvo: %s (%d bytes)", file_path, len(data))

    def _init_duckdb_schemas(self) -> None:
        con = duckdb.connect(str(self._db_path))
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.close()

    def _load_to_duckdb(self, file_path: Path, source: str) -> None:
        table_name = _SOURCE_TO_TABLE.get(source)
        if not table_name:
            raise ValueError(f"Nenhum mapeamento de tabela para '{source}'.")

        date_str   = self._target_date.isoformat()
        full_table = f"bronze.{table_name}"
        # Barra invertida não funciona no DuckDB — normaliza para forward slash
        path_str   = str(file_path).replace("\\", "/")

        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {full_table} AS
                SELECT * FROM read_ndjson_auto('{path_str}')
                WHERE 1 = 0
            """)
            con.execute(f"""
                DELETE FROM {full_table}
                WHERE CAST(date AS VARCHAR) = '{date_str}'
            """)
            con.execute(f"""
                INSERT INTO {full_table}
                SELECT * FROM read_ndjson_auto('{path_str}')
            """)
            count = con.execute(
                f"SELECT COUNT(*) FROM {full_table} WHERE CAST(date AS VARCHAR) = '{date_str}'"
            ).fetchone()[0]
            logger.info("[Pipeline] DuckDB: %d registros em %s para %s",
                        count, full_table, date_str)
        finally:
            con.close()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Data inválida '{value}'. Formato esperado: YYYY-MM-DD") from exc


def main() -> None:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Raw-to-Gold Bronze ingestion (DuckDB)")
    parser.add_argument("--date",
                        default=(date.today() - timedelta(days=1)).isoformat(),
                        help="Data alvo YYYY-MM-DD (padrão: ontem)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Executa conectores sem salvar nada em disco")
    args = parser.parse_args()

    pipeline = BronzePipeline(target_date=_parse_date(args.date), dry_run=args.dry_run)
    report   = pipeline.run()
    print(report.to_json())

    if report.overall_status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
"""
ingestion/pipeline.py
----------------------
Main orchestrator: runs all connectors and lands raw data in the Bronze layer.

Flow
----
    1. For each registered connector, call connector.run(target_date)
    2. Serialize ConnectorResult.records to Newline-Delimited JSON (NDJSON)
    3. Upload NDJSON to GCS under the Bronze prefix (partitioned by date)
    4. Load GCS file into BigQuery Bronze table (WRITE_APPEND + date partition)
    5. Emit a structured execution report (success / partial / failure)

Design decisions
----------------
  - GCS is the source of truth for raw data; BQ load job reads from GCS.
    This gives a replayable audit trail and decouples storage from compute.
  - Partitioning key: `date` column (DATE type) — configured in Terraform.
  - Clustering keys per table defined in BQ schema (see terraform/bigquery.tf).
  - A failed connector does NOT abort the pipeline — other connectors still run.
  - The execution report is printed as structured JSON to stdout so GitHub
    Actions / Cloud Logging can parse it without extra tooling.

Environment variables (see .env.example)
-----------------------------------------
    GCP_PROJECT_ID       Google Cloud project ID
    GCS_BUCKET_BRONZE    GCS bucket name for the Bronze layer
    BQ_DATASET_BRONZE    BigQuery dataset for Bronze tables
"""

from __future__ import annotations
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any
from google.cloud import bigquery, storage
from ingestion.connectors.base_connector import BaseConnector, ConnectorError, ConnectorResult
from ingestion.connectors.exchange_rates import ExchangeRatesConnector
from ingestion.connectors.open_meteo import OpenMeteoConnector

logger = logging.getLogger(__name__)

GCP_PROJECT_ID: str = os.environ.get("GCP_PROJECT_ID", "")
GCS_BUCKET_BRONZE: str = os.environ.get("GCS_BUCKET_BRONZE", "")
BQ_DATASET_BRONZE: str = os.environ.get("BQ_DATASET_BRONZE", "bronze")

# Maps connector.source_name → BigQuery target table name
_SOURCE_TO_BQ_TABLE: dict[str, str] = {
    "open_meteo":     "weather_daily",
    "exchange_rates": "exchange_rates_daily",
}


@dataclass
class ConnectorRun:
    """Result summary for a single connector within a pipeline execution."""

    source: str
    status: str          # "success" | "partial" | "failed"
    records_loaded: int
    gcs_uri: str
    error: str | None = None


@dataclass
class PipelineReport:
    """Full execution report emitted at the end of every pipeline run."""

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
            "connector_runs": [asdict(r) for r in self.connector_runs],
        }
        return json.dumps(data, indent=2)

class BronzePipeline:
    """Orchestrates extraction → GCS → BigQuery for all registered connectors.

    Parameters
    ----------
    target_date  : The date whose data should be extracted.
    project_id   : GCP project ID (defaults to env var GCP_PROJECT_ID).
    gcs_bucket   : GCS bucket for Bronze raw files (defaults to env var).
    bq_dataset   : BigQuery Bronze dataset name (defaults to env var).
    connectors   : List of connector instances to run. Defaults to all built-in
                   connectors (OpenMeteo + ExchangeRates).
    dry_run      : If True, skips GCS upload and BQ load — useful for local testing.
    """

    def __init__(
        self,
        target_date: date,
        project_id: str = GCP_PROJECT_ID,
        gcs_bucket: str = GCS_BUCKET_BRONZE,
        bq_dataset: str = BQ_DATASET_BRONZE,
        connectors: list[BaseConnector] | None = None,
        dry_run: bool = False,
    ) -> None:
        self._target_date = target_date
        self._project_id = project_id
        self._gcs_bucket = gcs_bucket
        self._bq_dataset = bq_dataset
        self._dry_run = dry_run

        self._connectors: list[BaseConnector] = connectors or [
            OpenMeteoConnector(),
            ExchangeRatesConnector(),
        ]

        if not dry_run:
            self._gcs_client = storage.Client(project=project_id)
            self._bq_client = bigquery.Client(project=project_id)

    def run(self) -> PipelineReport:
        """Execute the full pipeline for self._target_date.

        Returns
        -------
        PipelineReport
            Structured summary of every connector's outcome.
        """
        report = PipelineReport(
            target_date=self._target_date.isoformat(),
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        logger.info(
            "[Pipeline] Starting Bronze ingestion for %s (%d connectors, dry_run=%s).",
            self._target_date,
            len(self._connectors),
            self._dry_run,
        )

        for connector in self._connectors:
            run_result = self._run_connector(connector)
            report.connector_runs.append(run_result)

        report.finished_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "[Pipeline] Finished. Overall status: %s",
            report.overall_status.upper(),
        )
        return report

    def _run_connector(self, connector: BaseConnector) -> ConnectorRun:
        """Run a single connector through the full extract → load flow.

        Catches all exceptions so one failed connector never aborts the pipeline.
        """
        source = connector.source_name
        gcs_uri = self._build_gcs_uri(source)

        try:
            result: ConnectorResult = connector.run(self._target_date)
        except ConnectorError as exc:
            logger.error("[Pipeline] Connector '%s' extraction failed: %s", source, exc)
            return ConnectorRun(
                source=source,
                status="failed",
                records_loaded=0,
                gcs_uri=gcs_uri,
                error=str(exc),
            )

        if result.is_empty():
            logger.warning("[Pipeline] Connector '%s' returned 0 records.", source)
            return ConnectorRun(
                source=source,
                status="partial",
                records_loaded=0,
                gcs_uri=gcs_uri,
                error="Connector returned empty result",
            )

        ndjson_bytes = self._serialize(result)

        if self._dry_run:
            logger.info(
                "[Pipeline][DRY RUN] Would upload %d records to %s",
                len(result),
                gcs_uri,
            )
            return ConnectorRun(
                source=source,
                status="success",
                records_loaded=len(result),
                gcs_uri=gcs_uri,
            )

        try:
            self._upload_to_gcs(ndjson_bytes, gcs_uri)
            self._load_to_bigquery(gcs_uri, source)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[Pipeline] Storage/BQ load failed for '%s': %s",
                source,
                exc,
                exc_info=True,
            )
            return ConnectorRun(
                source=source,
                status="failed",
                records_loaded=0,
                gcs_uri=gcs_uri,
                error=str(exc),
            )

        return ConnectorRun(
            source=source,
            status="success",
            records_loaded=len(result),
            gcs_uri=gcs_uri,
        )

    @staticmethod
    def _serialize(result: ConnectorResult) -> bytes:
        """Convert records to UTF-8 encoded Newline-Delimited JSON (NDJSON).

        NDJSON is the format expected by BigQuery load jobs with
        source_format=NEWLINE_DELIMITED_JSON.
        """
        lines = [json.dumps(record, ensure_ascii=False, default=str) for record in result.records]
        return "\n".join(lines).encode("utf-8")

    def _build_gcs_uri(self, source: str) -> str:
        """Build the GCS object URI for a connector's daily file.

        Pattern: gs://<bucket>/bronze/<source>/date=YYYY-MM-DD/<source>.ndjson

        Hive-style partition prefix (date=...) enables partition discovery
        in tools like Dataproc, Athena and external BQ tables.
        """
        date_str = self._target_date.isoformat()
        key = f"bronze/{source}/date={date_str}/{source}.ndjson"
        return f"gs://{self._gcs_bucket}/{key}"

    def _upload_to_gcs(self, data: bytes, gcs_uri: str) -> None:
        """Upload *data* to *gcs_uri*.

        Args
        ----
        data    : NDJSON bytes to upload.
        gcs_uri : Full GCS URI (gs://bucket/path/file.ndjson).
        """
        # Strip gs://bucket/ prefix to get the object key
        object_key = gcs_uri.replace(f"gs://{self._gcs_bucket}/", "")
        bucket = self._gcs_client.bucket(self._gcs_bucket)
        blob = bucket.blob(object_key)
        blob.upload_from_string(data, content_type="application/x-ndjson")
        logger.info("[Pipeline] Uploaded %d bytes to %s", len(data), gcs_uri)

    def _load_to_bigquery(self, gcs_uri: str, source: str) -> None:
        """Trigger a BigQuery load job from *gcs_uri* into the Bronze table.

        Load strategy:
          - WRITE_APPEND: new daily file is appended to the partitioned table.
          - Partition decorator ($YYYYMMDD) ensures data lands in the correct
            date partition, enabling cheap partition pruning in queries.
          - AUTODETECT is disabled — schema is managed by Terraform (bigquery.tf).

        Args
        ----
        gcs_uri : Source GCS file URI.
        source  : Connector source name used to resolve the BQ table name.
        """
        table_name = _SOURCE_TO_BQ_TABLE.get(source)
        if not table_name:
            raise ValueError(
                f"No BigQuery table mapping for source '{source}'. "
                f"Add it to _SOURCE_TO_BQ_TABLE in pipeline.py."
            )

        date_nodash = self._target_date.strftime("%Y%m%d")
        table_ref = (
            f"{self._project_id}.{self._bq_dataset}.{table_name}${date_nodash}"
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
            ignore_unknown_values=False,
        )

        load_job = self._bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
        )

        logger.info(
            "[Pipeline] BQ load job %s started: %s → %s",
            load_job.job_id,
            gcs_uri,
            table_ref,
        )

        load_job.result()  # blocks until job completes

        logger.info(
            "[Pipeline] BQ load job %s completed. Rows loaded: %s",
            load_job.job_id,
            load_job.output_rows,
        )

def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{value}'. Expected format: YYYY-MM-DD") from exc


def main() -> None:
    """CLI entry point.

    Usage:
        python -m ingestion.pipeline                    # runs for yesterday
        python -m ingestion.pipeline --date 2024-06-01  # runs for specific date
        python -m ingestion.pipeline --dry-run          # skips GCS and BQ
    """
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Raw-to-Gold Bronze ingestion pipeline")
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Target date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run connectors but skip GCS upload and BQ load",
    )
    args = parser.parse_args()

    target_date = _parse_date(args.date)
    pipeline = BronzePipeline(target_date=target_date, dry_run=args.dry_run)
    report = pipeline.run()

    print(report.to_json())

    if report.overall_status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
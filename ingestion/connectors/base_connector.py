from __future__ import annotations
import abc
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

@dataclass
class ConnectorResult:
    """Standardised envelope returned by every connector.

    Attributes:
        source      : Logical name of the data source (e.g. "open_meteo").
        records     : List of dicts ready for BigQuery / GCS ingestion.
        fetched_at  : UTC timestamp of the successful extraction.
        metadata    : Optional dict with request-level diagnostics.
    """

    source: str
    records: list[dict[str, Any]]
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.records)

    def is_empty(self) -> bool:
        return len(self.records) == 0

    def summary(self) -> str:
        return (
            f"[{self.source}] {len(self.records)} records "
            f"fetched at {self.fetched_at.isoformat()}"
        )

@dataclass
class RetryConfig:
    """Declarative retry policy used when building the HTTP session.

    Attributes:
        total           : Maximum number of retry attempts.
        backoff_factor  : Multiplier applied between retries (exponential backoff).
                          Wait = backoff_factor * 2^(attempt - 1).
                          E.g. factor=1 → waits: 0 s, 2 s, 4 s, 8 s …
        status_forcelist: HTTP status codes that trigger a retry.
        allowed_methods : HTTP verbs eligible for retry.
        raise_on_status : Whether to raise HTTPError on 4xx/5xx after retries.
    """

    total: int = 4
    backoff_factor: float = 1.0
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    allowed_methods: frozenset[str] = frozenset({"GET", "POST"})
    raise_on_status: bool = True

class BaseConnector(abc.ABC):
    """Template for every source connector in the pipeline.

    Subclasses must implement:
        - source_name   (property)  – unique slug, e.g. "open_meteo"
        - fetch()                   – main extraction method

    Subclasses may override:
        - _build_session()          – to add auth headers, client certs, etc.
        - _before_fetch()           – pre-flight validation hook
        - _after_fetch()            – post-fetch side-effects (metrics, cache, …)
    """

    # Default timeout for every HTTP call (connect, read) in seconds
    DEFAULT_TIMEOUT: tuple[int, int] = (10, 30)

    def __init__(
        self,
        retry_config: RetryConfig | None = None,
        timeout: tuple[int, int] | None = None,
    ) -> None:
        self._retry_cfg = retry_config or RetryConfig()
        self._timeout = timeout or self.DEFAULT_TIMEOUT
        self._session: requests.Session = self._build_session()
        self._call_count: int = 0
        self._total_latency_ms: float = 0.0

    @property
    @abc.abstractmethod
    def source_name(self) -> str:
        """Unique, URL-safe identifier for this data source."""

    @abc.abstractmethod
    def fetch(self, target_date: date) -> ConnectorResult:
        """Extract data for *target_date* and return a ConnectorResult.

        Args:
            target_date: The calendar date whose data should be fetched.
                         Connectors are expected to be *idempotent* — calling
                         fetch() twice for the same date must produce the
                         same logical result.

        Returns:
            ConnectorResult with validated, raw records.

        Raises:
            ConnectorError: On unrecoverable extraction failure.
        """

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """Thin wrapper around session.get() with logging and latency tracking.

        Args:
            url    : Fully-qualified URL to request.
            params : Query-string parameters.
            headers: Per-request headers (merged with session headers).

        Returns:
            requests.Response (already validated via raise_for_status).

        Raises:
            ConnectorError: Wraps any requests exception for uniform handling.
        """
        log_params = {k: v for k, v in (params or {}).items() if "key" not in k.lower()}
        logger.debug("[%s] GET %s params=%s", self.source_name, url, log_params)

        t0 = time.perf_counter()
        try:
            response = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=self._timeout,
            )
            if self._retry_cfg.raise_on_status:
                response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise ConnectorError(
                self.source_name, f"Request timed out after {self._timeout}s: {exc}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            raise ConnectorError(
                self.source_name, f"Connection error: {exc}"
            ) from exc
        except requests.exceptions.HTTPError as exc:
            raise ConnectorError(
                self.source_name,
                f"HTTP {exc.response.status_code} from {url}: {exc}",
            ) from exc
        finally:
            elapsed_ms = (time.perf_counter() - t0) * 1_000
            self._call_count += 1
            self._total_latency_ms += elapsed_ms
            logger.debug(
                "[%s] response in %.1f ms (call #%d)",
                self.source_name,
                elapsed_ms,
                self._call_count,
            )

        return response

    def _before_fetch(self, target_date: date) -> None:
        """Called immediately before fetch(). Override for pre-flight checks."""
        logger.info("[%s] Starting fetch for %s", self.source_name, target_date)

    def _after_fetch(self, result: ConnectorResult) -> None:
        """Called immediately after a successful fetch(). Override for side-effects."""
        logger.info("[%s] %s", self.source_name, result.summary())

    def run(self, target_date: date) -> ConnectorResult:
        """Orchestrates _before_fetch → fetch → _after_fetch.

        Use this method instead of calling fetch() directly from the pipeline
        to ensure hooks always execute.
        """
        self._before_fetch(target_date)
        result = self.fetch(target_date)
        self._after_fetch(result)
        return result

    @property
    def avg_latency_ms(self) -> float:
        """Average HTTP call latency in milliseconds."""
        if self._call_count == 0:
            return 0.0
        return self._total_latency_ms / self._call_count

    def diagnostics(self) -> dict[str, Any]:
        """Returns a snapshot of runtime metrics for observability."""
        return {
            "source": self.source_name,
            "http_calls": self._call_count,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "total_latency_ms": round(self._total_latency_ms, 2),
        }

    def _build_session(self) -> requests.Session:
        """Creates and configures the shared requests.Session.

        Subclasses that need auth headers or custom adapters should call
        super()._build_session() and mutate the returned session object.
        """
        session = requests.Session()

        retry = Retry(
            total=self._retry_cfg.total,
            backoff_factor=self._retry_cfg.backoff_factor,
            status_forcelist=list(self._retry_cfg.status_forcelist),
            allowed_methods=list(self._retry_cfg.allowed_methods),
            raise_on_status=False,  # we raise manually for richer error messages
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update(
            {
                "User-Agent": f"raw-to-gold-pipeline/{self.source_name}",
                "Accept": "application/json",
            }
        )
        return session

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"source={self.source_name!r}, "
            f"retries={self._retry_cfg.total}, "
            f"timeout={self._timeout})"
        )

class ConnectorError(Exception):
    """Raised when a connector fails to extract data after all retries.

    Attributes:
        source  : Name of the connector that raised the error.
        message : Human-readable description of the failure.
    """

    def __init__(self, source: str, message: str) -> None:
        self.source = source
        self.message = message
        super().__init__(f"[{source}] {message}")

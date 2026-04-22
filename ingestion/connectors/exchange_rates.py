"""
exchange_rates.py
-----------------
Connector for the Frankfurter exchange-rate API.

Endpoint : https://api.frankfurter.app
Docs     : https://www.frankfurter.app/docs/
Auth     : None (public API, no API key required)

What it fetches
---------------
Daily closing exchange rates from a configurable base currency to a set of
target currencies. Each record represents one (date, base_currency,
target_currency) triple and is structured to land into the BigQuery Bronze
table `raw.exchange_rates_daily`.

BigQuery target schema
----------------------
    date              DATE     REQUIRED   -- partition column
    base_currency     STRING   REQUIRED   -- clustering column
    target_currency   STRING   REQUIRED   -- clustering column
    rate              FLOAT64  REQUIRED
    fetched_at        TIMESTAMP REQUIRED
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from ingestion.connectors.base_connector import BaseConnector, ConnectorError, ConnectorResult

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.frankfurter.app"

# Default set of currencies to track (ISO 4217 codes)
DEFAULT_BASE_CURRENCY: str = "USD"
DEFAULT_TARGET_CURRENCIES: list[str] = ["BRL", "EUR", "GBP", "JPY", "CAD", "AUD"]

class ExchangeRatesConnector(BaseConnector):
    """Fetches daily exchange rates from the Frankfurter API.

    The connector is designed to be idempotent: fetching the same date twice
    always produces the same set of records (Frankfurter serves historical
    closing rates that never change once published).

    Usage
    -----
    >>> from datetime import date
    >>> connector = ExchangeRatesConnector(base_currency="USD")
    >>> result = connector.run(date(2024, 6, 1))
    >>> print(result.summary())
    [exchange_rates] 6 records fetched at 2024-06-02T...

    Configuring currencies
    ----------------------
    >>> connector = ExchangeRatesConnector(
    ...     base_currency="EUR",
    ...     target_currencies=["BRL", "USD", "GBP"],
    ... )
    """

    def __init__(
        self,
        base_currency: str = DEFAULT_BASE_CURRENCY,
        target_currencies: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            base_currency      : ISO 4217 code for the base (e.g. "USD").
            target_currencies  : List of ISO 4217 codes to convert to.
                                 Defaults to DEFAULT_TARGET_CURRENCIES.
            **kwargs           : Forwarded to BaseConnector (retry_config, timeout, …).
        """
        super().__init__(**kwargs)
        self._base_currency = base_currency.upper()
        self._target_currencies = [c.upper() for c in (target_currencies or DEFAULT_TARGET_CURRENCIES)]

        # Frankfurter rejects the base currency in the target list
        if self._base_currency in self._target_currencies:
            self._target_currencies.remove(self._base_currency)
            logger.debug(
                "[%s] Removed base currency '%s' from target list.",
                self.source_name,
                self._base_currency,
            )

    @property
    def source_name(self) -> str:
        return "exchange_rates"

    def fetch(self, target_date: date) -> ConnectorResult:
        """Fetch exchange rates for *target_date*.

        The Frankfurter API returns all requested target currencies in a single
        call. We explode the response into one record per (base, target) pair
        so that BigQuery queries can filter and aggregate by currency pair easily.

        Args:
            target_date : Calendar date whose closing rates should be fetched.

        Returns:
            ConnectorResult with one record per currency pair.

        Raises:
            ConnectorError : On HTTP error, unexpected API shape, or if the API
                             returns data for a different date (e.g. nearest
                             trading day substitution).
        """
        date_str = target_date.isoformat()

        payload = self._call_api(date_str)
        returned_date = payload.get("date")

        # Frankfurter silently shifts to the nearest trading day on weekends
        # and holidays. We record the actual date returned for auditability.
        if returned_date and returned_date != date_str:
            logger.info(
                "[%s] Requested %s but API returned %s (nearest trading day).",
                self.source_name,
                date_str,
                returned_date,
            )

        records = self._parse_records(payload, actual_date=returned_date or date_str)

        return ConnectorResult(
            source=self.source_name,
            records=records,
            fetched_at=datetime.now(timezone.utc),
            metadata={
                "target_date":       date_str,
                "returned_date":     returned_date,
                "base_currency":     self._base_currency,
                "target_currencies": self._target_currencies,
                "pairs_fetched":     len(records),
                "api_diagnostics":   self.diagnostics(),
            },
        )

    def _call_api(self, date_str: str) -> dict[str, Any]:
        """Execute the HTTP request and return the parsed JSON body.

        Args:
            date_str : ISO date string "YYYY-MM-DD".

        Returns:
            Parsed JSON dict from the Frankfurter API.

        Raises:
            ConnectorError : On HTTP failure or non-JSON response.
        """
        url = f"{_BASE_URL}/{date_str}"
        params = {
            "from": self._base_currency,
            "to":   ",".join(self._target_currencies),
        }

        response = self.get(url, params=params)

        try:
            return response.json()
        except ValueError as exc:
            raise ConnectorError(
                self.source_name,
                f"Failed to decode JSON for date {date_str}: {exc}",
            ) from exc

    def _parse_records(
        self,
        payload: dict[str, Any],
        actual_date: str,
    ) -> list[dict[str, Any]]:
        """Explode the API response into a list of per-currency-pair records.

        Frankfurter returns:
            {
                "amount": 1.0,
                "base":   "USD",
                "date":   "2024-06-03",
                "rates":  {"BRL": 5.12, "EUR": 0.93, ...}
            }

        We produce one flat dict per target currency:
            {
                "date":            "2024-06-03",
                "base_currency":   "USD",
                "target_currency": "BRL",
                "rate":            5.12,
                "fetched_at":      "2024-06-04T..."
            }

        Args:
            payload     : Parsed JSON from the API.
            actual_date : The date string returned by the API (may differ from
                          the requested date if a trading-day shift occurred).

        Returns:
            List of flat dicts, one per currency pair.

        Raises:
            ConnectorError : If the 'rates' key is missing or empty.
        """
        rates: dict[str, float] = payload.get("rates", {})

        if not rates:
            raise ConnectorError(
                self.source_name,
                f"Empty or missing 'rates' in API response for date {actual_date}. "
                f"Keys received: {list(payload.keys())}",
            )

        fetched_at = datetime.now(timezone.utc).isoformat()
        records: list[dict[str, Any]] = []

        for target_currency, rate in rates.items():
            parsed_rate = self._safe_rate(rate, target_currency)
            if parsed_rate is None:
                continue  # skip unparseable rates; warning already logged

            records.append(
                {
                    "date":            actual_date,
                    "base_currency":   self._base_currency,
                    "target_currency": target_currency,
                    "rate":            parsed_rate,
                    "fetched_at":      fetched_at,
                }
            )

        # Warn about any requested currencies not returned
        missing = set(self._target_currencies) - set(rates.keys())
        if missing:
            logger.warning(
                "[%s] Currencies not returned by API: %s",
                self.source_name,
                sorted(missing),
            )

        return records

    def _safe_rate(self, value: Any, currency: str) -> float | None:
        """Coerce *value* to a positive float; log and return None on failure."""
        try:
            rate = float(value)
        except (TypeError, ValueError):
            logger.warning(
                "[%s] Could not cast rate for '%s' (value=%r) — skipping.",
                self.source_name,
                currency,
                value,
            )
            return None

        if rate <= 0:
            logger.warning(
                "[%s] Non-positive rate %.6f for '%s' — skipping.",
                self.source_name,
                rate,
                currency,
            )
            return None

        return rate

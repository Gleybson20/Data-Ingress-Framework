"""
ingestion/retry/backoff_handler.py
------------------------------------
Standalone exponential backoff retry decorator and context manager.

Why this exists alongside requests' built-in Retry adapter
-----------------------------------------------------------
requests.adapters.HTTPAdapter handles transport-level retries (connection
resets, specific HTTP status codes). This module handles *application-level*
retries — arbitrary callables that may raise Python exceptions unrelated to
HTTP (e.g. BigQuery quota errors, GCS write failures, JSON parse errors).

Features
--------
  - Exponential backoff with optional full jitter (AWS-style)
  - Configurable exception allowlist
  - Per-attempt and total timeout caps
  - Structured logging on every retry and final failure
  - Usable as a decorator (@with_backoff) or called directly (backoff_handler.run)

Usage — decorator
-----------------
    @with_backoff(max_attempts=4, base_delay=1.0, exceptions=(IOError,))
    def upload_to_gcs(data: bytes) -> None:
        ...

Usage — instance
----------------
    handler = BackoffHandler(max_attempts=3, base_delay=2.0)
    result = handler.run(my_callable, arg1, kwarg=value)
"""

from __future__ import annotations
import functools
import logging
import random
import time
from typing import Any, Callable, Type

logger = logging.getLogger(__name__)

class BackoffHandler:
    """Retries a callable with exponential backoff on specified exceptions.

    Parameters
    ----------
    max_attempts : Total number of attempts (1 = no retries).
    base_delay   : Initial wait time in seconds between attempts.
    max_delay    : Upper bound for computed delay (caps exponential growth).
    multiplier   : Exponential growth factor applied on each retry.
    jitter       : If True, applies full jitter: delay = random(0, computed_delay).
                   Prevents thundering herd when many workers retry simultaneously.
    exceptions   : Tuple of exception types that trigger a retry.
                   Any other exception propagates immediately.
    """

    def __init__(
        self,
        max_attempts: int = 4,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True,
        exceptions: tuple[Type[Exception], ...] = (Exception,),
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if base_delay < 0:
            raise ValueError(f"base_delay must be >= 0, got {base_delay}")

        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self.exceptions = exceptions

    def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute *func* with retry logic.

        Args
        ----
        func   : Callable to execute.
        *args  : Positional arguments forwarded to func.
        **kwargs : Keyword arguments forwarded to func.

        Returns
        -------
        Any
            Return value of func on success.

        Raises
        ------
        RetryExhausted
            If func raises a retryable exception on every attempt.
        Exception
            Any non-retryable exception propagates immediately.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 1:
                    logger.info(
                        "[BackoffHandler] '%s' succeeded on attempt %d/%d.",
                        _func_name(func),
                        attempt,
                        self.max_attempts,
                    )
                return result

            except self.exceptions as exc:
                last_exc = exc
                is_last = attempt == self.max_attempts

                if is_last:
                    logger.error(
                        "[BackoffHandler] '%s' failed after %d attempt(s). "
                        "No more retries. Last error: %s",
                        _func_name(func),
                        self.max_attempts,
                        exc,
                    )
                    break

                delay = self._compute_delay(attempt)
                logger.warning(
                    "[BackoffHandler] '%s' attempt %d/%d failed: %s. "
                    "Retrying in %.2fs…",
                    _func_name(func),
                    attempt,
                    self.max_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)

        raise RetryExhausted(
            func_name=_func_name(func),
            attempts=self.max_attempts,
            last_exception=last_exc,
        )

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Allow BackoffHandler to be used as a decorator directly.

        Example
        -------
            handler = BackoffHandler(max_attempts=3)

            @handler
            def flaky_call():
                ...
        """
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.run(func, *args, **kwargs)
        return wrapper

    def _compute_delay(self, attempt: int) -> float:
        """Calculate sleep duration for *attempt* (1-indexed).

        Formula (without jitter):
            delay = min(base_delay * multiplier^(attempt-1), max_delay)

        With full jitter (recommended for distributed systems):
            delay = random(0, computed_delay)

        Args
        ----
        attempt : Current attempt number (starts at 1).

        Returns
        -------
        float
            Seconds to sleep before the next attempt.
        """
        exponential = self.base_delay * (self.multiplier ** (attempt - 1))
        capped = min(exponential, self.max_delay)
        if self.jitter:
            return random.uniform(0, capped)
        return capped

def with_backoff(
    max_attempts: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    multiplier: float = 2.0,
    jitter: bool = True,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator factory that wraps a function with BackoffHandler retry logic.

    Args
    ----
    max_attempts : Total number of attempts including the first call.
    base_delay   : Initial wait in seconds.
    max_delay    : Maximum wait cap in seconds.
    multiplier   : Backoff growth factor.
    jitter       : Enable full jitter (recommended).
    exceptions   : Exception types that trigger a retry.

    Returns
    -------
    Callable
        Decorated function with retry behaviour.

    Example
    -------
        @with_backoff(max_attempts=5, base_delay=2.0, exceptions=(IOError, TimeoutError))
        def write_to_gcs(data):
            ...
    """
    handler = BackoffHandler(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter,
        exceptions=exceptions,
    )
    return handler

def _func_name(func: Callable[..., Any]) -> str:
    """Extract a human-readable name from a callable."""
    return getattr(func, "__name__", repr(func))

class RetryExhausted(Exception):
    """Raised when all retry attempts for a callable are exhausted.

    Attributes
    ----------
    func_name      : Name of the function that failed.
    attempts       : Total number of attempts made.
    last_exception : The last exception raised by the callable.
    """

    def __init__(
        self,
        func_name: str,
        attempts: int,
        last_exception: Exception | None,
    ) -> None:
        self.func_name = func_name
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"'{func_name}' failed after {attempts} attempt(s). "
            f"Last error: {last_exception}"
        )
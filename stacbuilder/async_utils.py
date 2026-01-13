"""Generic asynchronous task execution mixin.

This mixin encapsulates a lightweight thread pool for offloading IO-bound or short CPU-bound
tasks so that the caller can continue producing work while previous tasks complete in
the background.

Expectations for the consumer class:
 - Call `_init_async_task_pool()` during `__init__` to initialize internal structures.
 - Use `_submit_async_task(callable, *args, **kwargs)` to submit work.
 - Call `_wait_for_tasks()` before finalizing to guarantee completion & surface errors.

Features:
 - Bounded number of outstanding futures via `_max_outstanding_tasks` (env override: `STAC_BUILDER_MAX_OUTSTANDING_SAVES` kept for backward compatibility).
 - Fail-fast error handling during throttling and final wait.
 - Lazy executor creation sized for IO-bound workloads.
"""

from __future__ import annotations

import os
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Callable, List, Optional

from loguru import logger


class AsyncTaskPoolMixin:
    """Mixin providing generic asynchronous task submission."""

    _executor: Optional[ThreadPoolExecutor]
    _task_futures: List[Future]
    _max_outstanding_tasks: int
    _result_callback: Optional[Callable[[Any], None]]

    def _init_async_task_pool(
        self,
        max_outstanding_tasks: int = 10_000,
        result_callback: Optional[Callable[[Any], None]] = None,
    ):  # to be called by subclass __init__
        """Initialize the async task pool.

        Args:
            max_outstanding_tasks: Maximum number of concurrent futures.
            result_callback: Optional callback to process results as they complete.
                           Called with the result of each completed task.
                           If None, results are not processed during throttling.
        """
        self._executor = None
        self._task_futures = []
        self._max_outstanding_tasks = max_outstanding_tasks
        self._result_callback = result_callback

    def _log(self, msg: str):
        """Legacy support for custom logging method in consumer class."""
        if hasattr(self, "_log_progress_message"):
            try:
                self._log_progress_message(msg)
                return
            except Exception:  # pragma: no cover
                pass
        logger.info(msg)

    def _ensure_executor(self):
        if self._executor is None:
            try:
                max_workers = min(32, (os.cpu_count() or 4) * 2)
            except Exception:
                max_workers = 8
            self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="stac-save")

    def _handle_finished_future(self, fut: Future) -> Optional[Exception]:
        """Process a finished future and return any exception that occurred.

        Args:
            fut: The completed future to process

        Returns:
            Exception if an error occurred, None otherwise
        """
        exc = fut.exception()
        if exc:
            return exc
        if self._result_callback:
            try:
                result = fut.result()
                self._result_callback(result)
            except Exception as e:
                return e
        return None

    def _enforce_futures_cap(self):
        if not self._task_futures:
            return
        while len(self._task_futures) >= self._max_outstanding_tasks:
            done, not_done = wait(self._task_futures, return_when=FIRST_COMPLETED)
            errors = [err for fut in done if (err := self._handle_finished_future(fut)) is not None]
            self._task_futures = [f for f in not_done]
            if errors:
                raise RuntimeError(
                    f"Error while executing async task (during throttling). First: {errors[0]}"
                ) from errors[0]

    def _submit_async_task(self, func, *args, **kwargs):
        """Submit a generic callable for asynchronous execution.

        Returns the Future instance.
        """
        self._ensure_executor()
        self._enforce_futures_cap()
        fut = self._executor.submit(func, *args, **kwargs)
        self._task_futures.append(fut)
        return fut

    def _wait_for_tasks(self, shutdown: bool = True):
        """Wait for all outstanding tasks to complete.

        Args:
            shutdown: If True, shutdown the executor after waiting. Defaults to True.
        """
        if not self._task_futures:
            return
        self._log(f"Waiting for {len(self._task_futures)} asynchronous task(s) to complete ...")
        wait(self._task_futures)
        errors = [err for fut in self._task_futures if (err := self._handle_finished_future(fut)) is not None]
        self._task_futures.clear()
        if shutdown:
            self.shutdown_executor()
        if errors:
            raise RuntimeError(
                f"{len(errors)} error(s) occurred while executing async tasks. First: {errors[0]}"
            ) from errors[0]

    def shutdown_executor(self):
        """Shutdown the executor if it exists."""
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

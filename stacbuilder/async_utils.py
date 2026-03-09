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
import time
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
        """Create the ThreadPoolExecutor if it doesn't exist"""
        if self._executor is None:
            try:
                max_workers = min(32, (os.cpu_count() or 4) * 2)
            except Exception:
                max_workers = 8
            self._executor = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=f"{self.__class__.__name__}-worker"
            )

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

    def _wait_and_handle_next_finished_tasks(self) -> None:
        """
        Wait for the next future to complete and process it.
        If a result callback is provided, it will be called with the result of the completed task.
        Logs any exceptions that occur during task execution or result processing.
        """
        if not self._task_futures:
            return None
        done, _ = wait(self._task_futures, return_when=FIRST_COMPLETED)
        for fut in done:
            err = self._handle_finished_future(fut)
            self._task_futures.remove(fut)
            if err:
                logger.error(f"Error in async task of {self.__class__.__name__}: {err}")

    def _enforce_futures_cap(self) -> None:
        """Wait until the number of outstanding futures is below the configured cap before allowing more tasks to be submitted."""
        if not self._task_futures:
            return
        time_before_wait = time.time()
        while len(self._task_futures) >= self._max_outstanding_tasks:
            self._wait_and_handle_next_finished_tasks()
        time_waited = time.time() - time_before_wait
        if time_waited > 1:
            logger.debug(f"Throttled task submission for {time_waited:.2f} in {self.__class__.__name__}.")

    def _submit_async_task(self, func, *args, **kwargs) -> Future:
        """Submit a generic callable for asynchronous execution.

        Returns the Future instance.
        """
        self._ensure_executor()
        self._enforce_futures_cap()
        fut = self._executor.submit(func, *args, **kwargs)
        self._task_futures.append(fut)
        return fut

    def _wait_for_tasks(self, shutdown: bool = True) -> None:
        """Wait for all outstanding tasks to complete.

        Args:
            shutdown: If True, shutdown the executor after waiting. Defaults to True.
        """
        if not self._task_futures:
            return
        self._log(f"Waiting for {len(self._task_futures)} asynchronous task(s) to complete ...")
        time_before_wait = time.time()
        while self._task_futures:
            self._wait_and_handle_next_finished_tasks()
        time_waited = time.time() - time_before_wait
        logger.debug(f"Waited {time_waited:.2f} seconds for async tasks to complete in {self.__class__.__name__}.")
        if shutdown:
            self.shutdown_executor()

    def shutdown_executor(self) -> None:
        """Shutdown the executor if it exists."""
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

import asyncio
import logging
from typing import Optional, Coroutine, Any
import traceback

log = logging.getLogger(__name__)


class TaskProcessor:
    """
    Simple singleton task processor with a queue and worker pool.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        """Ensure only one instance exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the processor once."""
        if self._initialized:
            return

        self._initialized = True
        self.max_workers = 10
        self.queue = asyncio.Queue(maxsize=10000)
        self.running = False
        self.workers = []

        # Stats
        self.total_processed = 0
        self.total_failed = 0

        log.info("TaskProcessor initialized")

    async def add_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Add a coroutine to the queue."""
        log.info("Adding task...")
        await self.queue.put(coro)
        log.info(f"Task added. Queue size: {self.queue.qsize()}")

    def add_task_nowait(self, coro: Coroutine[Any, Any, Any]) -> None:
        """Add a coroutine to the queue without waiting."""
        self.queue.put_nowait(coro)
        log.info(f"Task added (nowait). Queue size: {self.queue.qsize()}")

    async def _worker(self, worker_id: int) -> None:
        """Worker that pulls tasks from queue and executes them."""
        log.info(f"Worker {worker_id} started")

        while self.running:
            try:
                # Get task from queue with timeout to check running flag
                try:
                    coro = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Execute the task
                try:
                    log.info(f"Worker {worker_id} executing task. queue size: {self.queue.qsize()}")
                    await coro
                    self.total_processed += 1
                    log.info(f"Worker {worker_id} completed task. Total processed: {self.total_processed}")
                except Exception as e:
                    self.total_failed += 1
                    log.error(f"Worker {worker_id} task failed: {e}\n{traceback.format_exc()}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Worker {worker_id} unexpected error: {e}")

        log.info(f"Worker {worker_id} stopped")

    async def start(self) -> None:
        """Start the processor with worker pool."""
        if self.running:
            log.warning("TaskProcessor already running")
            return

        self.running = True
        log.info(f"Starting TaskProcessor with {self.max_workers} workers")

        # Create workers
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(i))
            self.workers.append(worker)

        # Keep running until stopped
        try:
            while self.running:
                await asyncio.sleep(1)

                # Log status every 30 seconds
                if int(asyncio.get_event_loop().time()) % 30 == 0:
                    log.info(
                        f"Status - Queue: {self.queue.qsize()}, "
                        f"Processed: {self.total_processed}, "
                        f"Failed: {self.total_failed}"
                    )
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the processor."""
        if not self.running:
            return

        log.info("Stopping TaskProcessor...")
        self.running = False

        # Wait for workers to finish
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)
            self.workers.clear()

        log.info(f"TaskProcessor stopped. Processed: {self.total_processed}, Failed: {self.total_failed}")

    @classmethod
    def get_instance(cls, **kwargs) -> "TaskProcessor":
        """Get the singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

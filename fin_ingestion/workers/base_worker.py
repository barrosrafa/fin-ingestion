"""Queue worker sharing the dispatcher routing, result and retry contracts."""

from fin_ingestion.dispatcher.task_dispatcher import TaskDispatcher


class IngestionWorker(TaskDispatcher):
    """Compatibility entry point used by worker-start and the container runner."""

    async def process_one_task(self, lease_duration_sec: int = 300) -> bool:
        result = await self.dispatch_one(lease_duration_sec=lease_duration_sec)
        return result is not None and result["status"] == "SUCCEEDED"

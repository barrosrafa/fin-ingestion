"""Entrypoint para inicialização dos workers Python em containers Docker."""

import asyncio
import signal

import structlog

from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.workers.base_worker import IngestionWorker

logger = structlog.get_logger(__name__)


async def main() -> None:
    dw = DWWriter()
    pool = await dw.get_pool()
    worker = IngestionWorker(pool=pool)

    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("Sinal de encerramento recebido no worker. Parando...")
        worker.is_running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass

    try:
        await worker.run_forever(poll_interval_sec=5.0)
    finally:
        await worker.close()
        await dw.close()


if __name__ == "__main__":
    asyncio.run(main())

import asyncio

import aiohttp

from config.diavgeia_config import DiavgeiaConfig
from utilities.logger_mixin import LoggerMixin
from workers.crawler import Crawler


class Dispatcher(LoggerMixin):
    config: DiavgeiaConfig

    def __init__(self, config: DiavgeiaConfig):
        super().__init__(config)
        self.config = config

    def execute(self):
        """Runs the pipeline"""
        self.log(f"Dispatcher started with config: {self.config.model_dump()}")
        self.log(f"Fetching all decisions for: {self.config.date_id}...")
        asyncio.run(self.fetch_loop())
        self.log("Fetching finished successfully.")

    async def fetch_loop(self):
        """Instantiates the workers and runs the main loop in async"""

        queue = asyncio.Queue()
        async with aiohttp.ClientSession() as session:
            crawlers = [
                asyncio.create_task(
                    Crawler(
                        id=f"crawler-{id_}",
                        queue=queue,
                        session=session,
                        config=self.config,
                    ).crawl()
                )
                for id_ in range(self.config.crawl_workers)
            ]
            downloaders = [
                asyncio.create_task(
                    Crawler(
                        id=f"downloader-{id_}",
                        queue=queue,
                        session=session,
                        config=self.config,
                    ).download()
                )
                for id_ in range(self.config.download_workers)
            ]
            await asyncio.gather(
                *crawlers,
                *downloaders,
            )

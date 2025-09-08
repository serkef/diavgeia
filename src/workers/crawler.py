import json
from asyncio import Queue
from gzip import compress
from math import ceil
from typing import Dict

from aiohttp import (
    ClientSession,
    ContentTypeError,
    BasicAuth,
    ClientPayloadError,
    ClientConnectorError,
    ServerDisconnectedError,
)
import datetime as dt
from config.diavgeia_config import DiavgeiaConfig
from utilities.async_helpers import save_file
from utilities.logger_mixin import LoggerMixin


class Crawler(LoggerMixin):
    config: DiavgeiaConfig
    queue: Queue
    session: ClientSession
    worker_id: str
    auth: BasicAuth

    def __init__(
        self,
        worker_id: str,
        queue: Queue,
        session: ClientSession,
        config: DiavgeiaConfig,
    ):
        self.worker_id = worker_id
        super().__init__(config)
        self.config = config
        self.queue = queue
        self.session = session
        self.auth = BasicAuth(config.diavgeia_api_user, config.diavgeia_api_password)

    async def crawl(self):
        """Crawls Diavgeia API to get all decisions for a day. Adds all decisions to
        the queue for processing by next workers."""

        from_date = self.config.date_id
        to_date = from_date + dt.timedelta(days=1)
        api_url = "https://diavgeia.gov.gr/opendata/search"

        params = {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "size": 500,
            "page": 0,
        }
        # Get total number of pages
        async with self.session.get(api_url, params=params, auth=self.auth) as res:
            try:
                response = await res.json()
            except ContentTypeError:
                self.log(f"No decisions for {params}")
                await self.queue.put(None)
                return
        total = response["info"]["total"]
        max_page = ceil(total / response["info"]["size"])
        crawled = 0

        for page in range(max_page):
            self.log(f"Crawling page {page}/{max_page:,d}.")

            params["page"] = page
            async with self.session.get(api_url, params=params, auth=self.auth) as res:
                response = await res.json()

            for dec in response["decisions"]:
                await self.crawl_decision(dec)
                crawled += 1
                if self.config.limit and crawled >= self.config.limit:
                    self.warn("Reached limit, stopping crawl.")
                    break
            if self.config.limit and crawled >= self.config.limit:
                self.warn("Reached limit, stopping crawl.")
                break
        await self.queue.put(None)

    @staticmethod
    def validate_decision(decision_dict: Dict) -> bool:
        """Validates a decision dictionary contents"""
        if "errorCode" in decision_dict:
            return False
        if "ada" not in decision_dict:
            return False
        if "documentUrl" not in decision_dict:
            return False
        return True

    async def crawl_decision(self, decision_dict: Dict, retry: bool = True):
        """Gets a decision object (map), crawls additional info and adds it to
        decision_queue to be downloaded."""

        if not self.validate_decision(decision_dict):
            # In some cases, documentUrl is empty, but querying the specific
            #  ada, returns more info. Example `ΡΟΗΩ46Ψ8ΧΒ-Ι5Δ`
            self.debug(f"Gor invalid decision. {decision_dict}")
            async with self.session.get(
                decision_dict["url"], auth=self.auth
            ) as response:
                decision_dict = await response.json()
            if retry:
                await self.crawl_decision(decision_dict, retry=False)
        else:
            await self.queue.put(decision_dict)

    async def download(self):
        """Downloads decisions from queue in asynchronous manner"""

        while True:
            # Check if queue is empty
            decision = await self.queue.get()
            if decision is None:
                await self.queue.put(None)
                break

            # Set export paths
            doc_url = decision["documentUrl"]
            ada = decision["ada"]
            date = dt.date.fromtimestamp(decision["submissionTimestamp"] // 1000)

            dec_path = self.config.export_path / date.isoformat() / ada
            dec_path.mkdir(parents=True, exist_ok=True)
            json_filepath = dec_path / f"{ada}.json.gz"
            pdf_filepath = dec_path / f"{ada}.pdf.gz"
            self.debug(f"Downloading {ada}")
            try:
                # Download decision info
                json_opts = dict(ensure_ascii=False, sort_keys=True)
                await save_file(
                    compress(
                        json.dumps(decision, **json_opts).encode("utf-8"),
                        mtime=decision["submissionTimestamp"] // 1000,
                    ),
                    json_filepath,
                )
                # Download decision document
                if not self.config.download_pdf:
                    self.debug("Skipping PDF download.")
                    continue
                if not doc_url:
                    self.warn(f"No document for ada {ada!r}.")
                    continue
                async with self.session.get(doc_url, auth=self.auth) as response:
                    res = await response.read()
                    await save_file(compress(res), pdf_filepath)
                self.debug(f"Downloaded: {decision['ada']}")
            except (ClientPayloadError, ClientConnectorError, ServerDisconnectedError):
                # Put decision back to the queue
                await self.queue.put(decision)

        self.log(f"Shutting down downloader.")

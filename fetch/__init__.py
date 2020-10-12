""" init for fetch module """
import argparse
import asyncio
import datetime
import json
import time
from asyncio import sleep
from gzip import compress
from math import ceil
from pathlib import Path
from typing import Dict

import aiohttp
from aiohttp import (
    BasicAuth,
    ClientConnectorError,
    ClientPayloadError,
    ContentTypeError,
    ServerDisconnectedError,
)
from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api

from fetch.utilities import (
    ASYNC_WORKERS,
    B2_KEY,
    B2_KEY_ID,
    B2_UPLOAD_PATH,
    BUCKET_NAME,
    DIAVGEIA_API_PASSWORD,
    DIAVGEIA_API_USER,
    DOWNLOAD_PDF,
    EXPORT_DIR,
    async_save_file,
    get_logger,
    md5,
)

AUTH = BasicAuth(DIAVGEIA_API_USER, DIAVGEIA_API_PASSWORD)


class DiavgeiaDailyFetch:
    """ Class that fetches diavgeia documents for a day """

    def __init__(self, date: datetime.date):
        """
        :param date: Date for which documents will be fetched
        """

        self.date_str = date.isoformat()
        self.date = date
        log_filename = f"diavgeia.{datetime.datetime.now().isoformat()}.log"
        self.logger = get_logger(f"DiavgeiaDailyFetch.{self.date_str}", log_filename)
        self.async_workers = ASYNC_WORKERS
        self.export_dir = EXPORT_DIR
        self.decision_queue = None
        self.upload_queue = None
        self.upload_path = Path(B2_UPLOAD_PATH)
        self.crawl_queue = None
        self.session = None
        b2_api = B2Api(InMemoryAccountInfo())
        b2_api.authorize_account("production", B2_KEY_ID, B2_KEY)
        self.bucket = b2_api.get_bucket_by_name(BUCKET_NAME)

    def execute(self):
        """ Runs the pipeline"""

        self.logger.info(f"Fetching all decisions for: {self.date_str!r}...")
        asyncio.run(self.fetch_loop())
        self.logger.info(f"Fetching finished successfully.")

    async def fetch_loop(self):
        """ Fetches all documents from diavgeia for a given date """

        self.crawl_queue = asyncio.Queue()
        self.decision_queue = asyncio.Queue()
        self.upload_queue = asyncio.Queue()
        async with aiohttp.ClientSession() as self.session:
            downloaders = [
                asyncio.create_task(self.downloader(str(i)))
                for i in range(self.async_workers)
            ]
            uploaders = [
                asyncio.create_task(self.upload_to_b2(str(i)))
                for i in range(self.async_workers)
            ]
            await asyncio.gather(
                asyncio.create_task(self.get_decisions()),
                *downloaders,
                *uploaders,
                asyncio.create_task(self.monitor()),
            )

    async def get_decisions(self):
        """ Iterates through all decisions of a day """

        from_date = self.date_str
        to_date = (self.date + datetime.timedelta(days=1)).isoformat()
        api_url = "https://diavgeia.gov.gr/opendata/search"

        params = {
            "from_date": from_date,
            "to_date": to_date,
            "size": 500,
            "page": 0,
        }
        # Get total number of pages
        async with self.session.get(api_url, params=params, auth=AUTH) as res:
            try:
                response = await res.json()
            except ContentTypeError:
                self.logger.info(f"No decisions for {params}")
                await self.decision_queue.put(None)
                return
        max_page = ceil(response["info"]["total"] / response["info"]["size"])

        for page in range(max_page):
            self.logger.info(f"Page {page}/{max_page:,d}.")

            params["page"] = page
            async with self.session.get(api_url, params=params, auth=AUTH) as res:
                response = await res.json()

            for dec in response["decisions"]:
                await self.crawl_decision(dec)
        await self.decision_queue.put(None)

    @staticmethod
    def validate_decision(decision_dict: Dict):
        """ Validates a decision dictionary contents """
        if "errorCode" in decision_dict:
            return False
        if "ada" not in decision_dict:
            return False
        if "documentUrl" not in decision_dict:
            return False
        return True

    async def crawl_decision(self, decision_dict: Dict, retry: bool = True):
        """ Gets a decision object (map), crawls additional info and adds it to
        decision_queue to be downloaded. """

        if not self.validate_decision(decision_dict):
            # In some cases, documentUrl is empty, but querying the specific
            #  ada, returns more info. Example `ΡΟΗΩ46Ψ8ΧΒ-Ι5Δ`
            self.logger.debug(f"Gor invalid decision. {decision_dict}")
            async with self.session.get(decision_dict["url"], auth=AUTH) as response:
                decision_dict = await response.json()
            if retry:
                await self.crawl_decision(decision_dict, retry=False)
        else:
            await self.decision_queue.put(decision_dict)

    async def monitor(self):
        """ Monitors progress asynchronously"""

        interval = 5
        avg_length = 10
        progresses = []
        last_remaining = 0

        while True:
            element = await self.decision_queue.get()
            await self.decision_queue.put(element)
            if element is None:
                break

            remaining = self.decision_queue.qsize()
            progress = last_remaining - remaining
            last_remaining = remaining
            progresses.append(progress)

            if len(progresses) > avg_length:
                avg = sum(progresses[-avg_length:]) / avg_length
                if avg:
                    eta = remaining * interval / avg
                    eta = time.strftime("%H:%M:%S", time.gmtime(eta))
                else:
                    eta = "unknown"
            else:
                eta = "unknown"
            self.logger.info(f"Remaining {remaining:,d} documents. ETA {eta}")
            await sleep(interval)
        self.logger.info(f"Shutting down monitor")

    async def downloader(self, worker):
        """ Downloads decisions from queue in asynchronous manner """

        while True:
            # Check if queue is empty
            decision = await self.decision_queue.get()
            if decision is None:
                await self.decision_queue.put(None)
                await self.upload_queue.put(None)
                break

            # Set export paths
            doc_url = decision["documentUrl"]
            ada = decision["ada"]
            date = datetime.date.fromtimestamp(decision["submissionTimestamp"] // 1000)

            dec_path = self.export_dir / date.isoformat() / ada
            dec_path.mkdir(parents=True, exist_ok=True)
            json_filepath = dec_path / f"{ada}.json.gz"
            pdf_filepath = dec_path / f"{ada}.pdf.gz"
            self.logger.debug(f"Worker {worker} - Downloading {ada}")
            try:
                # Download decision info
                json_opts = dict(ensure_ascii=False, sort_keys=True)
                await async_save_file(
                    compress(
                        json.dumps(decision, **json_opts).encode("utf-8"),
                        mtime=decision["submissionTimestamp"] // 1000,
                    ),
                    json_filepath,
                )
                await self.upload_queue.put(json_filepath)
                # Download decision document
                if not DOWNLOAD_PDF:
                    continue
                if not doc_url:
                    self.logger.warning(f"No document for ada {ada!r}.")
                    continue
                async with self.session.get(doc_url, auth=AUTH) as response:
                    res = await response.read()
                    await async_save_file(compress(res), pdf_filepath)
                    await self.upload_queue.put(pdf_filepath)
                self.logger.debug(f"Worker {worker} - Downloaded: {decision['ada']}")
            except (ClientPayloadError, ClientConnectorError, ServerDisconnectedError):
                # Put decision back to the queue
                await self.decision_queue.put(decision)

        self.logger.info(f"Worker {worker} - Shutting down downloader.")

    async def upload_to_b2(self, worker):
        """ Uploads a local file to B2 bucket """

        while True:
            # Check if queue is empty
            file = await self.upload_queue.get()
            if file is None:
                await self.decision_queue.put(file)
                break
            self.logger.debug(f"Worker {worker} - Uploading {file}")
            self.logger.debug(f"Checking previous versions of file {file.name}")
            cksum = await md5(file)
            remote = self.upload_path / file.parts[-3] / file.parts[-2] / file.name
            for version in self.bucket.list_file_versions(f"{remote}"):
                if version.content_md5 == cksum:
                    self.logger.debug(f"File {file.name} exists as {version.id_}")
                    break
            else:
                self.logger.debug(f"Uploading file {file.name}")
                self.bucket.upload_local_file(
                    file_name=f"{remote}", local_file=str(file)
                )


def main():
    """ Main function """

    description = ""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        help="Date to fetch documents for. Format: YYYY-MM-DD",
        required=True,
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
    )
    args = parser.parse_args()

    date = args.date
    fetcher = DiavgeiaDailyFetch(date)
    fetcher.execute()


if __name__ == "__main__":
    main()

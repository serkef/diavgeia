""" init for fetch module """
import argparse
import asyncio
import datetime
import json
import os
import time
from asyncio import sleep
from pathlib import Path
from shutil import make_archive, rmtree
from tempfile import TemporaryDirectory

import aiohttp
import urllib3
from aiohttp import (
    BasicAuth,
    ClientConnectorError,
    ClientPayloadError,
    ServerDisconnectedError,
)
from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from dotenv import find_dotenv, load_dotenv

from fetch.utilities import get_logger

load_dotenv(find_dotenv())

# Basic Configuration
load_dotenv(find_dotenv())
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
AUTH = BasicAuth(os.environ["API_USER"], os.environ["API_PASSWORD"])
BUCKET_NAME = os.environ["BUCKET_NAME"]
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_KEY = os.environ["B2_KEY"]


class DiavgeiaDailyFetch:
    """ Class that fetches diavgeia documents for a day """

    def __init__(self, date: datetime.date, workers: int):
        """
        :param date: Date for which documents will be fetched
        :param workers: Number of concurrent downloads
        """

        self.date_str = date.isoformat()
        self.date = date
        log_filename = f"diavgeia.{self.date_str}.log"
        self.logger = get_logger(f"DiavgeiaDailyFetch.{self.date_str}", log_filename)
        self.workers = workers
        self.export_root = Path(TemporaryDirectory().name)
        self.export_dir = self.export_root / self.date_str
        self.export_archive = self.export_dir.with_suffix(".zip")
        self.decision_queue = None
        self.session = None

    def execute(self):
        """ Runs the pipeline"""

        self.logger.info(f"Fetching all decisions for: {self.date_str!r}...")
        asyncio.run(self.fetch_daily())
        self.logger.info(f"Fetching finished.")
        if not self.export_dir.exists():
            self.logger.info(f"No data fetched.")
            return

        self.logger.info(f"Compressing archive '{self.export_dir}'...")
        make_archive(f"{self.export_dir}", "zip", root_dir=f"{self.export_dir}")
        self.logger.info(f"Compressing finished.")

        self.logger.info(f"Uploading archive '{self.export_archive}'...")
        self.upload_to_b2()
        self.logger.info(f"Upload finished.")
        rmtree(self.export_root)

        self.logger.info(f"Finished successfully.")

    async def fetch_daily(self):
        """ Fetches all documents from diavgeia for a given date """

        self.decision_queue = asyncio.Queue()
        async with aiohttp.ClientSession() as self.session:
            downloaders = [
                asyncio.create_task(self.downloader(str(i)))
                for i in range(self.workers)
            ]
            await asyncio.gather(
                asyncio.create_task(self.get_meta()),
                *downloaders,
                asyncio.create_task(self.monitor()),
            )

    async def get_meta(self):
        """ Iterates through all decisions of a day """

        from_date = self.date_str
        to_date = (self.date + datetime.timedelta(days=1)).isoformat()
        api_url = "https://diavgeia.gov.gr/opendata/search"

        query_size = 500

        params = {
            "from_date": from_date,
            "to_date": to_date,
            "size": query_size,
            "page": 0,
        }
        # Get total number of pages
        async with self.session.get(api_url, params=params, auth=AUTH) as res:
            response = await res.json()
        max_page = response["info"]["total"] // response["info"]["size"]

        for page in range(max_page):
            params["page"] = page
            self.logger.info(f"Page {params['page']}/{max_page:,d}.")

            async with self.session.get(api_url, params=params, auth=AUTH) as res:
                response = await res.json()

            for dec in response["decisions"]:
                await self.decision_queue.put(dec)
        await self.decision_queue.put(None)

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
        """ Downloads documents from queue in asynchronous manner """

        while True:
            decision = await self.decision_queue.get()
            if decision is None:
                await self.decision_queue.put(decision)
                break
            self.logger.debug(f"Worker {worker} - Downloading {decision['ada']}")

            try:
                await self.download_decision(decision)
                self.logger.debug(f"Worker {worker} - Downloaded: {decision['ada']}")
            except (ClientPayloadError, ClientConnectorError, ServerDisconnectedError):
                self.logger.warning(
                    f"Worker {worker} - "
                    f"Failed to fetch {decision['ada']!r}. Will retry later..."
                )
                await self.decision_queue.put(decision)
                continue

        self.logger.info(f"Worker {worker} - Shutting down downloader.")

    async def download_decision(self, decision):
        """ Downloads and stores a decision object to disk """

        if not decision["documentUrl"]:
            # In some cases, documentUrl is empty, but querying the specific
            #  ada, returns more info. Example `ΡΟΗΩ46Ψ8ΧΒ-Ι5Δ`
            self.logger.debug(f"Extra call for ada {decision['ada']!r}.")
            async with self.session.get(decision["url"], auth=AUTH) as response:
                decision = await response.json()

        # Set export paths
        export_filepath = self.export_dir / decision["ada"]
        export_filepath.mkdir(parents=True, exist_ok=True)
        json_filepath = export_filepath / f"{decision['ada']}.json"
        pdf_filepath = export_filepath / f"{decision['ada']}.pdf"

        # Store document info
        with open(json_filepath, "w") as fout:
            json.dump(decision, fout, ensure_ascii=False)

        # Store document
        if not decision["documentUrl"]:
            self.logger.warning(f"No document for ada {decision['ada']!r}.")
            return
        async with self.session.get(decision["documentUrl"], auth=AUTH) as response:
            document = await response.read()
        with open(pdf_filepath, "wb") as fout:
            fout.write(document)

    def upload_to_b2(self):
        """ Uploads a local file to B2 bucket """

        b2_api = B2Api(InMemoryAccountInfo())
        b2_api.authorize_account("production", B2_KEY_ID, B2_KEY)
        bucket = b2_api.get_bucket_by_name(BUCKET_NAME)

        remote = f"sink/{self.export_archive.name}"
        local = self.export_archive
        bucket.upload_local_file(file_name=remote, local_file=local)


def main():
    """ Main function """

    def validate_number_of_workers(workers):
        workers = int(workers)
        if 1 <= workers <= 100:
            return workers
        raise argparse.ArgumentTypeError("Number of workers can be 1-100")

    description = ""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--from_date",
        help="Date to fetch documents for. Format: YYYY-MM-DD",
        required=True,
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
    )
    parser.add_argument(
        "--to_date",
        help="Date to fetch documents for. Format: YYYY-MM-DD",
        required=False,
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
    )
    parser.add_argument(
        "--workers",
        help="Number of workers (1-100). Defaults to 4.",
        required=False,
        default=4,
        type=validate_number_of_workers,
    )
    args = parser.parse_args()

    date = args.from_date
    to_date = args.to_date or date + datetime.timedelta(days=1)

    if date >= to_date:
        raise ValueError(
            f"to_date ({date.isoformat()}) is greater or equal "
            f"than from_date ({to_date.isoformat()})."
        )

    while date < to_date:
        fetcher = DiavgeiaDailyFetch(date, args.workers)
        fetcher.execute()
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    main()

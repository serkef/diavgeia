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
from aiohttp import BasicAuth, ClientPayloadError
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

    def __init__(self, date: datetime.date):
        """
        :param date: Date for which documents will be fetched
        """
        log_filename = f"diavgeia.{date.isoformat()}.log"
        self.logger = get_logger("DiavgeiaDailyFetch", log_filename)
        self.date = date
        self.export_root = Path(TemporaryDirectory().name)
        self.export_dir = self.export_root / date.isoformat()
        self.export_archive = self.export_dir.with_suffix(".zip")
        self.decision_queue = None

    def execute(self):
        """ Runs the pipeline"""

        self.logger.info(f"Fetching all decisions for: {self.date.isoformat()!r}...")
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
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                asyncio.create_task(self.get_meta(session)),
                asyncio.create_task(self.download("A", session)),
                asyncio.create_task(self.download("B", session)),
                asyncio.create_task(self.download("C", session)),
                asyncio.create_task(self.download("D", session)),
                asyncio.create_task(self.monitor()),
            )

    async def get_meta(self, session):
        """ Iterates through all decisions of a day """

        from_date = self.date.isoformat()
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
        async with session.get(api_url, params=params, auth=AUTH) as res:
            response = await res.json()
        max_page = response["info"]["total"] // response["info"]["size"]

        for page in range(max_page):
            params["page"] = page
            self.logger.info(f"Page {params['page']}/{max_page:,d}.")

            async with session.get(api_url, params=params, auth=AUTH) as res:
                response = await res.json()

            for dec in response["decisions"]:
                await self.decision_queue.put((dec["ada"], dec["url"]))
        await self.decision_queue.put((None, None))

    async def monitor(self):
        """ Monitors progress asynchronously"""

        interval = 5
        remainings = []
        progresses = []
        last_remaining = 0

        while True:
            element = await self.decision_queue.get()
            await self.decision_queue.put(element)
            if element[0] is None and element[1] is None:
                break

            remaining = self.decision_queue.qsize()
            progress = last_remaining - remaining
            last_remaining = remaining
            remainings.append(remaining)
            progresses.append(progress)

            if len(progresses) > 10:
                avg = sum(progresses[-10:]) / 10
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

    async def download(self, worker, session):
        """ Downloads documents from queue in asynchronous manner """

        while True:
            decision_ada, decision_url = await self.decision_queue.get()
            if decision_ada is None and decision_url is None:
                await self.decision_queue.put((decision_ada, decision_url))
                break
            self.logger.debug(f"Worker {worker}. Downloading {decision_ada}")

            # Get basic document info
            async with session.get(decision_url, auth=AUTH) as response:
                decision = await response.json()

            # Set export paths
            export_filepath = self.export_dir / decision_ada
            export_filepath.mkdir(parents=True, exist_ok=True)
            json_filepath = export_filepath / f"{decision_ada}.json"
            pdf_filepath = export_filepath / f"{decision_ada}.pdf"

            # Store document info
            with open(json_filepath, "w") as fout:
                json.dump(decision, fout, ensure_ascii=False)

            # Store document
            if "documentUrl" in decision:
                async with session.get(decision["documentUrl"], auth=AUTH) as response:
                    try:
                        document = await response.read()
                    except ClientPayloadError:
                        self.logger.error(
                            f"Failed to fetch {decision_ada!r} from "
                            f"{decision['documentUrl']!r}",
                            exc_info=True,
                        )
                        continue
                with open(pdf_filepath, "wb") as fout:
                    fout.write(document)
            self.logger.debug(f"Worker {worker}. Downloaded: {decision_ada}")
        self.logger.info(f"Worker {worker}. Shutting down downloader")

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
    args = parser.parse_args()

    date = args.from_date
    to_date = args.to_date or date + datetime.timedelta(days=1)

    if date >= to_date:
        raise ValueError(
            f"to_date ({date.isoformat()}) is greater or equal "
            f"than from_date ({to_date.isoformat()})."
        )

    while date < to_date:
        fetcher = DiavgeiaDailyFetch(date)
        fetcher.execute()
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    main()

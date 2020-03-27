""" init for fetch module """
import argparse
import datetime
import json
import logging
import os
from pathlib import Path
from shutil import make_archive, rmtree
from tempfile import TemporaryDirectory
from typing import Tuple

import requests
import urllib3
from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from dotenv import find_dotenv, load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv(find_dotenv())

# Basic Configuration
load_dotenv(find_dotenv())
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=os.getenv("LOGLEVEL", "INFO"),
)

# Constants
LOGGER = logging.getLogger("diavgeia.fetch")
AUTH = HTTPBasicAuth(
    username=os.environ["API_USER"], password=os.environ["API_PASSWORD"]
)
BUCKET_NAME = os.environ["BUCKET_NAME"]
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_KEY = os.environ["B2_KEY"]


def daily_decisions(date: datetime.date) -> Tuple[str, str]:
    """ Iterates through all decisions of a day"""

    from_date = date.isoformat()
    to_date = (date + datetime.timedelta(days=1)).isoformat()
    api_url = "https://diavgeia.gov.gr/opendata/search"

    query_size = 500

    headers = {"Accept": "application/json", "Connection": "Keep-Alive"}

    params = {
        "from_date": from_date,
        "to_date": to_date,
        "size": query_size,
        "page": 0,
    }
    decision_idx = 0
    while True:
        response = requests.get(
            api_url, params=params, auth=AUTH, headers=headers, verify=False
        )
        total_size = response.json()["info"]["total"]
        page_size = response.json()["info"]["size"]
        if params["page"] > total_size // page_size:
            break
        params["page"] += 1

        for dec in response.json()["decisions"]:
            decision_idx += 1
            LOGGER.info(
                f"Page {params['page']}/{total_size // page_size + 1:,d}. "
                f"Result {decision_idx}/{total_size + 1:,d}. "
                f"ADA:{dec['ada']!r}"
            )
            yield dec["ada"], dec["url"]


def fetch_daily_diavgeia(date: datetime.date, export_root: Path):
    """ Fetches all documents from diavgeia for a given date """

    for decision_ada, decision_url in daily_decisions(date):
        decision = requests.get(decision_url, auth=AUTH, verify=False).json()
        export_filepath = export_root / decision_ada
        export_filepath.mkdir(parents=True, exist_ok=True)

        json_filepath = export_filepath / f"{decision_ada}.json"
        with open(json_filepath, "w") as fout:
            json.dump(decision, fout, ensure_ascii=False)
            LOGGER.info(f"Exported: {json_filepath}")

        pdf_filepath = export_filepath / f"{decision_ada}.pdf"
        with open(pdf_filepath, "wb") as fout:
            fout.write(requests.get(decision["documentUrl"]).content)
            LOGGER.info(f"Exported: {pdf_filepath}")


def upload_to_b2(filepath: Path):
    """ Uploads a local file to B2 bucket """

    b2_api = B2Api(InMemoryAccountInfo())
    b2_api.authorize_account("production", B2_KEY_ID, B2_KEY)
    bucket = b2_api.get_bucket_by_name(BUCKET_NAME)
    bucket.upload_local_file(
        file_name=f"sink/{filepath.name}", local_file=f"{filepath}"
    )


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
        LOGGER.warning(
            f"{date.isoformat()} is greater or equal than {to_date.isoformat()}. "
            f"No data will be fetched."
        )

    while date < to_date:
        export_root = Path(TemporaryDirectory().name) / date.isoformat()
        export_archive = export_root.with_suffix(".zip")
        LOGGER.info(f"Fetching all decisions for: {date.isoformat()!r}...")
        fetch_daily_diavgeia(date, export_root)
        LOGGER.info(f"Fetching finished.")

        LOGGER.info(f"Compressing archive '{export_root}'...")
        make_archive(f"{export_root}", "zip", root_dir=f"{export_root}")
        LOGGER.info(f"Compressing finished.")

        LOGGER.info(f"Upload archive '{export_archive}'...")
        upload_to_b2(export_archive)
        LOGGER.info(f"Upload finished.")
        rmtree(export_root.parent)

        date += datetime.timedelta(days=1)

    LOGGER.info(f"Finished successfully")

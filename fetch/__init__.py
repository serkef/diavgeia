""" init for fetch module """
import argparse
import datetime
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple

import requests
import urllib3
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
            LOGGER.info(
                f"Page {params['page']}/{total_size // page_size}. "
                f"ADA:{dec['ada']!r}"
            )
            yield dec["ada"], dec["url"]


def fetch_daily_diavgeia(date: datetime.date):
    """ Fetches all documents from diavgeia for a given date """

    export_root = Path(TemporaryDirectory().name) / date.isoformat()
    LOGGER.info(f"Getting all decisions for: {date.isoformat()}")
    LOGGER.info(f"Export directory: {export_root}")

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
    fetch_daily_diavgeia(args.date)

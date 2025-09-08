import click

from config.diavgeia_config import DiavgeiaConfig
from workers.dispatcher import Dispatcher


@click.command()
@click.option(
    "--date-id",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Date to fetch documents for. Format: YYYY-MM-DD",
)
@click.option("--log-path", type=click.Path(), help="Override log path")
@click.option("--export-path", type=click.Path(), help="Override export path")
@click.option("--download-pdf", type=bool, help="Override download_pdf setting")
@click.option("--crawl-workers", type=int, help="Override crawl_workers setting")
@click.option("--download-workers", type=int, help="Override download_workers setting")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Override log_level setting",
)
def main(**cli_args):
    """Main function"""

    Dispatcher(
        DiavgeiaConfig(**{k: v for k, v in cli_args.items() if v is not None})
    ).execute()


if __name__ == "__main__":
    main()

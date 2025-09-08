import click

from config.diavgeia_config import DiavgeiaConfig
from workers.dispatcher import Dispatcher
from workers.scheduler import Scheduler


@click.command()
@click.option(
    "--date-id",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Date to fetch documents for. Format: YYYY-MM-DD",
)
@click.option("--log-path", type=click.Path(), help="The path for the logs")
@click.option("--export-path", type=click.Path(), help="The path for all downloads")
@click.option(
    "--download-pdf", default=False, type=bool, help="If set to True, download PDFs"
)
@click.option("--download-workers", type=int, help="Number of download workers")
@click.option("--daemon-mode", type=bool, help="Run in daemon mode")
@click.option("--schedule-time", type=str, help="Time to run daily job, eg '04:00')")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Log level",
)
def main(**cli_args):
    """Main function"""

    cli_args = {k: v for k, v in cli_args.items() if v is not None}
    config = DiavgeiaConfig(**cli_args)

    if config.date_id is None and not config.daemon_mode:
        raise ValueError(
            "Either DATE_ID must be provided (--date-id) or DAEMON_MODE (--daemon-mode) must be set"
        )

    if config.date_id is not None:
        Dispatcher(config).execute()
    elif config.daemon_mode:
        Scheduler(config).start_daemon()


if __name__ == "__main__":
    main()

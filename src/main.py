import click
import datetime as dt

from config.diavgeia_config import DiavgeiaConfig
from workers.dispatcher import Dispatcher
from workers.scheduler import Scheduler


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date to fetch documents for. Format: YYYY-MM-DD",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date to fetch documents for. Format: YYYY-MM-DD",
)
@click.option("--log-path", type=click.Path(), help="The path for the logs")
@click.option("--export-path", type=click.Path(), help="The path for all downloads")
@click.option("--download-pdf", type=bool, help="If set to True, download PDFs")
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

    if config.start_date is None and not config.daemon_mode:
        raise ValueError(
            "Either start/end date must be provided or daemon mode must be set"
        )

    if config.start_date is not None:
        if config.end_date is None:
            config = config.model_copy(update={"end_date": config.start_date})
        dates = (
            config.start_date + dt.timedelta(days=i)
            for i in range((config.end_date - config.start_date).days + 1)
        )
        for date_id in dates:
            config = config.model_copy(
                update={"start_date": date_id, "end_date": date_id}
            )
            Dispatcher(config).execute()

    if config.daemon_mode:
        Scheduler(config).start_daemon()


if __name__ == "__main__":
    main()

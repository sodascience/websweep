from typing import List, Optional
import typer
from scraper import __app_name__, __version__
from tkinter import filedialog as fd
from tkinter import Tk
import time

app = typer.Typer()

def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help = "Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return

@app.command(name = "scrape.all")
def start_scraper_ui() -> None:
    """
    Start scraper where entire file is scraped
    """
    print("Starting scraper")

@app.callback(invoke_without_command=True)
@app.command(name = "init")
def init() -> None:
    """
    Initialise the scraper
    """

    print("\nWELCOME to the corporate scraper.\nFollow the instructions to set up the scraper and start scraping.\n")

    time.sleep(1)

    ask_continue = typer.confirm("Select the .csv file with kvk and base url.\nContinue?\n")
    if not ask_continue:
        typer.echo("\nScraper stopped\n")
        return

    Tk().withdraw()
    filename = fd.askopenfilename()

    time.sleep(1)

    print("\nFile {} selected\n".format(filename))

    time.sleep(1)

    print("Scraper is initialised and ready to scrape.\nUse the --help command to see scrape actions.\n")


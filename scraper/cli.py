"""This module provides the RP To-Do CLI."""

from pathlib import Path
from typing import List, Optional
from tkinter import filedialog as fd
from tkinter import Tk
import time
import logging

import typer

from scraper import ERRORS, __app_name__, __version__, config, scraper

app = typer.Typer()


@app.command(name = "init")
def init() -> None:
    """
    Initialise the scraper
    """

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to set up the scraper and start scraping.\n", fg=typer.colors.YELLOW
    )

    logging.basicConfig(filename="logs/scraper.log", level=logging.INFO)

    typer.secho(
        "Logger initialized\n", fg=typer.colors.GREEN
    )
    time.sleep(0.5)

    ask_continue_file = typer.confirm("SELECT the .csv file with kvk and base url\nContinue?\n")
    if not ask_continue_file:
        typer.secho(
            f'Initalisation stopped\n',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    Tk().withdraw()
    source_filename = fd.askopenfilename()

    typer.secho(
        "File {} selected\n".format(source_filename), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    ask_continue_folder = typer.confirm("SELECT a folder to store the scraper output\nContinue?\n")
    if not ask_continue_folder:
        typer.secho(
            f'Initalisation stopped\n',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    Tk().withdraw()
    folder = fd.askdirectory()

    typer.secho(
        "Folder {} selected\n".format(folder), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    data_filename = typer.prompt("Target folder name, ENTER for default", "scraper_data")

    while Path("{}/{}".format(folder, data_filename)).exists():
        typer.secho(
            "Target folder {}/{} does already exist, choose other folder name\n".format(folder, data_filename), fg=typer.colors.RED
        )
        time.sleep(0.5)

        data_filename = typer.prompt("Target folder name", "scraper_data")

    typer.secho(
        "Target folder {}/{} saved\n".format(folder, data_filename), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    app_init_error = config.init_app(source_filename, "{}/{}".format(folder, data_filename))
    if app_init_error:
        typer.secho(
            f'Creating config file failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else :
        typer.secho(f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ", fg=typer.colors.GREEN)


def get_todoer() -> scraper.Worker:
    if config.CONFIG_FILE_PATH.exists():
        source_file_path = config.get_source_file_path(config.CONFIG_FILE_PATH)
    else:
        typer.secho(
            'Config file not found. Please, run "rptodo init"',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    if source_file_path.exists():
        return scraper.Worker(source_file_path)
    else:
        typer.secho(
            'Source file not found. Please, run "scraper init" or use scrape --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)


@app.command()
def scrape() -> None:
    """
    Start scraping
    """

    worker = get_todoer()

    start = time.time()
    
    with open(config.get_source_file_path(config.CONFIG_FILE_PATH), "r") as f:
        f.readline() #header
        urls = [line.split(",") for line in f.readlines()]        
        urls = sorted([(kvk.strip(), f"https://www.{url}/") for url, kvk in urls])
    print(len(urls))

    # Run scraper
    # Start scraper, downloading 20 companies in parallel
    worker.scrape_companies(urls)

    #Read what we did
    with open("data/overview_urls.tsv") as f:
        count = 0
        for line in f:
            if line.split("\t")[4] == "200":
                count += 1
    print(f"Downloaded {count} pages from {len(urls)} urls to level {3} in {time() - start:2.1f} seconds.")



@app.command()
def status() -> None:
    """
    Get status and parameters of current application
    """

    typer.secho(
        '\nScraper status OK\n',
        fg=typer.colors.GREEN,
    )
    typer.secho(
        'Source file: {}'.format(config.get_source_file_path(config.CONFIG_FILE_PATH)),
        fg=typer.colors.YELLOW,
    )
    typer.secho(
        'Target folder: {}\n'.format(config.get_target_folder_path(config.CONFIG_FILE_PATH)),
        fg=typer.colors.YELLOW,
    )
    


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
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return

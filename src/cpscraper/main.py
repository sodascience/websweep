from importlib.resources import path
import typer
from pathlib import Path
from typing import List, Optional
from tkinter import filedialog as fd
from tkinter import Tk
import time
import os
import ndjson
from datetime import date as datelib
from multiprocess import Pool
import sys
from shutil import rmtree
import sqlite3 as sql
import asyncio
from tqdm import tqdm
import tqdm.asyncio
import webbrowser
import regex as re

from .scraper.scraper import Scraper
from .extractor.extractor import Extractor
from .utils.utils import classify_url
from cpscraper import ERRORS, __app_name__, __version__, config

app = typer.Typer()


# Helper method for main callback of Typer app
def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


# Helper for all called CLI methods that need to be provided with a WORKER unit
def _get_scraper() -> Scraper:

    if config.CONFIG_FILE_PATH.exists():
        source_file_path = config.get_source_file_path()
    else:
        typer.secho(
            'Config file not found. Please, run "scraper init" or use scraper --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    if source_file_path.exists():
        return Scraper(target_folder_path = config.get_target_folder_path(), classifier=classify_url)
    else:
        typer.secho(
            'Source file not found. Please, run "scraper init" or use scrape --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

# Helper for all called CLI methods that need to be provided with a WORKER unit
def _get_extractor() -> Extractor:

    #TODO: Implement checks
    return Scraper(target_folder_path = config.get_target_folder_path(), classifier=classify_url)


@app.command(name="config")
def cli_config(
    delete_processed_files: bool = typer.Option(None, help="Delete / Not-Delete extractor processed raw files"),
    target_folder_path: str = typer.Option(None, "--target-folder-path", help="Set new path for scraped data output"), 
    source_file_path: str = typer.Option(None, "--source-file-path", help="Set new path for csv source file")) -> None:
    
    """
    Alter scraper configuration settings
    """

    if delete_processed_files is None and target_folder_path is None and source_file_path is None:
        typer.secho(f"Scraper is configured:", fg=typer.colors.YELLOW)
        typer.secho(f"- scraper config location: {config.CONFIG_FILE_PATH}", fg=typer.colors.YELLOW)
        typer.secho(f"- scraper instance location: {config.get_target_folder_path()}", fg=typer.colors.YELLOW)
        typer.secho(f"- source file location: {config.get_source_file_path()}", fg=typer.colors.YELLOW)
        typer.secho(f"- delete extracted files: {config.get_extractor_delete()}\n", fg=typer.colors.YELLOW)
    else:
        if delete_processed_files is not None:
            if delete_processed_files:
                config._save_extractor_delete(True)
            else:
                config._save_extractor_delete(False)
        if target_folder_path is not None:
            config._save_target_folder(target_folder_path)
        if source_file_path is not None:
            config._save_source_file(source_file_path)
        
        typer.secho(
            f"Config settings saved", fg=typer.colors.GREEN
        )


@app.command(name = "scrape")
def _scrape() -> None:
    """
    Start caching websites
    """
    scrape(config.get_source_file_path())


def scrape(config_file) -> None:
    """
    Start caching websites
    """

    typer.secho(f"Scraper is started with instructions:", fg=typer.colors.YELLOW)
    typer.secho(f"- source file: {config.get_source_file_path()}", fg=typer.colors.YELLOW)
    typer.secho(f"- target folder: {config.get_target_folder_path()}\n", fg=typer.colors.YELLOW)

    worker = _get_scraper()
    
    with open(config_file, "r") as f:
        f.readline() #header
        urls = [line.split(",") for line in f.readlines()]        
        urls = sorted([(kvk.strip(), f"https://www.{url}/") for url, kvk in urls])

    worker.scrape_companies(urls)


@app.command()
def extract(
    start_date: str = typer.Option(None, help="Date on which the files are retreieved and extracted should start extracting"), 
    end_date: str = typer.Option(None, help="Date on which the files are retreieved and extractor should stop extracting")) -> None:
    """
    Start extracting data from the fetched files
    """

    typer.secho(f"Extractor is started with instructions:", fg=typer.colors.YELLOW)
    typer.secho(f"- source folder: {config.get_target_folder_path()}", fg=typer.colors.YELLOW)
    typer.secho(f"- delete extracted files: {config.get_extractor_delete()}\n", fg=typer.colors.YELLOW)

    # TODO: Build check whether the provided dates are conform the format and if not indicate that
    if (start_date is None and end_date is None):
        worker = Extractor(target_folder_path = config.get_target_folder_path(), use_sqlite = False, extractor_delete_files = True)
        worker.extract_companies()
    else:
        typer.secho(f"Given start and/or end date do not conform to the YYYY-MM-DD format, extractor was terminated", fg=typer.colors.RED)
        

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



@app.command(name = "instance")
def scraper_address() -> None:
    """
    Open configured scraper instance folder
    """
    try:
        webbrowser.open('file:////{}'.format(config.current_scraper()))
    except: 
        typer.secho(
            "Could not open scraper instance folder\n", fg=typer.colors.RED
        )



@app.command(name = "restore")
def init(headless: bool = typer.Option(False, help="Run without GUI elements")) -> None:
    """
    Restore configuration of existing scraper instance
    """

    if headless == False:
        try:
            if sys.stdin.isatty():
                headless = False
        except:
            headless = True

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to restore an existing scraper instance.\n", fg=typer.colors.GREEN
    )

    if headless == True:
        typer.secho(
            "headless mode turned on\n", fg=typer.colors.YELLOW
        )
    else:
        typer.secho(
            "headless mode turned off\n", fg=typer.colors.YELLOW
        )

    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm("SELECT a scraper instance folder \nContinue?\n")
        if not ask_continue_folder:
            typer.secho(
                f'Restoring stopped\n',
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except:
            typer.secho(
                "\nGUI Interface failed to load", fg=typer.colors.RED
            )
            folder = typer.prompt("ENTER scraper instance folder base PATH\n")
    else:
        folder = typer.prompt("ENTER scraper instance folder base PATH\n")

    

    app_init_error = config.restore_app(Path(folder))
    if app_init_error:
        typer.secho(f'Restoring scraper instance failed with "{ERRORS[app_init_error]}"', fg=typer.colors.RED)
        typer.secho('The settings file for the given instance is incomplete, does not adhere to the expected format or could not be read.', fg=typer.colors.RED)
        raise typer.Exit(1)
    else :
        typer.secho(f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ", fg=typer.colors.GREEN)





@app.command(name = "init")
def init(headless: bool = typer.Option(False, help="Run without GUI elements")) -> None:
    """
    Initialise a new scraper instance
    """

    if headless == False:
        try:
            if sys.stdin.isatty():
                headless = False
        except:
            headless = True

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to set up a new scraper instance and start scraping.\n", fg=typer.colors.GREEN
    )

    if headless == True:
        typer.secho(
            "headless mode turned on\n", fg=typer.colors.YELLOW
        )
    else:
        typer.secho(
            "headless mode turned off\n", fg=typer.colors.YELLOW
        )

    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm("SELECT a configuration and scraper output storage folder \nContinue?\n")
        if not ask_continue_folder:
            typer.secho(
                f'Initalisation stopped\n',
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except:
            typer.secho(
                "\nGUI Interface failed to load", fg=typer.colors.RED
            )
            folder = typer.prompt("ENTER target folder base PATH\n")
    else:
        folder = typer.prompt("ENTER target folder base PATH\n")
   
    typer.secho(
        "Folder {} selected\n".format(folder), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm("SELECT a source file (.csv) with kvk and url columns \nContinue?\n")
        if not ask_continue_folder:
            typer.secho(
                f'Initalisation stopped\n',
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            file = fd.askopenfilename(
                title="Choose a file",
                filetypes=[('csv files', '.csv')])
        except:
            typer.secho(
                "\nGUI Interface failed to load", fg=typer.colors.RED
            )
            file = typer.prompt("ENTER source file location base PATH\n")
    else:
        file = typer.prompt("ENTER source file location base PATH\n")

   
    typer.secho(
        "Source file {file} selected\n", fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    ask_delete_files = typer.confirm("SELECT to remove raw files after extractor processing?\n")

    typer.secho(
        f"Raw files will be removed: {ask_delete_files}\n", fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    app_init_error = config.init_app(str(folder), str(file), ask_delete_files)
    if app_init_error:
        typer.secho(
            f'Creating config file failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else :
        typer.secho(f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ", fg=typer.colors.GREEN)


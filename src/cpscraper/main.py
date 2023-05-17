import asyncio
import datetime
import os
import sqlite3 as sql
import sys
import time
import typer #TODO: can we just import Typer?
import webbrowser
from datetime import date as datelib
from functools import wraps
from importlib.resources import path
from pathlib import Path
from shutil import rmtree
from tkinter import Tk
from tkinter import filedialog as fd
from typing import List, Optional

from cpscraper import ERRORS, __app_name__, __status__, __version__, config
from .extractor.extractor import Extractor, FirmBackBoneFileExtractor
from .scraper.scraper import Scraper
from .utils.utils import classify_url


app = typer.Typer()


# Wrapping function (decorator) for operating application commands
# Verifies whether the application is ready to receive various operational commands such as scraping and extracting as the application should first be configured
# TODO: This function is of high importance for the stability of the application and should be extended with validity checks
def operate():
    def deco_operate(f):
        @wraps(f)
        def f_operate(*args, **kwargs):

            try:

                if not config.CONFIG_FILE_PATH.exists():
                    typer.secho(
                        'Application config file was not found. Please run "scraper init" or use scraper --help',
                        fg=typer.colors.RED,
                    )
                    return
                elif (
                    config.current_scraper() == config.CONFIG_DIR_PATH
                    or not config.current_scraper().exists()
                ):
                    typer.secho(
                        "Application config file has no instance location pointer. Please initalise or restore an instance or use cpscraper --help",
                        fg=typer.colors.RED,
                    )
                    return
                elif (
                    config.get_source_file_path() is None
                    or not config.get_source_file_path().exists()
                ):
                    typer.secho(
                        "Settings file does not contain essential instance data. Please initalise or restore an instance or use cpscraper --help",
                        fg=typer.colors.RED,
                    )
                    return

                if (
                    f.__name__ == "extract"
                    and not any((config.get_target_folder_path() / "data").iterdir())
                ):
                    typer.secho(
                        'There are no scraped files to extract from. Please start scraping using "scrape" or use cpscraper --help',
                        fg=typer.colors.RED,
                    )
                    return

                return f(*args, **kwargs)

            except:
                if __status__ == "development":
                    raise
                else:
                    typer.secho(
                            'An unexpected error occured, please consult the documentation and usage instructions',
                            fg=typer.colors.RED,
                        )

        return f_operate

    return deco_operate


# Set up a new cpscraper instance
# Creates system application cpscraper folder and creates config.ini file therein
# Stores new cpscraper instance location in the system's application config.ini file
# Stores a newly created settings.ini file in the cpscraper folder
# Can be run at any time and does not need the operation verification
@app.command(name="init")
def init(headless: bool = typer.Option(False, help="Run without GUI elements")) -> None:
    """
    Initialise a new scraper instance.
    The instance location is stored in the application config file,
    a new folder location is created and a setting file is created within this folder.
   
    """

    if headless == False:
        try:
            if sys.stdin.isatty():
                headless = False
        except:
            headless = True

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to set up a new scraper instance and start scraping.\n",
        fg=typer.colors.GREEN,
    )

    if headless == True:
        typer.secho("headless mode turned on\n", fg=typer.colors.YELLOW)
    else:
        typer.secho("headless mode turned off\n", fg=typer.colors.YELLOW)

    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm(
            "SELECT a configuration and scraper output storage folder \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Initalisation stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            folder = typer.prompt("ENTER target folder base PATH\n")
    else:
        folder = typer.prompt("ENTER target folder base PATH\n")

    typer.secho(f"Folder {folder} selected\n", fg=typer.colors.YELLOW)
    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm(
            "SELECT a source file (.csv) with kvk and url columns \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Initalisation stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            file = fd.askopenfilename(
                title="Choose a file", filetypes=[("csv files", ".csv")]
            )
        except:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            file = typer.prompt("ENTER source file location base PATH\n")
    else:
        file = typer.prompt("ENTER source file location base PATH\n")

    typer.secho(f"Source file {file} selected\n", fg=typer.colors.YELLOW)
    time.sleep(0.5)

    ask_delete_files = typer.confirm(
        "SELECT to remove raw files after extractor processing?\n"
    )

    typer.secho(
        f"Raw files will be removed: {ask_delete_files}\n", fg=typer.colors.YELLOW
    )

    time.sleep(0.5)
    
    ask_use_sql = typer.confirm(
        "SELECT do you want to use a SQL (Y) or CSV (n) database?\n"
    )
    
    typer.secho(
        f"A SQL database will be used: {ask_use_sql}\n", fg=typer.colors.YELLOW
    )

    time.sleep(0.5)

    app_init_error = config.init_app(str(folder), str(file), ask_delete_files, ask_use_sql)
    if app_init_error:
        typer.secho(
            f'Creating config file failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else:
        typer.secho(
            "Scraper is initialised and ready to use \nUse the --help command for instructions\n ",
            fg=typer.colors.GREEN,
        )


# Helper method for main callback of Typer app
def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


# Main Typer app callback
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


# Restores an existing cpscraper instance as the active instance which can be worked on
# Creates system application cpscraper folder and creates config.ini file therein
# Stores the existing cpscraper instance location in the system's application config.ini file and overwrites any previous ones
# Verifies whether the expected values are within the settings.ini file
# Can be run at any time and does not need the operation verification
@app.command(name="restore")
def init(headless: bool = typer.Option(False, help="Run without GUI elements")) -> None:
    """
    Restore configuration of existing scraper instance.
    The exisiting location is stored in the application config file and the exisiting settings in the settings file are validated.
    
    """

    if headless == False:
        try:
            if sys.stdin.isatty():
                headless = False
        except:
            headless = True

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to restore an existing scraper instance.\n",
        fg=typer.colors.GREEN,
    )

    if headless == True:
        typer.secho("headless mode turned on\n", fg=typer.colors.YELLOW)
    else:
        typer.secho("headless mode turned off\n", fg=typer.colors.YELLOW)

    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm(
            "SELECT a scraper instance folder \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Restoring stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            folder = typer.prompt("ENTER scraper instance folder base PATH\n")
    else:
        folder = typer.prompt("ENTER scraper instance folder base PATH\n")

    app_init_error = config.restore_app(Path(folder))
    if app_init_error:
        typer.secho(
            f'Restoring scraper instance failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        typer.secho(
            "The settings file for the given instance is incomplete, does not adhere to the expected format or could not be read.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else:
        typer.secho(
            "Scraper is initialised and ready to use \nUse the --help command for instructions\n ",
            fg=typer.colors.GREEN,
        )


# Allows for the modification of the settings.ini file in the active working cpscraper instance folder
# Allows modification of the source file and target folder locations + whether extracted documents are deleted
@app.command(name="config")
def cli_config(
    delete_processed_files: bool = typer.Option(
        None, help="Delete / Not-Delete extractor processed raw files"
    ),
    target_folder_path: str = typer.Option(
        None, "--target-folder-path", help="Set new path for scraped data output"
    ),
    source_file_path: str = typer.Option(
        None, "--source-file-path", help="Set new path for csv source file"
    ),
) -> None:

    """
    Alter scraper configuration settings
    
    """

    if (
        delete_processed_files is None
        and target_folder_path is None
        and source_file_path is None
    ):
        typer.secho("Scraper is configured:", fg=typer.colors.YELLOW)
        typer.secho(
            f"- scraper config location: {config.CONFIG_FILE_PATH}",
            fg=typer.colors.YELLOW,
        )
        typer.secho(
            f"- scraper instance location: {config.get_target_folder_path()}",
            fg=typer.colors.YELLOW,
        )
        typer.secho(
            f"- source file location: {config.get_source_file_path()}",
            fg=typer.colors.YELLOW,
        )
        typer.secho(
            f"- delete extracted files: {config.get_extractor_delete()}\n",
            fg=typer.colors.YELLOW,
        )
    else:
        if delete_processed_files is not None:
            if delete_processed_files:
                config._save_extractor_delete(True)
            else:
                config._save_extractor_delete(False)
        # TODO: Verify what happens when the target folder location is changed, the folder should be moved as well
        if target_folder_path is not None:
            config._save_target_folder(target_folder_path)
        if source_file_path is not None:
            config._save_source_file(source_file_path)

        typer.secho("Config settings saved", fg=typer.colors.GREEN)


# Opens the current cpscraper working folder that is stored in the system's application config.ini file
# Does not work in headless operation mode as it involves GUI commands
@app.command(name="instance")
@operate()
def scraper_address() -> None:
    """
    Open configured scraper instance folder

    """
    try:
        webbrowser.open(f"file:////{config.current_scraper()}")
    except:
        typer.secho("Could not open scraper instance folder\n", fg=typer.colors.RED)


# Starts the scraping of targets from the set source file in the settings.ini file in the active working cpscraper instance settings folder
# Calls a Scraper instance which handles the scraping procedure
# Scraped documents are stored in the active working cpscraper instance folder under 'data'
# Can only be run when the application has been initialised
@app.command(name="scrape")
@operate()
def scrape(
    complement: str = typer.Option(
        None,
        help="Complement the folder with failed pages, takes the scrape date as argument",
    ),
    sock_connect: int = typer.Option(
        None,
        help="Timeout value (ms) for establishing a connection to remote server",
    ),
) -> None:
    """
    Start caching websites
    
    """
    typer.secho("Scraper is started with instructions:", fg=typer.colors.GREEN)

    typer.secho(
        f"- source file: {config.get_source_file_path()}", fg=typer.colors.YELLOW
    )
    typer.secho(
        f"- target folder: {config.get_target_folder_path()}\n", fg=typer.colors.YELLOW
    )


    worker = Scraper(
        target_folder_path=config.get_target_folder_path(), 
        classifier=classify_url, 
        use_sqlite=config.get_use_database(),
        sock_connect=sock_connect
    )

    if complement != None:
        try:
            complement_date = datetime.date.fromisoformat(complement)
        except:
            typer.secho(
            f"Given date does not conform to the YYYY-MM-DD format, scraper was terminated",
            fg=typer.colors.RED,
        )

        worker.scrape_complement_companies(complement_date)

    else:
        with open(config.get_source_file_path(), "r") as f:
            f.readline()
            urls = [line.split(",") for line in f.readlines() if len(line) > 1]
            urls = sorted([(url.strip(), kvk.strip(), f"https://www.{url.strip()}/") for url, kvk in urls])

        worker.scrape_companies(urls)

    typer.secho(f"Scraper finished successfully\n", fg=typer.colors.GREEN)

# Starts the extracting of files in the active working cpscraper instance folder
# Calls an Extractor instance which handles the extracting procedure
# Extracted data is stored in the active working cpscraper instance folder under 'scraped_data'
# Can only be run when the application has been initialised
# Can only be run when the scrape function has been called before and there are extractable documents in the 'data' folder
@app.command(name="extract")
@operate()
def extract(
    start_date: str = typer.Option(
        None,
        help="Date on which the files are retreieved and extracted should start extracting",
    ),
    end_date: str = typer.Option(
        None,
        help="Date on which the files are retreieved and extractor should stop extracting",
    ),
) -> None:
    """
    Start extracting data from the fetched files
    
    """

    typer.secho(f"Extractor is started with instructions:", fg=typer.colors.GREEN)
    typer.secho(
        f"- source folder: {config.get_target_folder_path()}", fg=typer.colors.YELLOW
    )
    typer.secho(
        f"- delete extracted files: {config.get_extractor_delete()}\n",
        fg=typer.colors.YELLOW,
    )

    # TODO: Build check whether the provided dates are conform the format and if not indicate that
    if start_date is None and end_date is None:
        worker = Extractor(
            target_folder_path=config.get_target_folder_path(),
            use_sqlite=config.get_use_database(),
            extractor_delete_files=config.get_extractor_delete(),
            file_extractor=FirmBackBoneFileExtractor

        )
        worker.extract_companies()
    else:
        try:
            start_date = datetime.date.fromisoformat(start_date)
            end_date = datetime.date.fromisoformat(end_date)
        except:
            typer.secho(
            "Given start and/or end date do(es) not conform to the YYYY-MM-DD format, extractor was terminated",
            fg=typer.colors.RED,
        )
 
        worker = Extractor(
            target_folder_path=config.get_target_folder_path(),
            use_sqlite=config.get_use_database(),
            extractor_delete_files=config.get_extractor_delete(),
            start_date=start_date,
            end_date=end_date
        )
        worker.extract_companies()
    
    typer.secho(f"Extractor finished successfully\n", fg=typer.colors.GREEN)
        

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
from .utils.utils import classify_url, Worker
from cpscraper import ERRORS, __app_name__, __version__, config
from functools import wraps

app = typer.Typer()


# Wrapping function (decorator) for operating application commands
# Verifies whether the application is ready to receive various operational commands such as scraping and extracting as the application should first be configured
# TODO: This function is of high importance for the stability of the application and should be extended with validity checks
def operate():
    """
    A decorator to verify whether the application is ready to receive various operational commands.
    This function is of high importance for the stability of the application and should be extended with validity checks.

    Returns:
        deco_operate: callable
            A decorator function that wraps the application commands.

    Example:
        >>> @operate()
        ... def extract() -> None:
        ...     ...

    """
    def deco_operate(f):
        """
        A decorator function that wraps the application commands.

        Parameters"
            f: callable
                The function to be decorated.

        Returns:
            f_operate: callable
                The decorated function.

        """
        @wraps(f)
        def f_operate(*args, **kwargs):
            """
            The decorated function that verifies the application is ready for operation before executing the wrapped function.

            Returns:
                The result of executing the wrapped function if the application is ready.

            Notes:
                - The function checks if the application config file exists, whether it contains a location pointer, and whether the source file exists before allowing an operation.
                - If the wrapped function is 'extract', the function checks if there are scraped files to extract from.
                - If the application is not ready for operation, the function outputs an error message and returns nothing.

            """

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
                and not (config.get_target_folder_path() / "data").exists()
            ):
                typer.secho(
                    'There are no scraped files to extract from. Please start scraping using "scrape" or use cpscraper --help',
                    fg=typer.colors.RED,
                )
                return

            return f(*args, **kwargs)

        return f_operate

    return deco_operate


# Opens the current cpscraper working folder that is stored in the system's application config.ini file
# Does not work in headless operation mode as it involves GUI commands
@app.command(name="instance")
@operate()
def scraper_address() -> None:
    """
    Opens the current cpscraper working folder that is stored in the system's application config.ini file using the default system browser.

    The function requires a graphical user interface (GUI) and will not work in headless operation mode. If the folder cannot be opened, an error message is printed.

    This function is decorated with `@operate()`, which checks whether the application is configured correctly before executing the function.

    Returns:
        None

    """
    try:
        webbrowser.open("file:////{}".format(config.current_scraper()))
    except:
        typer.secho("Could not open scraper instance folder\n", fg=typer.colors.RED)


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
    Modify the settings.ini file in the active working cpscraper instance folder.

    Parameters:
        delete_processed_files : bool, optional
            Delete / Not-Delete extractor processed raw files.
        target_folder_path : str, optional
            Set new path for scraped data output.
        source_file_path : str, optional
            Set new path for csv source file.

    Returns:
        None

    Examples:
        >>> cli_config(delete_processed_files=True, target_folder_path="/path/to/new/folder", source_file_path="/path/to/new/csv")
        Config settings saved

    """

    if (
        delete_processed_files is None
        and target_folder_path is None
        and source_file_path is None
    ):
        typer.secho(f"Scraper is configured:", fg=typer.colors.YELLOW)
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

        typer.secho(f"Config settings saved", fg=typer.colors.GREEN)


# Starts the scraping of targets from the set source file in the settings.ini file in the active working cpscraper instance settings folder
# Calls a Scraper instance which handles the scraping procedure
# Scraped documents are stored in the active working cpscraper instance folder under 'data'
# Can only be run when the application has been initialised
@app.command(name="scrape")
@operate()
def scrape() -> None:
    """
    Start caching websites. 
    This function starts the scraping process of targets from the source file in the settings.ini file of the
    active working cpscraper instance settings folder. It calls a Scraper instance that handles the scraping
    procedure. The scraped documents are stored in the active working cpscraper instance folder under 'data'.
    Note that this function can only be run when the application has been initialized.

    Parameters:
        None

    Returns:
        None

    Example:
        >>> scrape()
        Scraper is started with instructions:
        - source file: /path/to/source/file.csv
        - target folder: /path/to/target/folder/

        # scrape_companies() method in Scraper instance is called with list of urls as input
        # and documents are stored in the target folder path specified

    """


# Starts the extracting of files in the active working cpscraper instance folder
# Calls an Extractor instance which handles the extracting procedure
# Extracted data is stored in the active working cpscraper instance folder under 'scraped_data'
# Can only be run when the application has been initialised
# Can only be run when the scrape function has been called before and there are extractable documents in the 'data' folder
@app.command()
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
    Extract data from previously scraped files.
    This function starts the extracting of files in the active working cpscraper instance folder.
    It calls an Extractor instance which handles the extracting procedure.
    Extracted data is stored in the active working cpscraper instance folder under 'scraped_data'.
    This function can only be run when the application has been initialised.
    It can only be run when the `scrape` function has been called before and there are extractable documents in the 'data' folder.

    Parameters:
        start_date : str, optional
            The date on which to start extracting, formatted as YYYY-MM-DD.
        end_date : str, optional
            The date on which to stop extracting, formatted as YYYY-MM-DD.

    Returns:
        None

    Example: 
        >>> extract(start_date='2022-01-01', end_date='2022-01-31')
        Extractor is started with instructions:
        - source folder: /path/to/target/folder
        - delete extracted files: True

        # given start and/or end date do not conform to the YYYY-MM-DD format, extractor was terminated
    
    """

    typer.secho(f"Extractor is started with instructions:", fg=typer.colors.YELLOW)
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
            use_sqlite=False,
            extractor_delete_files=True,
        )
        worker.extract_companies()
    else:
        typer.secho(
            f"Given start and/or end date do not conform to the YYYY-MM-DD format, extractor was terminated",
            fg=typer.colors.RED,
        )


# Restores an existing cpscraper instance as the active instance which can be worked on
# Creates system application cpscraper folder and creates config.ini file therein
# Stores the existing cpscraper instance location in the system's application config.ini file and overwrites any previous ones
# Verifies whether the expected values are within the settings.ini file
# Can be run at any time and does not need the operation verification
@app.command(name="restore")
def restore(
    headless: Optional[bool] = typer.Option(False, help="Run without GUI elements")
) -> None:
    """
    Restores an existing cpscraper instance as the active instance which can be worked on.
    If the system application cpscraper folder and config.ini file do not exist, they are created. 
    The existing cpscraper instance location is stored in the system's application config.ini file and overwrites any previous ones. 
    Verifies whether the expected values are within the settings.ini file. 

    Parameters:
        headless : Optional[bool], optional
            Boolean flag to run without GUI elements. Default is False.

    Returns:
        None

    Example:
        >>> restore(headless=True) # Runs in headless mode
        "WELCOME to the corporate scraper.
        Follow the instructions to restore an existing scraper instance.
        
        headless mode turned on
        
        Scraper instance <folder_path> selected
        
        Scraper is initialised and ready to use
        Use the --help command for instructions

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
            typer.secho(f"Restoring stopped\n", fg=typer.colors.RED)
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
            f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ",
            fg=typer.colors.GREEN,
        )


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
    a new folder location is created, and a settings file is created within this folder.

    Parameters:
        headless : bool, optional
            If True, runs without GUI elements (default is False).

    Returns:
        None
    
    Example:
        >>> init(headless=True)
        WELCOME to the corporate scraper.
        Follow the instructions to set up a new scraper instance and start scraping.

        headless mode turned on

        Folder <folder_path> selected

        Source file <file_path> selected

        Raw files will be removed: True

        Scraper is initialised and ready to use
        Use the --help command for instructions
        
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
            typer.secho(f"Initalisation stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            folder = typer.prompt("ENTER target folder base PATH\n")
    else:
        folder = typer.prompt("ENTER target folder base PATH\n")

    typer.secho("Folder {} selected\n".format(folder), fg=typer.colors.YELLOW)
    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm(
            "SELECT a source file (.csv) with kvk and url columns \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho(f"Initalisation stopped\n", fg=typer.colors.RED)
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

    typer.secho("Source file {file} selected\n", fg=typer.colors.YELLOW)
    time.sleep(0.5)

    ask_delete_files = typer.confirm(
        "SELECT to remove raw files after extractor processing?\n"
    )

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
    else:
        typer.secho(
            f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ",
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

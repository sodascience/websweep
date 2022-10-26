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

from .scraper.scraper import Scraper
from .extractor.extractor import Extractor
from .utils.utils import classify_url
from cpscraper import ERRORS, __app_name__, __version__, config

app = typer.Typer()


# Helper for scraping
def _create_results(path):
    [id, domain, level, url, date, path] = path
    #folder = _get_folder(path)

    start_time_file = time.perf_counter()
    #website_name = os.path.basename(Path(folder).parents[0])
    # TODO: integrate get scraper into _get_worker method since now no checks are performed if target folders exist
    cached_corporate = Extractor([id, domain, level, url, date, path])
    metadata = cached_corporate.extracting()
    end_time_file = time.perf_counter()
    
    return ({path: end_time_file - start_time_file }, metadata)

# Helper for scraping
def _get_folder(path):
    # Remove hidden files
    next_folder = [_ for _ in os.listdir(path) if not _.startswith(".")][0]
    final_dir = os.path.join(path, next_folder)
    
    # This should be a parameter of the code instead of finding it automatically (TODO). It could use if no parameter is passed.
    if os.path.isdir(final_dir):
        list_of_files = os.listdir(final_dir)
        list_of_files = [_ for _ in list_of_files if not _.startswith(".")]
        list_of_files.sort(reverse=True)
        return os.path.join(final_dir, list_of_files[0])
    else:
        return None


# Helper method for main callback of Typer app
def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


# Helper for all called CLI methods that need to be provided with a WORKER unit
def _get_worker() -> Scraper:
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

    worker = _get_worker()
    
    with open(config_file, "r") as f:
        f.readline() #header
        urls = [line.split(",") for line in f.readlines()]        
        urls = sorted([(kvk.strip(), f"https://www.{url}/") for url, kvk in urls])

    # Run scraper
    # Start scraper, downloading 20 companies in parallel
    worker.scrape_companies(urls)


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


@app.command()
def extract() -> None:
    """
    Start scraping the data from cached website files
    """

    typer.secho(f"Extractor is started with instructions:", fg=typer.colors.YELLOW)
    typer.secho(f"- source folder: {config.get_target_folder_path()}", fg=typer.colors.YELLOW)
    typer.secho(f"- delete extracted files: {config.get_extractor_delete()}\n", fg=typer.colors.YELLOW)

    start = time.time()

    
    i = 0

    file_res = config.get_target_folder_path()  /  ('scraped_data_' + str(datelib.today()) + '.ndjson')
    pdf_file = config.get_target_folder_path(config.CONFIG_FILE_PATH)  /  ('pdf_links_' + str(datelib.today()) + '.ndjson')
    pdf_list = []
    
    Path(file_res).parent.mkdir(parents=True, exist_ok=True)

    # Read file
    if use_sqlite:
        connection = sql.connect(os.path.join(config.get_target_folder_path(config.CONFIG_FILE_PATH),  "overview_urls.db"))
        cursor = connection.cursor()
        results = cursor.execute(f'''SELECT id, domain, level, url, date, path FROM Overview 
                         WHERE (date >= '{date_start}') 
                         AND (date <= '{date_end}') 
                         AND (status == "200")''').fetchall()
        connection.close()
    else:
        with open(os.path.join(config.get_target_folder_path(config.CONFIG_FILE_PATH), "overview_urls.tsv")) as f:
            f.readline() #header
            results = []
            for line in f:
                id, domain, level, url, status, date, path = line.split("\t")
                if (date >= date_start) and (date <= date_end) and (status == "200"):
                    results.append([id, domain, level, url, date, path.strip()])

    # Parallelize loop 
    with Pool() as pool, open(file_res, "w+", encoding='UTF-8') as f_res:
        writer_res = ndjson.writer(f_res, ensure_ascii=False)

        with tqdm.tqdm(total=len(results), leave = True, miniters=1) as pbar:
            for result in pool.imap_unordered(_create_results, results):
                time_dict, json_dict = result
                writer_res.writerow(json_dict)
                pbar.update()
                if json_dict["pdf_links"] != []:
                    print(json_dict['pdf_links'])
                    pdf_list.append(json_dict["pdf_links"])

                # IF you want to only run extract through a part of the dataset, uncomment this and change the if statement
                # i += 1
                # if i == 3500:
                #     break

    with open(pdf_file, "w+", encoding='UTF-8') as pdf_res:
        pdf_writer = ndjson.writer(pdf_res, ensure_ascii=False)
        pdf_writer.writerow(pdf_list)
        
    
    print(f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds.")

    if config.get_extractor_delete():
        data_folder = os.path.join(config.get_target_folder_path(), "data")
        for folder in os.listdir(data_folder):
            if os.path.isdir(folder):
                rmtree(os.path.join(data_folder, folder))


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
        typer.secho(
            f'Creating config file failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
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


import typer
from pathlib import Path
from typing import List, Optional
from tkinter import filedialog as fd
from tkinter import Tk
import time
import os
import json
from datetime import date
from multiprocessing import Pool

from .scraper.scraper import Scraper
from .extractor.extractor import Extractor
from src.cpscraper import ERRORS, __app_name__, __version__, config

app = typer.Typer()


# Helper for scraping
def _create_results(path):
    folder = _get_folder(path)

    start_time_file = time.perf_counter()
    website_name = os.path.basename(Path(folder).parents[0])
    # TODO: integrate get scraper into _get_worker method since now no checks are performed if target folders exist
    cached_corporate = Extractor(working_dir=folder, website=website_name)
    cached_corporate.run_loops()
    end_time_file = time.perf_counter()
    json_dict = cached_corporate.__dict__
    json_dict.pop("working_dir")

    return ({folder: end_time_file - start_time_file }, json_dict)

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
        source_file_path = config.get_source_file_path(config.CONFIG_FILE_PATH)
    else:
        typer.secho(
            'Config file not found. Please, run "scraper init" or use scraper --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    if source_file_path.exists():
        return Scraper(target_folder_path = config.get_target_folder_path(config.CONFIG_FILE_PATH))
    else:
        typer.secho(
            'Source file not found. Please, run "scraper init" or use scrape --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)


# Init command
@app.command(name = "init")
def init() -> None:
    """
    Initialise the scraper
    """

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to set up the scraper and start scraping.\n", fg=typer.colors.YELLOW
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
    source_filename = fd.askopenfilename(filetypes=[("Excel files", ".csv")])

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


@app.command()
def scrape() -> None:
    """
    Start caching websites
    """

    worker = _get_worker()

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


@app.command()
def extract() -> None:
    """
    Start scraping the data from cached website files
    """

    start_time = time.perf_counter()
    test_data_dir = config.get_target_folder_path(config.CONFIG_FILE_PATH) / 'data'  # Get the folder 3 folders up, then add /data/test_data to that filepath
    time_dict = {}
    json_list = []
    i = 0

    file_perm = config.get_target_folder_path(config.CONFIG_FILE_PATH)  /  ('performance_' + str(date.today()) + '.json')
    Path(file_perm).parent.mkdir(parents=True, exist_ok=True)

    file_res = config.get_target_folder_path(config.CONFIG_FILE_PATH)  /  ('scraped_data_' + str(date.today()) + '.json')
    Path(file_res).parent.mkdir(parents=True, exist_ok=True)

    # Create path to files
    files = [os.path.join(test_data_dir, file) for file in os.listdir(test_data_dir) if not file.startswith(".")]
    # Check if they are directories
    files = [file for file in files if os.path.isdir(file)]
    
    # Parallelize loop (it may not work on Windows unless you keep "create_results" in a different file)
    with Pool() as pool, open(file_perm, "w+") as f_perm, open(file_res, "w+") as f_res:
        i = 0
        
        for result in pool.imap_unordered(_create_results, files):
            i += 1
            if i % 100 == 0:
                print(f"Finished {i} files out of {len(files)}")
            time_dict_temp, json_dict = result
            time_dict.update(time_dict_temp)
            json_list.append(json_dict)

        # Write data to file  (TODO: it should be line by line to avoid using update/append. 
        prettify = json.dumps(time_dict, indent=4)
        f_perm.write(prettify)

        prettify = json.dumps(json_list, indent=4)
        f_res.write(prettify)


    end_time = time.perf_counter()
    total_runtime = end_time - start_time
    time_dict["total runtime: "] = total_runtime






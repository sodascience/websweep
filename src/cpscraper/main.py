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
        source_file_path = config.get_source_file_path(config.CONFIG_FILE_PATH)
    else:
        typer.secho(
            'Config file not found. Please, run "scraper init" or use scraper --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    if source_file_path.exists():
        return Scraper(target_folder_path = config.get_target_folder_path(config.CONFIG_FILE_PATH), classifier=classify_url)
    else:
        typer.secho(
            'Source file not found. Please, run "scraper init" or use scrape --help',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)


@app.command(name = "init")
def init(headless: bool = typer.Option(False, help="Run without GUI elements")) -> None:
    """
    Initialise the scraper
    """

    if headless == False:
        try:
            if sys.stdin.isatty():
                headless = False
        except:
            headless = True

    typer.secho(
        "\nWELCOME to the corporate scraper.\nFollow the instructions to set up the scraper and start scraping.\n", fg=typer.colors.GREEN
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
        ask_continue_file = typer.confirm("SELECT the .csv file with kvk and base url\nContinue?\n")
        if not ask_continue_file:
            typer.secho(
                f'Initalisation stopped\n',
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            source_filename = fd.askopenfilename(filetypes=[("Excel files", ".csv")])
        except:
            typer.secho(
                "\nGUI Interface failed to load", fg=typer.colors.RED
            )
            source_filename = typer.prompt("ENTER source file PATH\n")
    else:
        source_filename = typer.prompt("ENTER source file PATH\n")

    # TODO: CHECK if file exists
    typer.secho(
        "File {} selected\n".format(source_filename), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    if headless == False:
        ask_continue_folder = typer.confirm("SELECT a folder to store the scraper output\nContinue?\n")
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

    # TODO: CHECK if folder exists    
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

        data_filename = typer.prompt("ENTER target folder name", "scraper_data")

    typer.secho(
        "Target folder {}/{} saved\n".format(folder, data_filename), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    ask_delete_files = typer.confirm("Remove raw files after extractor processing?\n")

    typer.secho(
        f"Raw files will be removed: {ask_delete_files}\n".format(folder, data_filename), fg=typer.colors.YELLOW
    )
    time.sleep(0.5)

    app_init_error = config.init_app(source_filename, "{}/{}".format(folder, data_filename), ask_delete_files)
    if app_init_error:
        typer.secho(
            f'Creating config file failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else :
        typer.secho(f"Scraper is initialised and ready to use \nUse the --help command for instructions\n ", fg=typer.colors.GREEN)


@app.command(name = "scrape")
def _scrape() -> None:
    """
    Start caching websites
    """
    scrape(config.get_source_file_path(config.CONFIG_FILE_PATH))

def scrape(config_file) -> None:
    """
    Start caching websites
    """
    print(config.CONFIG_FILE_PATH)
    typer.secho(f"Scraper is started with instructions:", fg=typer.colors.YELLOW)
    typer.secho(f"- source file: {config.get_source_file_path(config.CONFIG_FILE_PATH)}", fg=typer.colors.YELLOW)
    typer.secho(f"- target folder: {config.get_target_folder_path(config.CONFIG_FILE_PATH)}", fg=typer.colors.YELLOW)
    typer.secho(f"- delete extracted files: {config.get_extractor_delete(config.CONFIG_FILE_PATH)}\n", fg=typer.colors.YELLOW)

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
    
    if delete_processed_files is not None:
        if delete_processed_files:
            config._save_extractor_delete(True)
        else:
            config._save_extractor_delete(False)
    if target_folder_path is not None:
        config._save_target_folder(target_folder_path)
    if source_file_path is not None:
        config._save_source_file(source_file_path)


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

    file_perm = config.get_target_folder_path(config.CONFIG_FILE_PATH)  /  ('performance_' + str(datelib.today()) + '.json')
    Path(file_perm).parent.mkdir(parents=True, exist_ok=True)

    file_res = config.get_target_folder_path(config.CONFIG_FILE_PATH)  /  ('scraped_data_' + str(datelib.today()) + '.ndjson')
    Path(file_res).parent.mkdir(parents=True, exist_ok=True)

    # Read file
    with open(os.path.join(config.get_target_folder_path(config.CONFIG_FILE_PATH), "overview_urls.tsv")) as f:
        f.readline() #header
        results = []
        for line in f:
            id, domain, level, url, status, date, path = line.split("\t")
            # TODO: filter by date, do this in sqlite
            if status == "200":
                results.append([id, domain, level, url, date, path.strip()])

    try:
        import tika
        # initialize Tika
        tika.initVM()
        # TODO: set up tika log and get data from tika
        # TODO: detect this while setting up the app
        use_tika = True
    except:
        use_tika = False




    # Parallelize loop 
    with Pool() as pool, open(file_perm, "w+") as f_perm, open(file_res, "w+", encoding='UTF-8') as f_res:
        i = 0
        writer_res = ndjson.writer(f_res, ensure_ascii=False)
        writer_perm = ndjson.writer(f_perm, ensure_ascii=False)
        for result in pool.imap_unordered(_create_results, results):
            i += 1
            if i % 100 == 0:
                print(f"Finished {i} files out of {len(results)}")
            time_dict, json_dict = result

            #time_dict.update(time_dict_temp)
            #json_list.append(json_dict)

            # Write data to file  (TODO: it should be line by line to avoid using update/append. 
            #prettify = ndjson.dump(f_perm, [time_dict])+"\n"#, indent=4)
            #prettify = ndjson.dump(prettify, [json_dict])+"\n"#json.dumps(json_list, indent=4)
            writer_res.writerow(json_dict)
            writer_perm.writerow(time_dict)

    if config.get_extractor_delete():
        #TODO Change the hardcoded "data" folder name into something that dynamically grabs folder generated by the scrape function
        data_folder = os.path.join(config.get_target_folder_path(), "data")
        for folder in os.listdir(data_folder):
            rmtree(os.path.join(data_folder, folder))
        
    end_time = time.perf_counter()
    total_runtime = end_time - start_time
    time_dict["total runtime: "] = total_runtime



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






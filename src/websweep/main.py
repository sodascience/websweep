import datetime
import os
import sys
import time
import typer
import webbrowser
from functools import wraps
from pathlib import Path
from typing import Optional

from websweep import ERRORS, __app_name__, __status__, __version__, config
from .extractor.extractor import Extractor
from .crawler.crawler import Crawler
from .consolidator.consolidator import Consolidator
from .utils.backend import resolve_overview_backend
from .utils.source_urls import read_source_urls

try:
    HEADLESS = False
    from tkinter import Tk
    from tkinter import filedialog as fd
except Exception:
    HEADLESS = True

app = typer.Typer()


def _has_crawled_data(target_folder: Path) -> bool:
    """Return ``True`` when the instance has at least one crawled-data artifact."""
    crawled_data = target_folder / "crawled_data"
    return (
        crawled_data.exists()
        and crawled_data.is_dir()
        and any(crawled_data.iterdir())
    )


def _parse_iso_date(value: str, option_name: str):
    """Parse an ISO date string and print a user-friendly CLI error when invalid."""
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        typer.secho(
            f"Invalid {option_name}: '{value}'. Expected YYYY-MM-DD.",
            fg=typer.colors.RED,
        )
        return None


def operate():
    """Validate active instance configuration before running operational commands."""
    def deco_operate(f):
        @wraps(f)
        def f_operate(*args, **kwargs):

            try:

                if not config.CONFIG_FILE_PATH.exists():
                    typer.secho(
                        'Application config file was not found. Please run "websweep init" or use websweep --help',
                        fg=typer.colors.RED,
                    )
                    return
                elif (
                    config.current_websweep_instance() == config.CONFIG_DIR_PATH
                    or not config.current_websweep_instance().exists()
                ):
                    typer.secho(
                        "Application config file has no instance location pointer. Please initalise or restore an instance or use websweep --help",
                        fg=typer.colors.RED,
                    )
                    return
                source_file = config.get_source_file_path()
                if source_file is None or not source_file.exists() or not source_file.is_file():
                    typer.secho(
                        "Settings file does not contain essential instance data. Please initalise or restore an instance or use websweep --help",
                        fg=typer.colors.RED,
                    )
                    return

                target_folder = config.get_target_folder_path()
                if not target_folder.exists() or not target_folder.is_dir():
                    typer.secho(
                        "Configured instance folder does not exist. Please run websweep restore or websweep init.",
                        fg=typer.colors.RED,
                    )
                    return

                if (
                    f.__name__ == "extract"
                    and not _has_crawled_data(target_folder)
                ):
                    typer.secho(
                        'There are no crawled files to extract from. Please start crawling using "crawl" or use websweep --help',
                        fg=typer.colors.RED,
                    )
                    return

                return f(*args, **kwargs)

            except Exception:
                if __status__ == "development":
                    raise
                else:
                    typer.secho(
                            'An unexpected error occured, please consult the documentation and usage instructions',
                            fg=typer.colors.RED,
                        )

        return f_operate

    return deco_operate


@app.command(name="init")
def init(headless: bool = typer.Option(HEADLESS, help="Run without GUI elements")) -> None:
    """
    Initialise a new WebSweep instance.
    The instance location is stored in the application config file,
    a new folder location is created and a setting file is created within this folder.
   
    """

    if not headless:
        try:
            if sys.stdin.isatty():
                headless = False
        except Exception:
            headless = True

    typer.secho(
        "\nWELCOME to WebSweep.\nFollow the instructions to set up a new WebSweep instance and start crawling.\n",
        fg=typer.colors.GREEN,
    )

    if headless:
        typer.secho("headless mode turned on\n", fg=typer.colors.YELLOW)
    else:
        typer.secho("headless mode turned off\n", fg=typer.colors.YELLOW)

    time.sleep(0.5)

    if not headless:
        ask_continue_folder = typer.confirm(
            "SELECT a configuration and WebSweep output storage folder \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Initalisation stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except Exception:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            folder = typer.prompt("ENTER target folder base PATH\n")
    else:
        folder = typer.prompt("ENTER target folder base PATH\n")

    typer.secho(f"Folder {folder} selected\n", fg=typer.colors.YELLOW)
    time.sleep(0.5)

    if not headless:
        ask_continue_folder = typer.confirm(
            "SELECT a source file urls (one url per file, with a header)\nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Initalisation stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            file = fd.askopenfilename(
                title="Choose a file", filetypes=[("csv files", ".csv")]
            )
        except Exception:
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
            "WebSweep is initialised and ready to use \nUse the --help command for instructions\n ",
            fg=typer.colors.GREEN,
        )


def _version_callback(value: bool) -> None:
    """Print version and exit when ``--version`` is requested."""
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
    """Typer root callback."""
    return


@app.command(name="restore")
def restore(headless: bool = typer.Option(HEADLESS, help="Run without GUI elements")) -> None:
    """
    Restore configuration of existing WebSweep instance.
    The exisiting location is stored in the application config file and the exisiting settings in the settings file are validated.
    
    """

    if not headless:
        try:
            if sys.stdin.isatty():
                headless = False
        except Exception:
            headless = True

    typer.secho(
        "\nWELCOME back to WebSweep.\nFollow the instructions to restore an existing WebSweep instance.\n",
        fg=typer.colors.GREEN,
    )

    if headless:
        typer.secho("headless mode turned on\n", fg=typer.colors.YELLOW)
    else:
        typer.secho("headless mode turned off\n", fg=typer.colors.YELLOW)

    time.sleep(0.5)

    if not headless:
        ask_continue_folder = typer.confirm(
            "SELECT a WebSweep instance folder \nContinue?\n"
        )
        if not ask_continue_folder:
            typer.secho("Restoring stopped\n", fg=typer.colors.RED)
            raise typer.Exit(1)
        try:
            Tk().withdraw()
            folder = fd.askdirectory()
        except Exception:
            typer.secho("\nGUI Interface failed to load", fg=typer.colors.RED)
            folder = typer.prompt("ENTER WebSweep instance folder base PATH\n")
    else:
        folder = typer.prompt("ENTER WebSweep instance folder base PATH\n")

    app_init_error = config.restore_app(Path(folder))
    if app_init_error:
        typer.secho(
            f'Restoring WebSweep instance failed with "{ERRORS[app_init_error]}"',
            fg=typer.colors.RED,
        )
        typer.secho(
            "The settings file for the given instance is incomplete, does not adhere to the expected format or could not be read.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    else:
        typer.secho(
            "WebSweep is initialised and ready to use \nUse the --help command for instructions\n ",
            fg=typer.colors.GREEN,
        )


@app.command(name="config")
def cli_config(
    delete_processed_files: bool = typer.Option(
        None, help="Delete / Not-Delete extractor processed raw files"
    ),
    # target_folder_path: str = typer.Option(
    #     None, "--target-folder-path", help="Set new path for crawled data output"
    # ),
    source_file_path: str = typer.Option(
        None, "--source-file-path", help="Set new path for csv source file"
    ),
) -> None:

    """
    Alter WebSweep configuration settings
    
    """

    if (
        delete_processed_files is None
        # and target_folder_path is None
        and source_file_path is None
    ):
        typer.secho("WebSweep is configured:", fg=typer.colors.YELLOW)
        typer.secho(
            f"- WebSweep config location: {config.CONFIG_FILE_PATH}",
            fg=typer.colors.YELLOW,
        )
        typer.secho(
            f"- WebSweep instance location: {config.get_target_folder_path()}",
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
        if source_file_path is not None:
            config._save_source_file(source_file_path)

        typer.secho("Config settings saved", fg=typer.colors.GREEN)


@app.command(name="instance")
@operate()
def websweep_address() -> None:
    """
    Open configured WebSweep instance folder

    """
    try:
        webbrowser.open(f"file:////{config.current_websweep_instance()}")
    except Exception:
        typer.secho("Could not open WebSweep instance folder\n", fg=typer.colors.RED)


@app.command(name="crawl")
@operate()
def crawl(
    complement: str = typer.Option(
        None,
        help="Complement the folder with failed pages, takes the crawl date (e.g. '2019-12-04') as argument",
    ),
    sock_connect: int = typer.Option(
        120,
        help="Timeout in seconds for establishing a connection to remote server.",
    ),
    extract: bool = typer.Option(
        False,
        help="Extract files instead of saving HTML",
    ),
    classification_file: Path = typer.Option(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'utils', 'default_regex.json'),
        help="Path to custom JSON URL classification rules (see default_regex.json).",
    ),
    allow_extensions: str = typer.Option(
        None,
        help="Comma-separated file extensions to allow (e.g. pdf,png), overriding blocked extensions.",
    ),
    block_extensions: str = typer.Option(
        None,
        help="Comma-separated file extensions to block explicitly.",
    ),
    target_temp_folder_path: Path = typer.Option(
        None,
        help="Set new path for temporary storage of crawled data",
    ),

) -> None:
    """
    Start crawling websites.
    
    """
    typer.secho("Crawler is started with instructions:", fg=typer.colors.GREEN)

    typer.secho(
        f"- source file: {config.get_source_file_path()}", fg=typer.colors.YELLOW
    )
    typer.secho(
        f"- target folder: {config.get_target_folder_path()}\n", fg=typer.colors.YELLOW
    )

    if target_temp_folder_path is not None and not Path.exists(target_temp_folder_path):
        typer.secho(
            "Given temporary folder does not exist, Crawler was terminated",
            fg=typer.colors.RED,
        )
        return
    
    if classification_file is not None and not Path.exists(classification_file):
        typer.secho(
            "Given classification file does not exist, Crawler was terminated",
            fg=typer.colors.RED,
        )
        return
    elif complement is not None:
        overview_folder = target_temp_folder_path or config.get_target_folder_path()
        resolved_backend = resolve_overview_backend(
            base_folder=Path(overview_folder),
            use_database=config.get_use_database(),
            override_backend=None,
        )
        worker = Crawler(
            target_folder_path=config.get_target_folder_path(),
            target_temp_folder_path=target_temp_folder_path,
            classification_file_path=classification_file,
            allow_extensions=allow_extensions,
            block_extensions=block_extensions,
            use_database=config.get_use_database(),
            overview_backend=resolved_backend,
            sock_connect=sock_connect,
            extract=extract,
            save_html=not extract,
        )

        try:
            complement_date = datetime.date.fromisoformat(complement)
            worker.crawl_complement_base_urls(complement_date)
        except ValueError:
            typer.secho(
                "Given date does not conform to the YYYY-MM-DD format, Crawler was terminated",
                fg=typer.colors.RED,
            )
            return

        
    else:
        urls = read_source_urls(Path(config.get_source_file_path()))
        if len(urls) == 0:
            typer.secho(
                "No valid URLs found in source CSV. Expected a header with at least a 'url' column.",
                fg=typer.colors.RED,
            )
            return
        typer.secho(
            f"- normalized base URLs to crawl: {len(urls)}",
            fg=typer.colors.YELLOW,
        )

        overview_folder = target_temp_folder_path or config.get_target_folder_path()
        resolved_backend = resolve_overview_backend(
            base_folder=Path(overview_folder),
            use_database=config.get_use_database(),
            override_backend=None,
            urls_count=len(urls),
        )
        typer.secho(
            f"- overview backend: {resolved_backend}",
            fg=typer.colors.YELLOW,
        )

        worker = Crawler(
            target_folder_path=config.get_target_folder_path(),
            target_temp_folder_path=target_temp_folder_path,
            classification_file_path=classification_file,
            allow_extensions=allow_extensions,
            block_extensions=block_extensions,
            use_database=config.get_use_database(),
            overview_backend=resolved_backend,
            sock_connect=sock_connect,
            extract=extract,
            save_html=not extract,
        )

        worker.crawl_base_urls(urls)

    typer.secho("Crawler finished successfully\n", fg=typer.colors.GREEN)

@app.command(name="extract")
@operate()
def extract(
    start_date: str = typer.Option(
        None,
        help="Start date (YYYY-MM-DD) for crawl sessions to extract.",
    ),
    end_date: str = typer.Option(
        None,
        help="End date (YYYY-MM-DD) for crawl sessions to extract.",
    ),
    workers: int = typer.Option(
        None,
        help="Number of worker processes for extraction. Defaults to CPU count.",
    ),
) -> None:
    """
    Start extracting data from fetched files.
    
    """

    typer.secho("Extractor is started with instructions:", fg=typer.colors.GREEN)
    typer.secho(
        f"- source folder: {config.get_target_folder_path()}", fg=typer.colors.YELLOW
    )
    typer.secho(
        f"- delete extracted files: {config.get_extractor_delete()}\n",
        fg=typer.colors.YELLOW,
    )
    resolved_backend = resolve_overview_backend(
        base_folder=Path(config.get_target_folder_path()),
        use_database=config.get_use_database(),
        override_backend=None,
    )
    typer.secho(
        f"- overview backend: {resolved_backend}\n",
        fg=typer.colors.YELLOW,
    )

    if start_date is None and end_date is None:
        worker = Extractor(
            target_folder_path=config.get_target_folder_path(),
            use_database=config.get_use_database(),
            overview_backend=resolved_backend,
            extractor_delete_files=config.get_extractor_delete(),
            workers=workers,

        )
        worker.extract_urls()
    else:
        if (start_date is None) != (end_date is None):
            typer.secho(
                "Please provide both --start-date and --end-date, or neither.",
                fg=typer.colors.RED,
            )
            return

        parsed_start_date = _parse_iso_date(start_date, "--start-date")
        parsed_end_date = _parse_iso_date(end_date, "--end-date")
        if parsed_start_date is None or parsed_end_date is None:
            return
        if parsed_start_date > parsed_end_date:
            typer.secho(
                "--start-date must be earlier than or equal to --end-date.",
                fg=typer.colors.RED,
            )
            return

        worker = Extractor(
            target_folder_path=config.get_target_folder_path(),
            use_database=config.get_use_database(),
            overview_backend=resolved_backend,
            extractor_delete_files=config.get_extractor_delete(),
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            workers=workers,
        )
        worker.extract_urls()
    
    typer.secho("Extractor finished successfully\n", fg=typer.colors.GREEN)
        
@app.command(name="consolidate")
@operate()
def consolidate(
    input_file: Optional[Path] = typer.Option(
        None,
        help="Path to extracted NDJSON file. Defaults to latest file in extracted_data/.",
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        help="Output NDJSON path. Defaults to consolidated_data/consolidated.ndjson.",
    ),
    chunk_size: int = typer.Option(
        10000,
        help="Number of extracted rows processed per consolidation chunk.",
    ),
) -> None:
    """
    Consolidate page-level extracted NDJSON into domain-level NDJSON.
    """
    target_folder = Path(config.get_target_folder_path())
    extracted_dir = target_folder / "extracted_data"

    if input_file is None:
        extracted_files = sorted(
            extracted_dir.glob("*.ndjson"),
            key=lambda p: p.stat().st_mtime,
        )
        if not extracted_files:
            typer.secho(
                "No extracted NDJSON files found. Run websweep extract first.",
                fg=typer.colors.RED,
            )
            return
        input_file = extracted_files[-1]

    if not input_file.exists() or not input_file.is_file():
        typer.secho(
            f"Input file does not exist: {input_file}",
            fg=typer.colors.RED,
        )
        return

    if output_file is None:
        output_file = target_folder / "consolidated_data" / "consolidated.ndjson"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    typer.secho("Consolidator is started with instructions:", fg=typer.colors.GREEN)
    typer.secho(f"- input file: {input_file}", fg=typer.colors.YELLOW)
    typer.secho(f"- output file: {output_file}", fg=typer.colors.YELLOW)
    typer.secho(f"- chunk size: {chunk_size}\n", fg=typer.colors.YELLOW)

    Consolidator(str(input_file), chunk_size=max(1, int(chunk_size))).consolidate(
        str(output_file)
    )

    typer.secho("Consolidator finished successfully\n", fg=typer.colors.GREEN)
        

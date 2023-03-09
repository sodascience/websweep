"""This module provides the Scraper config functionality."""
import sys
import configparser
from pathlib import Path
import os
import typer
from datetime import datetime
from cpscraper import DIR_ERROR, FILE_ERROR, SUCCESS, __app_name__

CONFIG_DIR_PATH = Path(typer.get_app_dir(__app_name__))
CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini" 

# TODO: Provide comments

def _truncate_section(config_file: Path, section: str) -> None:
    config_parser = configparser.ConfigParser()
    
    with open(config_file, "r") as f:
        config_parser.readfp(f)

    config_parser.remove_section(section)

    try:
        with config_file.open("w") as file:
            config_parser.write(file)
    except:
        pass

def current_scraper() -> Path:
    """
    Return the current scraper location.
    This function reads the scraper location from the system's application config.ini file
    and returns it as a Path object. If the scraper location cannot be read, it returns
    the path to the system's application config directory.

    Returns:
        Path: The current scraper location.

    Example:
        >>> current_scraper()
        Path('/home/user/cpscraper')

    """
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE_PATH)
        return Path(config_parser["Scraper"]["location"])
    except:
        return CONFIG_DIR_PATH

def init_app(target_folder_path: str, source_file_path: str, extractor_delete_files: bool, use_database: bool) -> int:
    """Initialize the application.

    Parameters:
        target_folder_path : str
            The path to the target folder.
        source_file_path : str
            The path to the source file.
        extractor_delete_files : bool
            A flag indicating whether to delete the extracted files.
        use_database : bool
            A flag indicating whether to use a database.

    Returns:
        int
            An integer code indicating whether the initialization was successful.
    
    Examples:
        >>> init_app('/path/to/target/folder', '/path/to/source/file', True, False)
        SUCCESS

    """
    # create the application config file location, config file and add the location of the scraper
    config_code = _init_application_config_file(Path(target_folder_path))
    if config_code != SUCCESS:
        return config_code

    # create the scraper folder and create the settings file
    settings_code = _create_settings_file()
    if settings_code != SUCCESS:
        return settings_code   

    # create data folder and add the location of the scraper to the settings file
    target_folder_code = _init_target_folder(Path(target_folder_path))
    if target_folder_code != SUCCESS:
        return target_folder_code

    # add the source file location to the settings file
    source_file_code = _save_source_file(Path(source_file_path))
    if source_file_code != SUCCESS:
        return source_file_code

    extractor_delete_files_code = _save_extractor_delete(extractor_delete_files)
    if extractor_delete_files_code != SUCCESS:
        return extractor_delete_files_code

    use_database = _save_use_database(use_database)
    if use_database != SUCCESS:
        return use_database

    return SUCCESS


def _init_application_config_file(location: Path) -> int:
    try:
        CONFIG_DIR_PATH.mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR
    try:
        Path(CONFIG_FILE_PATH).touch(exist_ok=True)
    except OSError:
        return FILE_ERROR

    _truncate_section(CONFIG_FILE_PATH, "Scraper")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Scraper')
    config_parser.set('Scraper', 'location', str(location))
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _init_target_folder(target_folder_path: Path) -> int:
    try:
        (target_folder_path / "data").mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR

    _truncate_section(target_folder_path / "settings.ini", "Target")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Scraper')
    config_parser.set('Scraper', 'location', str(target_folder_path))
    try:
        with (target_folder_path / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _create_settings_file() -> int:
    try:
        Path(current_scraper() / "settings.ini").touch(exist_ok=True)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def restore_app(target_folder_path: Path) -> int:
    """
    Restore an existing application with the given target folder path.
    This function checks whether the given target folder path exists and contains
    a "settings.ini" file. If both conditions are satisfied, it initializes the
    application configuration file and checks whether the required settings
    (target folder path, source file path, extractor delete flag) are present in
    the settings file. If any of these checks fail, the function returns an error
    code.

    Parameters:
        target_folder_path : Path
            The path to the target folder of the application.

    Returns:
        int
            An integer representing the status code of the restore operation.

    Example:
        >>> restore_app(Path("/path/to/target/folder"))
        SUCCESS

    """

    if not Path.is_dir(target_folder_path):
        return DIR_ERROR
    if not Path.is_file(target_folder_path / "settings.ini"):
        return FILE_ERROR

    # create the application config file location, config file and add the location of the scraper
    config_code = _init_application_config_file(target_folder_path)
    if config_code != SUCCESS:
        return config_code

    try:
        get_target_folder_path()
        get_source_file_path()
        get_extractor_delete()
    except:
        return FILE_ERROR

    return SUCCESS


def get_target_folder_path(config_file: Path = (current_scraper() / "settings.ini")) -> Path:
    """
    Return the path of the current scraper location.

    Parameters:
        config_file : Path, optional
            The path to the configuration file, by default the current scraper's "settings.ini".

    Returns:
        Path
            The path of the current scraper location.

    Example:
        >>> get_target_folder_path(Path("path/to/settings.ini"))
        Path('/path/to/cpscraper/')

    """
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Scraper"]["location"])


def _save_source_file(source_file_path: Path) -> int:
    _truncate_section(current_scraper() / "settings.ini", "Source")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Source')
    config_parser.set('Source', 'source_file', str(source_file_path))
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_source_file_path(config_file: Path = (current_scraper() / "settings.ini")) -> Path:
    """
    Return the current source file path.

    Parameters:
        config_file : Path, optional
            The path to the configuration file (default is the current scraper's settings.ini file).

    Returns:
        Path or None
            The current source file path if it exists in the configuration file, otherwise None.

    Example:
        >>> get_source_file_path()
        Path('/path/to/source_file.csv')

    """
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file)
        return Path(config_parser["Source"]["source_file"])
    except:
        return None

def _save_extractor_delete(extractor_delete_files: bool) -> int:
    _truncate_section(current_scraper() / "settings.ini", "Extractor")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Extractor')
    config_parser.set('Extractor', 'extractor_delete_files', str(extractor_delete_files))
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_extractor_delete(config_file: Path = (current_scraper() / "settings.ini")) -> bool:
    """
    Return whether to delete processed raw files.

    Parameters:
        config_file : Path, optional
            The path to the configuration file (default is current scraper's "settings.ini")

    Returns:
        bool
            Whether to delete processed raw files (True or False)

    Example:
        >>> get_extractor_delete()
        True

    """
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return eval(config_parser["Extractor"]["extractor_delete_files"])

def _save_use_database(use_database: bool) -> int:
    _truncate_section(current_scraper() / "settings.ini", "Database")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Database')
    config_parser.set('Database', 'use_database', str(use_database))
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_use_database(config_file: Path = (current_scraper() / "settings.ini")) -> bool:
    """
    Return whether to use database.

    Parameters:
        config_file : Path object, optional
            The location of the scraper's configuration file, default is `current_scraper() / "settings.ini"`.

    Returns:
        bool
            A boolean value indicating whether to use database.

    Example:
        >>> get_use_database()
        True

    """
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return eval(config_parser["Database"]["use_database"])

def set_last_scrape_date() -> bool:
    """
    Set the last scrape date in the application settings file.

    Returns:
        bool
            A boolean indicating whether the operation was successful or not.

    Example:
        >>> set_last_scrape_date()
        True
        
    """
    _truncate_section(current_scraper() / "settings.ini", "History")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('History')
    config_parser.set('History', 'last_scrape', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


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
    """Return the current scraper location"""
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE_PATH)
        return Path(config_parser["Scraper"]["location"])
    except:
        return CONFIG_DIR_PATH



def init_app(
    target_folder_path: str, source_file_path: str, extractor_delete_files: bool
) -> int:
    """Initialize the application."""

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
    config_parser.add_section("Scraper")
    config_parser.set("Scraper", "location", str(location))
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
    config_parser.add_section("Scraper")
    config_parser.set("Scraper", "location", str(target_folder_path))
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
    """Restore existing application."""

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


def get_target_folder_path(
    config_file: Path = (current_scraper() / "settings.ini"),
) -> Path:
    """Return the current scraper location path"""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Scraper"]["location"])


def _save_source_file(source_file_path: Path) -> int:
    _truncate_section(current_scraper() / "settings.ini", "Source")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Source")
    config_parser.set("Source", "source_file", str(source_file_path))
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def get_source_file_path(
    config_file: Path = (current_scraper() / "settings.ini"),
) -> Path:
    """Return the current source file path"""
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file)
        return Path(config_parser["Source"]["source_file"])
    except:
        return None


def _save_extractor_delete(extractor_delete_files: bool) -> int:
    _truncate_section(current_scraper() / "settings.ini", "Extractor")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Extractor")
    config_parser.set(
        "Extractor", "extractor_delete_files", str(extractor_delete_files)
    )
    try:
        with (current_scraper() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def get_extractor_delete(
    config_file: Path = (current_scraper() / "settings.ini"),
) -> bool:
    """Return whether to delete processed raw files"""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return eval(config_parser["Extractor"]["extractor_delete_files"])


#TODO: @Bjorn, this 3 function was removed in the last merge, but I think they are needed
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


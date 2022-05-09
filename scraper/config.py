"""This module provides the RP To-Do config functionality."""

import configparser
from pathlib import Path

import typer

from scraper import DB_WRITE_ERROR, DIR_ERROR, FILE_ERROR, SUCCESS, __app_name__

CONFIG_DIR_PATH = Path(typer.get_app_dir(__app_name__))
CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini"


def init_app(source_file_path: str, target_folder_path: str) -> int:
    """Initialize the application."""
    config_code = _init_config_file()
    if config_code != SUCCESS:
        return config_code
    source_file_code = _save_source_file(source_file_path)
    if source_file_code != SUCCESS:
        return source_file_code
    target_folder_code = _save_target_folder(target_folder_path)
    if target_folder_code != SUCCESS:
        return target_folder_code
    return SUCCESS


def _init_config_file() -> int:
    try:
        CONFIG_DIR_PATH.mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR
    try:
        CONFIG_FILE_PATH.touch(exist_ok=True)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _save_source_file(source_file_path: str) -> int:
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Source')
    config_parser.set('Source', 'source_file', source_file_path)
    try:
        with CONFIG_FILE_PATH.open("w") as file:
            config_parser.write(file)
    except OSError:
        return DB_WRITE_ERROR
    return SUCCESS

def get_source_file_path(config_file: Path) -> Path:
    """Return the current path to the to-do database."""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Source"]["source_file"])


def _save_target_folder(target_folder_path: str) -> int:
    try:
        Path(target_folder_path).mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR

    config_parser = configparser.ConfigParser()
    config_parser.add_section('Target')
    config_parser.set('Target', 'target_folder', target_folder_path)
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return DB_WRITE_ERROR
    return SUCCESS

def get_target_folder_path(config_file: Path) -> Path:
    """Return the current path to the to-do database."""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Target"]["target_folder"])



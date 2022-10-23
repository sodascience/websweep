"""This module provides the Scraper config functionality."""
import sys
import configparser
from pathlib import Path
import os
import typer
from cpscraper import DIR_ERROR, FILE_ERROR, SUCCESS, __app_name__

CONFIG_DIR_PATH = Path(typer.get_app_dir(__app_name__))
CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini" 

def _truncate_section(section: str) -> None:
    config_parser = configparser.ConfigParser()
    
    with open(CONFIG_FILE_PATH, "r") as f:
        config_parser.readfp(f)

    config_parser.remove_section(section)

    try:
        with CONFIG_FILE_PATH.open("w") as file:
            config_parser.write(file)
    except:
        pass


def init_app(target_folder_path: str, extractor_delete_files: bool) -> int:
    """Initialize the application."""
    
    global CONFIG_DIR_PATH
    CONFIG_DIR_PATH = Path(target_folder_path)
    global CONFIG_FILE_PATH
    CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini" 

    config_code = _init_config_file()
    if config_code != SUCCESS:
        return config_code

    # source_file_code = _save_source_file(source_file_path)
    # if source_file_code != SUCCESS:
    #     return source_file_code

    target_folder_code = _save_target_folder(target_folder_path)
    if target_folder_code != SUCCESS:
        return target_folder_code

    extractor_delete_files_code = _save_extractor_delete(extractor_delete_files)
    if extractor_delete_files_code != SUCCESS:
        return extractor_delete_files_code

    return SUCCESS


def restore_app(target_folder_path: str) -> int:
    """Restore existing application."""
    
    global CONFIG_DIR_PATH
    CONFIG_DIR_PATH = Path(target_folder_path)
    global CONFIG_FILE_PATH
    CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini" 

    if not Path.is_dir(CONFIG_DIR_PATH):
        return DIR_ERROR
    if not Path.is_file(CONFIG_FILE_PATH):
        return FILE_ERROR

    return SUCCESS


def _init_config_file() -> int:
    try:
        Path(CONFIG_DIR_PATH).mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR
    try:
        Path(CONFIG_FILE_PATH).touch(exist_ok=True)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _save_source_file(source_file_path: str) -> int:
    _truncate_section("Source")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Source')
    config_parser.set('Source', 'source_file', source_file_path)
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_source_file_path(config_file: Path = CONFIG_FILE_PATH) -> Path:
    """Return the current path to the to-do database."""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Source"]["source_file"])


def _save_target_folder(target_folder_path: str) -> int:
    _truncate_section("Target")
    try:
        Path(target_folder_path).mkdir(exist_ok=True)
        Path(target_folder_path+"/data").mkdir(exist_ok=True)
    except OSError:
        return DIR_ERROR

    config_parser = configparser.ConfigParser()
    config_parser.add_section('Target')
    config_parser.set('Target', 'target_folder', target_folder_path)
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_target_folder_path(config_file: Path = CONFIG_FILE_PATH) -> Path:
    """Return the current path to the to-do database."""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Target"]["target_folder"])


def _save_extractor_delete(extractor_delete_files: bool) -> int:
    _truncate_section("Extractor")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Extractor')
    config_parser.set('Extractor', 'extractor_delete_files', str(extractor_delete_files))
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS

def get_extractor_delete(config_file: Path = CONFIG_FILE_PATH) -> bool:
    """Return the current path to the to-do database."""
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return eval(config_parser["Extractor"]["extractor_delete_files"])
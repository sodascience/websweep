"""This module provides the WebSweep config functionality."""
import configparser
from pathlib import Path

import typer

from websweep import DIR_ERROR, FILE_ERROR, SUCCESS, __app_name__

CONFIG_DIR_PATH = Path(typer.get_app_dir(__app_name__))
CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini"


def _truncate_section(config_file: Path, section: str) -> None:
    """Remove a section from an INI file when it exists."""
    config_parser = configparser.ConfigParser()

    with open(config_file, "r") as f:
        config_parser.read_file(f)

    config_parser.remove_section(section)

    try:
        with config_file.open("w") as file:
            config_parser.write(file)
    except OSError:
        pass


def current_websweep_instance() -> Path:
    """Return the current websweep location"""
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE_PATH)
        return Path(config_parser["Instance"]["location"])
    except (KeyError, configparser.Error):
        return CONFIG_DIR_PATH



def init_app(
    target_folder_path: str, source_file_path: str, extractor_delete_files: bool, use_database: bool,
) -> int:
    """Initialize the application."""

    # create the application config file location, config file and add the location of the WebSweep instance
    config_code = _init_application_config_file(Path(target_folder_path))
    if config_code != SUCCESS:
        return config_code

    # create the WebSweep instance folder and create the settings file
    settings_code = _create_settings_file()
    if settings_code != SUCCESS:
        return settings_code

    # create data folder and add the location of the WebSweep instance to the settings file
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
    """Create/update the global config file and store active instance location."""
    try:
        CONFIG_DIR_PATH.mkdir(exist_ok=True, parents=True)
    except OSError:
        return DIR_ERROR
    try:
        Path(CONFIG_FILE_PATH).touch(exist_ok=True)
    except OSError:
        return FILE_ERROR

    _truncate_section(CONFIG_FILE_PATH, "Instance")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Instance")
    config_parser.set("Instance", "location", str(location))
    try:
        with CONFIG_FILE_PATH.open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _init_target_folder(target_folder_path: Path) -> int:
    """Initialize instance folders and write the local ``settings.ini`` pointer."""
    try:
        (target_folder_path / "crawled_data").mkdir(exist_ok=True, parents=True)
    except OSError:
        return DIR_ERROR

    _truncate_section(target_folder_path / "settings.ini", "Instance")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Instance")
    config_parser.set("Instance", "location", str(target_folder_path))
    try:
        with (target_folder_path / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def _create_settings_file() -> int:
    """Ensure the active instance has a ``settings.ini`` file."""
    try:
        instance_path = current_websweep_instance()
        instance_path.mkdir(exist_ok=True, parents=True)
        Path(instance_path / "settings.ini").touch(exist_ok=True)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def restore_app(target_folder_path: Path) -> int:
    """Restore existing application."""

    if not Path.is_dir(target_folder_path):
        return DIR_ERROR
    if not Path.is_file(target_folder_path / "settings.ini"):
        return FILE_ERROR

    # create the application config file location, config file and add the location of the WebSweep instance
    config_code = _init_application_config_file(target_folder_path)
    if config_code != SUCCESS:
        return config_code

    try:
        get_target_folder_path()
        get_source_file_path()
        get_extractor_delete()
    except Exception:
        return FILE_ERROR

    return SUCCESS


def get_target_folder_path(
    config_file: Path = None,
) -> Path:
    """Return the current WebSweep instance location path"""
    if config_file is None:
        config_file = current_websweep_instance() / "settings.ini"
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return Path(config_parser["Instance"]["location"])


def _save_source_file(source_file_path: Path) -> int:
    """Persist the source URL file path in ``settings.ini``."""
    _truncate_section(current_websweep_instance() / "settings.ini", "Source")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Source")
    config_parser.set("Source", "source_file", str(source_file_path))
    try:
        with (current_websweep_instance() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def get_source_file_path(
    config_file: Path = None,
) -> Path:
    """Return the current source file path"""
    if config_file is None:
        config_file = current_websweep_instance() / "settings.ini"
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file)
        return Path(config_parser["Source"]["source_file"])
    except (KeyError, configparser.Error):
        return None


def _parse_bool(value, default: bool) -> bool:
    """Parse flexible string/boolean config values with a default fallback."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _save_extractor_delete(extractor_delete_files: bool) -> int:
    """Persist extractor cleanup preference in ``settings.ini``."""
    _truncate_section(current_websweep_instance() / "settings.ini", "Extractor")
    config_parser = configparser.ConfigParser()
    config_parser.add_section("Extractor")
    config_parser.set(
        "Extractor", "extractor_delete_files", str(extractor_delete_files)
    )
    try:
        with (current_websweep_instance() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def get_extractor_delete(
    config_file: Path = None,
) -> bool:
    """
    Return whether to delete processed raw files
    
    """
    if config_file is None:
        config_file = current_websweep_instance() / "settings.ini"
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    value = config_parser.get("Extractor", "extractor_delete_files", fallback=None)
    return _parse_bool(value, default=False)


def _save_use_database(use_database: bool) -> int:
    """Persist whether crawl overview data should use a DB backend."""
    _truncate_section(current_websweep_instance() / "settings.ini", "Database")
    config_parser = configparser.ConfigParser()
    config_parser.add_section('Database')
    config_parser.set('Database', 'use_database', str(use_database))
    try:
        with (current_websweep_instance() / "settings.ini").open("a") as file:
            config_parser.write(file)
    except OSError:
        return FILE_ERROR
    return SUCCESS


def get_use_database(
    config_file: Path = None,
) -> bool:
    """
    Return whether to use an SQL database raw files

    """
    if config_file is None:
        config_file = current_websweep_instance() / "settings.ini"
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    value = config_parser.get("Database", "use_database", fallback=None)
    return _parse_bool(value, default=True)

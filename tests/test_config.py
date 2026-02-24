import configparser

from websweep import SUCCESS
from websweep import config as cfg


def test_init_app_creates_target_folder_and_settings_file(monkeypatch, tmp_path):
    app_config_dir = tmp_path / "app_config"
    app_config_file = app_config_dir / "config.ini"
    target_folder = tmp_path / "fresh_instance"
    source_file = tmp_path / "urls.csv"
    source_file.write_text("url,identifier\nhttps://example.com,example\n", encoding="utf-8")

    monkeypatch.setattr(cfg, "CONFIG_DIR_PATH", app_config_dir)
    monkeypatch.setattr(cfg, "CONFIG_FILE_PATH", app_config_file)

    assert not target_folder.exists()
    code = cfg.init_app(
        target_folder_path=str(target_folder),
        source_file_path=str(source_file),
        extractor_delete_files=False,
        use_database=True,
    )
    assert code == SUCCESS

    assert target_folder.exists()
    assert (target_folder / "settings.ini").exists()
    assert app_config_file.exists()

    app_parser = configparser.ConfigParser()
    app_parser.read(app_config_file)
    assert app_parser["Instance"]["location"] == str(target_folder)

    settings_parser = configparser.ConfigParser()
    settings_parser.read(target_folder / "settings.ini")
    assert settings_parser["Instance"]["location"] == str(target_folder)
    assert settings_parser["Source"]["source_file"] == str(source_file)
    assert settings_parser["Extractor"]["extractor_addon_file"] == ""


def test_restore_app_switches_active_instance(monkeypatch, tmp_path):
    app_config_dir = tmp_path / "app_config"
    app_config_file = app_config_dir / "config.ini"
    instance_a = tmp_path / "instance_a"
    instance_b = tmp_path / "instance_b"
    source_a = tmp_path / "source_a.csv"
    source_b = tmp_path / "source_b.csv"
    source_a.write_text("url,identifier\nhttps://example.com,a\n", encoding="utf-8")
    source_b.write_text("url,identifier\nhttps://example.org,b\n", encoding="utf-8")

    monkeypatch.setattr(cfg, "CONFIG_DIR_PATH", app_config_dir)
    monkeypatch.setattr(cfg, "CONFIG_FILE_PATH", app_config_file)

    assert cfg.init_app(str(instance_a), str(source_a), False, True) == SUCCESS
    assert cfg.init_app(str(instance_b), str(source_b), False, True) == SUCCESS

    assert cfg.current_websweep_instance() == instance_b
    assert cfg.restore_app(instance_a) == SUCCESS
    assert cfg.current_websweep_instance() == instance_a


def test_init_app_persists_extractor_addon_file(monkeypatch, tmp_path):
    app_config_dir = tmp_path / "app_config"
    app_config_file = app_config_dir / "config.ini"
    target_folder = tmp_path / "instance"
    source_file = tmp_path / "urls.csv"
    addon_file = tmp_path / "addon.py"
    source_file.write_text("url,identifier\nhttps://example.com,example\n", encoding="utf-8")
    addon_file.write_text("from websweep.extractor.extractor import FileExtractor\n", encoding="utf-8")

    monkeypatch.setattr(cfg, "CONFIG_DIR_PATH", app_config_dir)
    monkeypatch.setattr(cfg, "CONFIG_FILE_PATH", app_config_file)

    code = cfg.init_app(
        target_folder_path=str(target_folder),
        source_file_path=str(source_file),
        extractor_delete_files=False,
        use_database=True,
        extractor_addon_file=addon_file,
    )
    assert code == SUCCESS
    copied_addon = target_folder / "extractor_addon.py"
    assert cfg.get_extractor_addon_file(target_folder / "settings.ini") == copied_addon
    assert copied_addon.exists()
    assert copied_addon.read_text(encoding="utf-8") == addon_file.read_text(encoding="utf-8")


def test_save_extractor_delete_preserves_addon_path(monkeypatch, tmp_path):
    app_config_dir = tmp_path / "app_config"
    app_config_file = app_config_dir / "config.ini"
    target_folder = tmp_path / "instance"
    source_file = tmp_path / "urls.csv"
    addon_file = tmp_path / "addon.py"
    source_file.write_text("url,identifier\nhttps://example.com,example\n", encoding="utf-8")
    addon_file.write_text("from websweep.extractor.extractor import FileExtractor\n", encoding="utf-8")

    monkeypatch.setattr(cfg, "CONFIG_DIR_PATH", app_config_dir)
    monkeypatch.setattr(cfg, "CONFIG_FILE_PATH", app_config_file)

    assert cfg.init_app(
        target_folder_path=str(target_folder),
        source_file_path=str(source_file),
        extractor_delete_files=False,
        use_database=True,
        extractor_addon_file=addon_file,
    ) == SUCCESS

    assert cfg._save_extractor_delete(True) == SUCCESS
    assert cfg.get_extractor_delete(target_folder / "settings.ini") is True
    assert cfg.get_extractor_addon_file(target_folder / "settings.ini") == (target_folder / "extractor_addon.py")


def test_init_app_persists_storage_path(monkeypatch, tmp_path):
    app_config_dir = tmp_path / "app_config"
    app_config_file = app_config_dir / "config.ini"
    target_folder = tmp_path / "instance"
    source_file = tmp_path / "urls.csv"
    storage_folder = tmp_path / "archive_storage"
    source_file.write_text("url,identifier\nhttps://example.com,example\n", encoding="utf-8")
    storage_folder.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cfg, "CONFIG_DIR_PATH", app_config_dir)
    monkeypatch.setattr(cfg, "CONFIG_FILE_PATH", app_config_file)

    code = cfg.init_app(
        target_folder_path=str(target_folder),
        source_file_path=str(source_file),
        extractor_delete_files=False,
        use_database=True,
        storage_path=storage_folder,
    )
    assert code == SUCCESS
    assert cfg.get_storage_path(target_folder / "settings.ini") == storage_folder

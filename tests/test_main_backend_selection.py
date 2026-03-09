from websweep.utils.backend import resolve_overview_backend


def test_auto_backend_prefers_duckdb_for_large_url_lists(monkeypatch, tmp_path):
    monkeypatch.setattr("websweep.utils.backend.duckdb_available", lambda: True)
    backend = resolve_overview_backend(
        base_folder=tmp_path,
        use_database=True,
        override_backend=None,
        urls_count=10001,
    )
    assert backend == "duckdb"


def test_auto_backend_falls_back_to_sqlite_when_duckdb_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr("websweep.utils.backend.duckdb_available", lambda: False)
    backend = resolve_overview_backend(
        base_folder=tmp_path,
        use_database=True,
        override_backend=None,
        urls_count=10001,
    )
    assert backend == "sqlite"


def test_default_backend_prefers_duckdb(monkeypatch, tmp_path):
    monkeypatch.setattr("websweep.utils.backend.duckdb_available", lambda: True)
    backend = resolve_overview_backend(
        base_folder=tmp_path,
        use_database=True,
        override_backend=None,
    )
    assert backend == "duckdb"


def test_default_backend_uses_csv_when_database_disabled(tmp_path):
    backend = resolve_overview_backend(
        base_folder=tmp_path,
        use_database=False,
        override_backend=None,
    )
    assert backend == "csv"

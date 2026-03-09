from pathlib import Path

from websweep.utils import public_suffix


def test_ensure_public_suffix_list_copies_packaged_file(tmp_path, monkeypatch):
    runtime_path = tmp_path / "runtime_psl.dat"
    monkeypatch.setattr(public_suffix, "PSL_RUNTIME_PATH", runtime_path)
    monkeypatch.setattr(public_suffix, "_env_bool", lambda *_args, **_kwargs: False)

    resolved = public_suffix.ensure_public_suffix_list()

    assert resolved == runtime_path
    assert runtime_path.exists()
    data = runtime_path.read_bytes()
    assert b"BEGIN ICANN DOMAINS" in data


def test_ensure_public_suffix_list_updates_when_enabled(tmp_path, monkeypatch):
    runtime_path = tmp_path / "runtime_psl.dat"
    runtime_path.write_bytes(b"// old\n")

    monkeypatch.setattr(public_suffix, "PSL_RUNTIME_PATH", runtime_path)
    monkeypatch.setattr(public_suffix, "_env_bool", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(public_suffix, "_should_update", lambda *_args, **_kwargs: True)

    def _fake_update(path: Path):
        path.write_bytes(
            b"// ===BEGIN ICANN DOMAINS===\n// ===END PRIVATE DOMAINS===\n"
        )

    monkeypatch.setattr(public_suffix, "_update_runtime_psl", _fake_update)

    resolved = public_suffix.ensure_public_suffix_list()
    assert resolved == runtime_path
    assert b"BEGIN ICANN DOMAINS" in runtime_path.read_bytes()

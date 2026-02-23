import json

from websweep.consolidator.consolidator import Consolidator


def test_consolidator_merges_domains_and_keeps_kvk_btw_separate(tmp_path):
    input_file = tmp_path / "input.ndjson"
    output_file = tmp_path / "output.ndjson"

    rows = [
        {
            "domain": "www.example.com",
            "identifier": "example",
            "phone": ["+311"],
            "email": ["hello@example.com"],
            "fax": [],
            "zipcode": ["1234 AB"],
            "address": ["Main Street 1"],
            "kvk": ["12345678"],
            "btw": ["NL001234567B01"],
            "text": "first page",
        },
        {
            "domain": "shop.example.com",
            "identifier": "example",
            "phone": ["+311"],
            "email": ["sales@example.com"],
            "fax": [],
            "zipcode": ["1234 AB"],
            "address": ["Main Street 1"],
            "kvk": ["12345678"],
            "btw": ["NL001234567B01"],
            "text": "second page",
        },
        {
            "domain": "other.nl",
            "identifier": "other",
            "phone": ["+312"],
            "email": ["info@other.nl"],
            "fax": [],
            "zipcode": ["4321 CD"],
            "address": ["Other Street 2"],
            "kvk": ["87654321"],
            "btw": ["NL765432109B01"],
            "text": "other domain page",
        },
    ]

    with input_file.open("wb") as handle:
        for row in rows:
            handle.write(json.dumps(row).encode("utf-8") + b"\n")

    consolidator = Consolidator(input_file=str(input_file), chunk_size=2)
    consolidator.consolidate(final_output=str(output_file))

    assert output_file.exists()
    output_rows = [json.loads(line.decode("utf-8")) for line in output_file.read_bytes().splitlines() if line]
    assert len(output_rows) == 2

    by_domain = {row["domain"]: row for row in output_rows}
    assert "example.com" in by_domain
    assert "other.nl" in by_domain

    example = by_domain["example.com"]
    assert example["phone"]["+311"] == 2
    assert example["kvk"]["12345678"] == 2
    assert example["btw"]["NL001234567B01"] == 2
    assert "NL001234567B01" not in example["address"]
    assert "12345678" not in example["address"]


def test_clean_domain_handles_multipart_suffix_with_psl(tmp_path):
    input_file = tmp_path / "input.ndjson"
    input_file.write_bytes(b'{"domain":"foo.example.co.uk","identifier":"x"}\n')
    consolidator = Consolidator(input_file=str(input_file), chunk_size=1)
    assert consolidator._clean_domain("foo.example.co.uk") == "example.co.uk"


def test_consolidate_creates_missing_output_parent_directory(tmp_path):
    input_file = tmp_path / "input.ndjson"
    rows = [
        {
            "domain": "www.example.com",
            "identifier": "example",
            "phone": [],
            "email": [],
            "fax": [],
            "zipcode": [],
            "address": [],
            "kvk": [],
            "btw": [],
            "text": "page",
        }
    ]

    with input_file.open("wb") as handle:
        for row in rows:
            handle.write(json.dumps(row).encode("utf-8") + b"\n")

    output_file = tmp_path / "research_output" / "consolidated_data" / "consolidated.ndjson"
    assert not output_file.parent.exists()

    consolidator = Consolidator(input_file=str(input_file), chunk_size=1)
    consolidator.consolidate(final_output=str(output_file))

    assert output_file.parent.exists()
    assert output_file.exists()


def test_consolidator_defaults_to_latest_extracted_and_standard_output(tmp_path):
    target_folder = tmp_path / "research_output"
    extracted_dir = target_folder / "extracted_data"
    extracted_dir.mkdir(parents=True, exist_ok=True)

    old_file = extracted_dir / "extracted_data_2026-01-01_0-1000000.ndjson"
    latest_file = extracted_dir / "extracted_data_2026-02-01_0-1000000.ndjson"

    old_file.write_bytes(
        b'{"domain":"old.example.com","identifier":"old","phone":[],"email":[],"fax":[],"zipcode":[],"address":[],"kvk":[],"btw":[],"text":"old"}\n'
    )
    latest_file.write_bytes(
        b'{"domain":"latest.example.com","identifier":"latest","phone":[],"email":[],"fax":[],"zipcode":[],"address":[],"kvk":[],"btw":[],"text":"latest"}\n'
    )

    # Ensure deterministic "latest" ordering by mtime.
    old_mtime = 1_700_000_000
    latest_mtime = old_mtime + 10
    old_file.touch()
    latest_file.touch()
    old_file.chmod(0o644)
    latest_file.chmod(0o644)
    import os
    os.utime(old_file, (old_mtime, old_mtime))
    os.utime(latest_file, (latest_mtime, latest_mtime))

    consolidator = Consolidator(target_folder_path=target_folder, chunk_size=1)
    consolidator.consolidate()

    output_file = target_folder / "consolidated_data" / "consolidated.ndjson"
    assert output_file.exists()

    rows = [json.loads(line.decode("utf-8")) for line in output_file.read_bytes().splitlines() if line]
    assert len(rows) == 1
    assert rows[0]["domain"] == "example.com"
    assert rows[0]["identifier"] == "latest"


def test_consolidator_infers_output_from_input_file_parent(tmp_path):
    target_folder = tmp_path / "research_output"
    extracted_dir = target_folder / "extracted_data"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    input_file = extracted_dir / "extracted_data_2026-02-01_0-1000000.ndjson"
    input_file.write_bytes(
        b'{"domain":"www.example.com","identifier":"example","phone":[],"email":[],"fax":[],"zipcode":[],"address":[],"kvk":[],"btw":[],"text":"page"}\n'
    )

    consolidator = Consolidator(input_file=input_file, chunk_size=1)
    consolidator.consolidate()

    inferred_output = target_folder / "consolidated_data" / "consolidated.ndjson"
    assert inferred_output.exists()

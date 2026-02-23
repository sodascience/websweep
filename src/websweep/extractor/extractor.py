"""This module provides the Extracter model-controller."""
import os
import shutil
import sqlite3 as sql
import time
import unicodedata
from collections import deque
from datetime import date as datelib
from pathlib import Path
from typing import Optional

import zipfile
from bs4 import BeautifulSoup
try:
    from multiprocess import Pool
except Exception:
    from multiprocessing import Pool

try:
    import tqdm
except Exception:
    class _NoOpProgress:
        def __init__(self, total=None, **_kwargs):
            self.total = total

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def update(self, *_args, **_kwargs):
            return None

    class _NoOpTqdmModule:
        @staticmethod
        def tqdm(*_args, **kwargs):
            return _NoOpProgress(total=kwargs.get("total"))

    tqdm = _NoOpTqdmModule()

try:
    from json_io import append_jsonl
    from backend import resolve_overview_backend
except Exception:
    from ..utils.json_io import append_jsonl
    from ..utils.backend import resolve_overview_backend

try:
    import re2 as re
except Exception:
    import regex as re

ADDRESS_PATTERN = re.compile(
    r"\b([ a-zA-ZÀ-ÿ\-]+\s+[\s0-9-_a-zA-Z]{1,9})[\s\-,\|]{0,5}"
)
ADDRESS_NOISE_PATTERN = re.compile(r"(?i)\b(?:gevestigd|aan|te)\b")


def _parse_html(markup: str) -> BeautifulSoup:
    """Parse HTML with lxml when available, otherwise fall back to html.parser."""
    try:
        return BeautifulSoup(markup, "lxml")
    except Exception:
        return BeautifulSoup(markup, "html.parser")

class FileExtractor:
    """
    A class for extracting data from one specific file.
    This class is used by the extractor pipeline. 
    Custom FileExtractor subclasses can be build, extending the data extracting functionalities.

    Parameters:
        info: tuple
            A tuple containing metadata about the file to extract data from, including the domain, level, website, date and path.

    Methods:
        extracting()
            Initiates the extracting of data from a file at the specified file path.
            Calls extracting_default_metadata() and extract_extended_metadata().
        extract_default_metadata()
            Defines methods that include the default extracting functionalities.
        extract_extended_metadata()
            Defines methods that include the extendable extracting functionalities in subclasses.
        
    """
    def __init__(self, info):
        self.metadata = dict()
        (
            self.metadata["domain"],
            self.metadata["identifier"],
            self.metadata["level"],
            self.metadata["website"],
            self.metadata["date"],
            self.metadata["path"],
        ) = info

        # Filter and include only those methods from self's attributes (dir(self))
        # that are callable (functions) and start with "_extract_", but exclude methods
        # defined in the FileExtractor class (dir(FileExtractor)), meaning that only the custom child methods are included
        self.child_methods = [method for method in dir(self) if callable(getattr(self, method)) and method.startswith('_extract_') and method not in [method for method in dir(FileExtractor) if callable(getattr(FileExtractor, method)) and method.startswith('_extract_')]]

        if isinstance(info[-1], BeautifulSoup):
            self.soup = info[-1]
            self.metadata["path"] = ""
        else:
            # Read HTML to parse
            next_slash = self.metadata["path"].find("/", self.metadata["path"].find("/crawled_data/") + len("/crawled_data/"))
            with zipfile.ZipFile(self.metadata["path"][:next_slash] + ".zip", 'r') as zip_file:
                with zip_file.open(self.metadata["path"][next_slash + 1:]) as file:
                    self.text = file.read().decode("utf-8", "ignore")
                    self.soup = _parse_html(self.text)

    def extracting(self):
        """Extract text and metadata for one page and return a record dictionary."""
        # Get metadata
        self.metadata.update(self._extract_metadata()) #future self.metadata |= self._extract_metadata()

        # Clean the HTML to raw text
        self.text = self._clean_html()

        # Call the method which defines which actions should be taken to extract data
        self.extract_default_metadata()
        # Call all methods that are customly created in any child classes of this basic File Extractor class
        self.extract_custom_metadata()

        # Add the raw text to the metadata at last
        self.metadata["text"] = self.text

        
        return self.metadata

    def extract_default_metadata(self):
        """Populate the built-in default metadata fields."""
        # Core defaults are intentionally conservative.
        # Contact fields (phone/email/fax) are opt-in via custom _extract_* methods.
        self.metadata["zipcode"] = self._extract_zipcode()
        self.metadata["address"] = self._extract_address()

    def extract_custom_metadata(self):
        """Run ``_extract_*`` methods defined by custom subclasses."""
        # Execute all the methods that start with "_extract_" in the name in the child class

        for method in self.child_methods:
            self.metadata[method.split("_extract_")[1]] = getattr(self, method)()


    def _extract_zipcode(self) -> list:
        """
        Extract the zipcode from the input file, and add found zipcodes to self.zipcode in set form
        """

        pattern = re.compile(r"\b(?:NL-)?\d{4}\s?[A-Z]{2}\b")

        # Remove non-feasible endings
        zipcodes = [zipcode for zipcode in set(re.findall(pattern, self.text)) if zipcode[-2:] not in {'SS', 'SD', 'SA'}]

        return zipcodes


    def _extract_address(self) -> list:
        """
        Extract the adres from the input file, and add found adres to self.adres in set form(

        """
        add_found = []

        for zipcode in self.metadata["zipcode"]:
            add, *_ = self.text.partition(zipcode)
            if _[0] == "":
                continue
    
            add = add[-100:].rstrip().rsplit("\n", 2)
            if (len(add[-1]) < 5) and (len(add)>1): #sometimes the postcode is NL-1933XX
                add = add[-2]
            else:
                add = add[-1] 

            matches = re.findall(ADDRESS_PATTERN, add.strip())
            if len(matches) > 0:
                # Remove unwanted words from matches
                filtered_matches = [re.sub(ADDRESS_NOISE_PATTERN, "", match) for match in matches]
                filtered_matches = [match.strip() for match in filtered_matches if match.strip()]
                if filtered_matches:
                    add_found.append(filtered_matches[-1])

        return add_found

    def _extract_metadata(self) -> dict:
        """
        This function is used to extract the metadata from the file, and return it as a dictionary.

        """
        # keep only the most used tags (>10% of a random subset of pages)
        options = {'og:title', 'article:publisher', 'msapplication-TileImage', 'robots', 'og:description', 'author', 'og:image:type', 
                   'format-detection', 'og:image:width', 'msapplication-TileColor', 'twitter:title', 'og:site_name', 'twitter:label1', 
                   'theme-color', 'og:type', 'twitter:description', 'og:image', 'generator', 'encoding', 'twitter:data1', 'og:url', 
                   'twitter:card', 'viewport', 'article:modified_time', 'keywords', 'description', 'og:locale', 'twitter:image', 
                   'google-site-verification', 'og:image:height'}

        metadata = dict()
        for el in self.soup("meta"):
            el = el.attrs
            encoding = el.get("charset")
            if encoding is not None:
                metadata["meta_encoding"] = encoding

            for type in ["name","property"]:
                nam = el.get(type) 
                if (nam is not None) and (nam in options):
                    cont = el.get("content")
                    if cont is not None:
                        metadata[f"meta_{nam}"] = cont
                        break #continue to next element, it won't have name and property

        return metadata

    def _clean_html(self) -> str:
        """Return plain normalized text extracted from BeautifulSoup HTML."""
        text = self.soup.get_text(separator="\n", strip=True)
        return unicodedata.normalize("NFKD", text)


class Extractor:
    """
    A class for extracting data from files and storing it in the target folder.

    Parameters:
        target_folder_path: str
            The path to the folder where the extracted data is stored.
        use_database: bool, optional
            Whether or not to use a database backend (duckdb/sqlite) for the overview file.
            If False, TSV is used. Default is True.
        extractor_delete_files: bool, optional
            Whether or not to delete the original files after extracting data. Default is False.
        file_extractor: FileExtractor, optional
            An custom instance of a FileExtractor class used to extract data from files. Default is None, in which case it will use the default FileExtractor class.

    Methods:
        _create_results(path)
            Extracts the data from one specific file
        extract_urls()
            Start the extracting of all data from the files
    """

    def __init__(
        self,
        target_folder_path,
        use_database=True,
        extractor_delete_files=False,
        start_date="0000-01-01",
        end_date="9999-01-01",
        file_extractor: FileExtractor = None,
        overview_backend: Optional[str] = None,
        workers: Optional[int] = None,
        imap_chunksize: int = 50,
        maxtasksperchild: int = 1000,
        extract_timeout_seconds: int = 10,
        **kwargs,
    ):
        self.target_folder_path = Path(target_folder_path)
        legacy_use_sqlite = kwargs.pop("use_sqlite", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword argument(s): {', '.join(kwargs.keys())}")
        if legacy_use_sqlite is not None:
            use_database = bool(legacy_use_sqlite)
        self.use_database = bool(use_database)

        self.overview_backend = resolve_overview_backend(
            base_folder=self.target_folder_path,
            use_database=self.use_database,
            override_backend=overview_backend,
        )
        self.extractor_delete_files = extractor_delete_files
        self.file_extractor = file_extractor
        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.number_error = 0
        self.workers = max(1, int(workers)) if workers is not None else max(1, (os.cpu_count() or 1))
        self.imap_chunksize = max(1, int(imap_chunksize))
        self.maxtasksperchild = max(1, int(maxtasksperchild))
        self.extract_timeout_seconds = max(1, int(extract_timeout_seconds))

    def _connect_overview_db(self):
        """Open a connection to the configured overview backend database."""
        if self.overview_backend == "duckdb":
            try:
                import duckdb
            except Exception as exc:
                raise RuntimeError(
                    "DuckDB backend requested, but dependency 'duckdb' is not installed."
                ) from exc
            return duckdb.connect(os.path.join(self.target_folder_path, "overview_urls.duckdb"))
        return sql.connect(os.path.join(self.target_folder_path, "overview_urls.db"))

    @staticmethod
    def _error_metadata(path, reason="Error extracting"):
        """Build a minimal metadata record for failed extraction attempts."""
        domain, identifier, level, url, date, _ = path
        return {
            "domain": domain,
            "identifier": identifier,
            "level": level,
            "website": url,
            "date": date,
            "path": reason,
        }

    @staticmethod
    def _is_error_metadata(metadata: dict) -> bool:
        """Return whether a metadata record represents an extraction failure."""
        return metadata.get("path") in {"Error extracting", "Timeout extracting"}

    def _create_results(self, path):
        """Extract one result record from a crawled page metadata tuple."""
        [domain, identifier, level, url, date, stored_path] = path

        try:
            extractor_class = self.file_extractor or FileExtractor
            metadata = extractor_class(
                [domain, identifier, level, url, date, stored_path]
            ).extracting()
            if metadata is None:
                return self._error_metadata(path)
            return metadata
        except Exception:
            return self._error_metadata(path)

    def _iter_chunk_results(self, chunk):
        """Yield extracted records for a chunk with per-task timeout enforcement."""
        if not chunk:
            return

        pending = {}
        to_submit = deque(chunk)
        pool = None

        try:
            pool = Pool(
                processes=self.workers,
                maxtasksperchild=self.maxtasksperchild,
            )

            while to_submit or pending:
                while to_submit and len(pending) < self.workers:
                    item = to_submit.popleft()
                    async_result = pool.apply_async(self._create_results, (item,))
                    pending[async_result] = (item, time.monotonic())

                yielded_result = False
                for async_result, (item, _) in list(pending.items()):
                    if not async_result.ready():
                        continue

                    try:
                        metadata = async_result.get()
                    except Exception:
                        metadata = self._error_metadata(item)

                    del pending[async_result]
                    yield metadata
                    yielded_result = True

                if yielded_result:
                    continue

                now = time.monotonic()
                timed_out = [
                    async_result
                    for async_result, (_, started_at) in pending.items()
                    if (now - started_at) > self.extract_timeout_seconds
                ]
                if timed_out:
                    timed_out_set = set(timed_out)
                    survivors = []
                    for async_result, (item, _) in list(pending.items()):
                        if async_result in timed_out_set:
                            yield self._error_metadata(item, reason="Timeout extracting")
                        else:
                            survivors.append(item)
                        del pending[async_result]

                    # Reset the worker pool to reliably kill stuck tasks and keep
                    # processing the remaining queue without global monkey patching.
                    pool.terminate()
                    pool.join()
                    pool = Pool(
                        processes=self.workers,
                        maxtasksperchild=self.maxtasksperchild,
                    )

                    for item in reversed(survivors):
                        to_submit.appendleft(item)
                    continue

                time.sleep(0.01)
        finally:
            if pool is not None:
                try:
                    pool.close()
                except Exception:
                    pool.terminate()
                pool.join()

    
    def extract_urls(self):
        """Extract all successful crawl rows for the configured date window."""
        start = time.time()

        if self.overview_backend in {"sqlite", "duckdb"}:
            connection = self._connect_overview_db()
            cursor = connection.cursor()
            query = (
                "SELECT domain, identifier, level, url, session_date, path "
                "FROM Overview WHERE (session_date >= ?) AND (session_date <= ?) AND (status == '200')"
            )
            results = cursor.execute(query, (self.start_date, self.end_date)).fetchall()
            connection.close()
        else:
            with open(os.path.join(self.target_folder_path, "overview_urls.tsv")) as f:
                f.readline()  # header
                results = []
                for line in f:
                    domain, identifier, level, url, status, date, _, path = line.split("\t")
                    if (
                        (date >= self.start_date)
                        and (date <= self.end_date)
                        and (status == "200")
                    ):
                        results.append([domain, identifier, level, url, date, path.strip()])
        
        if len(results) == 0:
            print("Extracted data from 0 pages (0 errors) in 0.0 seconds.")
            return

        # chunking in 1M files
        n = 1000000

        with tqdm.tqdm(total=len(results), leave=True, miniters=1) as pbar:
            # chunk output in files of n lines
            for i in range(0, len(results), n):
                file_res = (
                    self.target_folder_path
                    / "extracted_data"
                    / (
                        "extracted_data_"
                        + str(datelib.today())
                        + f"_{i}-{i+n}.ndjson"
                    )
                )
                Path(file_res).parent.mkdir(parents=True, exist_ok=True)

                chunk = results[i : i + n]
                for json_dict in self._iter_chunk_results(chunk):
                    if self._is_error_metadata(json_dict):
                        self.number_error += 1
                    append_jsonl(file_res, [json_dict])
                    pbar.update()

        if self.extractor_delete_files:
            # Loop through all subdirectories in the given folder
            for root, dirs, files in os.walk(self.target_folder_path / "crawled_data"):
                # Delete all files in the current subdirectory
                for dir in dirs:
                    if re.match(r"\d{4}-\d{2}-\d{2}", dir) and dir >= self.start_date and dir <= self.end_date:

                        shutil.rmtree(os.path.join(root, dir))

        print(
            f"Extracted data from {len(results)} pages ({self.number_error} errors) in {time.time() - start:2.1f} seconds."
        )

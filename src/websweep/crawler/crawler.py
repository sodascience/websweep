"""This module provides the Crawler model-controller."""
import csv
from typing import Generator, Iterable, List, Optional
import asyncio
from asyncio import CancelledError
import datetime
import functools
import http.cookies
import os
from time import time
from email.utils import parsedate_to_datetime
import zipfile
import shutil
import sqlite3 as sql
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from urllib.parse import urljoin, urlparse
try:
    import tldextract
except Exception:
    tldextract = None
try:
    import tqdm
except Exception:
    class _NoOpProgress:
        def __init__(self, total=None):
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
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
try:
    from protego import Protego
except Exception:
    class Protego:  # type: ignore
        @classmethod
        def parse(cls, *_args, **_kwargs):
            return cls()

        def can_fetch(self, *_args, **_kwargs):
            return True

import warnings
from bs4.builder import XMLParsedAsHTMLWarning
warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)

http.cookies._is_legal_key = lambda _: True

try:
    from extractor import Extractor
    from utils import clean_url, set_regex, classify_url
    from backend import resolve_overview_backend
    from public_suffix import build_tldextract_extractor
    from json_io import append_jsonl
except Exception:
    from ..extractor.extractor import Extractor
    from ..utils.utils import clean_url, set_regex, classify_url
    from ..utils.backend import resolve_overview_backend
    from ..utils.public_suffix import build_tldextract_extractor
    from ..utils.json_io import append_jsonl

_TLD_EXTRACTOR = None
_TLD_EXTRACTOR_READY = False


def _parse_html(markup: str) -> BeautifulSoup:
    """Parse HTML with lxml when available, otherwise fall back to html.parser."""
    try:
        return BeautifulSoup(markup, "lxml")
    except Exception:
        return BeautifulSoup(markup, "html.parser")


def _needs_public_suffix_resolution(parts: List[str]) -> bool:
    """Return whether a host likely needs PSL-aware domain extraction."""
    # Most domains can be resolved with the last two labels.
    # Use PSL-aware extraction only for likely multi-part country suffixes.
    if len(parts) < 3:
        return False
    return len(parts[-1]) == 2 and len(parts[-2]) <= 3


def _get_tld_extractor():
    """Create and cache a configured tldextract extractor on first use."""
    global _TLD_EXTRACTOR, _TLD_EXTRACTOR_READY
    if _TLD_EXTRACTOR_READY:
        return _TLD_EXTRACTOR
    _TLD_EXTRACTOR_READY = True

    if tldextract is None:
        _TLD_EXTRACTOR = None
        return None

    try:
        _TLD_EXTRACTOR = build_tldextract_extractor(tldextract)
    except Exception:
        _TLD_EXTRACTOR = None
    return _TLD_EXTRACTOR


def _extract_registered_domain(extractor, url: str) -> str:
    """Extract registered domain using whichever tldextract attribute exists."""
    value = extractor(url)
    # tldextract renamed this attribute; keep compatibility across versions.
    registered = (
        getattr(value, "top_domain_under_public_suffix", None)
        or getattr(value, "registered_domain", "")
    )
    return registered.replace("www.", "")


@functools.lru_cache(maxsize=200000)
def _registered_domain(url: str) -> str:
    """Normalize a URL/host into a registered-domain-like key for deduping."""
    host = urlparse(url).netloc or str(url)
    host = host.split("/", 1)[0].split(":", 1)[0].lower().replace("www.", "")
    parts = [part for part in host.split(".") if part]
    if parts and all(part.isdigit() for part in parts):
        return ".".join(parts)

    if _needs_public_suffix_resolution(parts):
        extractor = _get_tld_extractor()
        if extractor is not None:
            registered = _extract_registered_domain(extractor, url)
            if registered:
                return registered

    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def _iter_chunks(items: Iterable, chunk_size: int) -> Generator[List, None, None]:
    """Yield lists of at most ``chunk_size`` items from an iterable."""
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _status_is_success(status) -> bool:
    """Return whether a status value represents HTTP 200."""
    value = str(status).strip()
    if not value:
        return False
    try:
        return int(float(value)) == 200
    except Exception:
        return value == "200"


def _status_is_permanent_not_found(status) -> bool:
    """Return whether a level-0 failure strongly indicates a dead/non-existing site."""
    value = str(status).strip().lower()
    if not value:
        return False
    return value in {
        "dns lookup failed",
        "website not found in __test_domain_robots",
        "website not found",
    }


def _normalize_base_url(raw_url: str) -> Optional[str]:
    """
    Normalize a base URL before crawling.

    - auto-prepend https:// when scheme is missing
    - keep only http(s) URLs
    - skip mailto/tel/mail and other non-web schemes
    """
    url = str(raw_url).strip()
    if not url:
        return None

    lowered = url.lower()
    if lowered.startswith(("mailto:", "mail:", "tel:", "javascript:")):
        return None

    parsed = urlparse(url)
    if parsed.scheme == "":
        return "https://" + url.lstrip("/")

    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    return url


class Crawler:
    """Crawl websites to a bounded depth and store crawl overview plus raw pages."""
    def __init__(
        self,
        target_folder_path,
        target_temp_folder_path=None,
        save_html=True,
        max_level=3,
        classification_file_path=None,
        allow_extensions=None,
        block_extensions=None,
        verify_ssl=False,
        concurrency_base_urls=60,
        threads_bs4=10,
        threads_download=120,
        use_database=True,
        sock_connect=180,
        extract=False,
        headers=None,
        file_extractor=None,
        max_pages_per_domain=50,
        min_days_between_crawls=30,
        chunk_size=1000000,
        overview_backend: Optional[str] = None,
        concurrency_pages: Optional[int] = None,
        page_batch_size: int = 500,
        base_url_batch_size: int = 1000,
        max_concurrency_per_domain: int = 2,
        overview_create_indexes: Optional[bool] = None,
        duckdb_deduplicate: bool = False,
        storage_path: Optional[Path] = None,
        **kwargs,
    ):
        self.target_folder_path = Path(target_folder_path)
        
        if target_temp_folder_path is None:
            self.target_temp_folder_path = self.target_folder_path
        else:
            self.target_temp_folder_path = Path(target_temp_folder_path)
        
        self.base_path = self.target_folder_path / "crawled_data"
        self.base_temp_path = self.target_temp_folder_path / "crawled_data"
        self.storage_path = (
            Path(storage_path).expanduser().resolve()
            if storage_path is not None
            else None
        )
        self.base_archive_path = (
            self.storage_path / "crawled_data"
            if self.storage_path is not None
            else self.base_path
        )

        if save_html:
            Path(self.base_path).mkdir(parents=True, exist_ok=True)
            Path(self.base_temp_path).mkdir(parents=True, exist_ok=True)
            Path(self.base_archive_path).mkdir(parents=True, exist_ok=True)
        else:
            Path(self.base_path).parent.mkdir(parents=True, exist_ok=True)
            Path(self.base_temp_path).parent.mkdir(parents=True, exist_ok=True)

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
        if self.overview_backend == "sqlite":
            self.overview_path = f"{self.target_folder_path}/overview_urls.db"
        elif self.overview_backend == "duckdb":
            self.overview_path = f"{self.target_folder_path}/overview_urls.duckdb"
        else:
            self.overview_path = f"{self.target_folder_path}/overview_urls.tsv"

        # Storage knobs:
        # - DuckDB defaults to append-only inserts (no dedupe query, no secondary indexes)
        #   for sustained high-volume crawls.
        # - SQLite keeps historical behavior.
        if overview_create_indexes is None:
            self.overview_create_indexes = self.overview_backend == "sqlite"
        else:
            self.overview_create_indexes = bool(overview_create_indexes)
        self.duckdb_deduplicate = bool(duckdb_deduplicate and self.overview_backend == "duckdb")

        self._duckdb_insert_sql = "INSERT INTO Overview VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        if self.duckdb_deduplicate:
            self._duckdb_insert_sql = """
                INSERT INTO Overview
                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM Overview
                    WHERE domain = ? AND url = ? AND session_date = ? AND status = ?
                )
            """

        # Keep DuckDB writes on a single persistent connection per crawl run.
        self._duckdb_connection = None
        self._duckdb_cursor = None
        self._duckdb_batch = []
        self._duckdb_batch_size = 500
        self.io_executor = None

        self.save_html = save_html
        self.max_level = max_level
        self.max_pages_per_domain = max_pages_per_domain
        self.min_days_between_crawls = min_days_between_crawls

        # Create file tracking downloaded packages
        self.__create_overview_file()

        # Avoid error in SSL certificates
        self.verify_ssl = verify_ssl
        (
            self.url_regex_mail,
            self.negative_regex,
            self.url_regex,
            self.allowed_extensions,
            self.blocked_extensions,
        ) = set_regex(
            classification_file_path=classification_file_path,
            allow_extensions=allow_extensions,
            block_extensions=block_extensions,
        )
        self.classifier = classify_url

        # Base urls processed in parallel
        self.concurrency_base_urls = max(1, int(concurrency_base_urls))
        self.sem_num_comps = None
        self.concurrency_pages = (
            threads_download if concurrency_pages is None else max(1, int(concurrency_pages))
        )
        self.max_concurrency_per_domain = max(1, int(max_concurrency_per_domain))
        self.sem_pages = None
        self.page_batch_size = max(1, int(page_batch_size))
        self.base_url_batch_size = max(1, int(base_url_batch_size))
        self.threads_bs4 = threads_bs4
        self.threads_download = threads_download
        self.sock_connect = sock_connect
        self.domain_wait_decay = 0.7
        self.domain_wait_on_429 = 2.0
        self.domain_wait_on_403 = 1.0
        self.domain_wait_on_transient_error = 0.75
        self.max_domain_wait = 8.0
        self.retry_waits = (0, 2, 5)

        self.waits = dict()
        self.domain_semaphores = dict()
        self.errors_website = dict()
        if headers is None:
            self.headers = {
                'User-Agent': 'WebSweep/1.0 (+https://github.com/odissei-data/websweep; crawler)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                "Cookie": "cookielawinfo-checkbox-necessary=yes; cookielawinfo-checkbox-functional=no; cookielawinfo-checkbox-performance=no; cookielawinfo-checkbox-analytics=no; cookielawinfo-checkbox-advertisement=no; cookielawinfo-checkbox-others=no; CookieLawInfoConsent=eyJuZWNlc3NhcnkiOnRydWUsImZ1bmN0aW9uYWwiOmZhbHNlLCJwZXJmb3JtYW5jZSI6ZmFsc2UsImFuYWx5dGljcyI6ZmFsc2UsImFkdmVydGlzZW1lbnQiOmZhbHNlLCJvdGhlcnMiOmZhbHNlfQ==; viewed_cookie_policy=yes; optiMonkClientId=f299334f-0413-e0e3-489b-d0ae48a7beb5",
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-GPC': '1',
            }
        else:
            self.headers = headers
        
        # self.start = time()
        self.crawler_session_date = self.__get_current_date()
        self.crawler_session_date_hour = self.__get_current_date("%Y-%m-%d-%H%M")
        self.count_downloads = 0
        self.chunk_size = chunk_size

        # extract data at the same time (no saving raw data)
        if not extract:
            self.extractor_unit = None
        else:
            self.extractor_unit = Extractor(
                target_folder_path = self.target_temp_folder_path,
                file_extractor=file_extractor)
            # save json to file (right now one big file)
            self.file_res_temp_path = (
                self.target_temp_folder_path
                / "extracted_data"
            )
            self.file_res_path = (
                self.target_folder_path
                / "extracted_data"
            )
            Path(self.file_res_temp_path).mkdir(parents=True, exist_ok=True)
            Path(self.file_res_path).mkdir(parents=True, exist_ok=True)
            self.file_res = self.file_res_temp_path / f"extracted_data_{self.crawler_session_date_hour}_{0}_{chunk_size}.ndjson"

    def _connect_overview_db(self):
        """Open a DB connection for the configured overview backend."""
        if self.overview_backend == "duckdb":
            try:
                import duckdb
            except Exception as exc:
                raise RuntimeError(
                    "DuckDB backend requested, but dependency 'duckdb' is not installed."
                ) from exc
            return duckdb.connect(self.overview_path)
        return sql.connect(self.overview_path)

    def _open_duckdb_overview_writer(self):
        """Open persistent DuckDB writer connection for this crawl run."""
        if self.overview_backend != "duckdb":
            return
        if self._duckdb_connection is not None:
            return
        self._duckdb_connection = self._connect_overview_db()
        self._duckdb_cursor = self._duckdb_connection.cursor()
        self._duckdb_batch = []

    def _flush_duckdb_overview_batch(self, force=False):
        """Flush buffered overview rows to DuckDB in one transaction."""
        if self.overview_backend != "duckdb":
            return
        if self._duckdb_connection is None or self._duckdb_cursor is None:
            return
        if not self._duckdb_batch:
            return
        if (not force) and (len(self._duckdb_batch) < self._duckdb_batch_size):
            return

        self._duckdb_cursor.executemany(self._duckdb_insert_sql, self._duckdb_batch)
        self._duckdb_connection.commit()
        self._duckdb_batch = []

    def _build_overview_row(self, domain, identifier, level, url, status, date, path):
        """Build one overview row tuple and optional dedupe suffix for DuckDB."""
        row = (
            domain,
            identifier,
            level,
            url,
            status,
            self.crawler_session_date,
            date,
            path,
        )
        if self.duckdb_deduplicate:
            return row + (domain, url, self.crawler_session_date, status)
        return row

    def _close_duckdb_overview_writer(self):
        """Flush and close persistent DuckDB writer connection."""
        if self.overview_backend != "duckdb":
            return
        if self._duckdb_connection is None:
            return
        try:
            self._flush_duckdb_overview_batch(force=True)
            try:
                # Ensure WAL/checkpoint state is finalized for this run.
                self._duckdb_connection.execute("CHECKPOINT")
            except Exception:
                pass
        finally:
            try:
                self._duckdb_connection.close()
            finally:
                self._duckdb_connection = None
                self._duckdb_cursor = None
                self._duckdb_batch = []


    def get_urls(self, r, url, domain, level, identifier):
        """
        Parse code and return content and urls. Defined it here to be able to pickle it and process it in a thread pool.
        """
        # resp.content is a byte array, convert to string
        contents = r.decode("utf-8", "ignore")
        
        # parse
        soup = _parse_html(contents)
        
        # extract urls from html code in beautiful soup
        # <a href="http://www.google.com/">Google</a>
        urls = [a.attrs.get("href") for a in soup.select("a[href]")]
        
        # filter out urls from other domains
        # create base url
        url_parsed = urlparse(url)
        base_url = url_parsed.scheme + "://" + url_parsed.netloc
        
        # add netloc/schema when missing
        urls = [urljoin(base_url, url_found) for url_found in urls]
        
        # keep only the urls found within the same domain
        urls = [
            url_found
            for url_found in urls
            if _registered_domain(url_found)
            == domain # compare to domain, allows for redirects
        ]

        # remove query string # bol.com/nl/producten/product/...?p=1
        urls = [urlparse(url_found)._replace(query="", fragment="").geturl() for url_found in urls]

        
        if self.extractor_unit is not None:
            date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            json_dict = self.extractor_unit._create_results([base_url, identifier, level, url, date, soup])

            # Save to the correct file
            i = self.chunk_size * (self.count_downloads // self.chunk_size)
            self.file_res = self.file_res_temp_path / f"extracted_data_{self.crawler_session_date_hour}_{i}_{i+self.chunk_size}.ndjson"

            append_jsonl(self.file_res, [json_dict])


        return urls

    def __create_overview_file(self):
        """
        This function creates an overview file with the required headers if it doesn't exist.
        If the overview backend is `sqlite` or `duckdb`, it creates a database with the required
        table and indexes. If the backend is `csv`, it creates a tab-separated file with headers.
        
        Parameters:
            self (object): The object instance.
        
        Returns:
            None.
        """
        if self.overview_backend in {"sqlite", "duckdb"}:
            connection = self._connect_overview_db()
            cursor = connection.cursor()
            if self.overview_backend == "duckdb" and not self.duckdb_deduplicate:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS Overview
                    (domain TEXT, identifier TEXT, level INT, url TEXT, status TEXT, session_date TEXT, crawl_date TEXT, path TEXT);
                    """
                )
            else:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS Overview
                    (domain TEXT, identifier TEXT, level INT, url TEXT, status TEXT, session_date TEXT, crawl_date TEXT, path TEXT,
                    UNIQUE (domain, url, session_date, status));
                    """
                )
            if self.overview_create_indexes:
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS index_date ON Overview (crawl_date);")
                    cursor.execute("CREATE INDEX IF NOT EXISTS index_status ON Overview (status);")
                except Exception:
                    # Not all backends support IF NOT EXISTS for indexes equally.
                    pass
            connection.commit()
            connection.close()

        else:
            # Check if overview file exists, if not create it
            if not Path(self.overview_path).is_file():
                with open(self.overview_path, "w") as f:
                    f.write("domain\tidentifier\tlevel\turl\tstatus\tsession_date\tscrape_date\tpath\n")

    def __update_overview_file(self, domain, level, url, identifier, status, path):

        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if (":" in url[:6]) and (url[:4] != "http"):  # tel: or mailto:
            domain = url
        else:
            domain = _registered_domain(url)

        if self.overview_backend in {"sqlite", "duckdb"}:
            if self.overview_backend == "duckdb" and self._duckdb_connection is not None:
                self._duckdb_batch.append(
                    self._build_overview_row(domain, identifier, level, url, status, date, path)
                )
                self._flush_duckdb_overview_batch(force=False)
                return

            # opening the file is fast (0.00x per query), minimum gain to keep it open (and potential trouble with threading)
            connection = self._connect_overview_db()
            cursor = connection.cursor()

            if self.overview_backend == "sqlite":
                cursor.execute(
                    "INSERT OR IGNORE INTO Overview VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (domain, identifier, level, url, status, self.crawler_session_date, date, path),
                )
            else:
                cursor.execute(
                    self._duckdb_insert_sql,
                    self._build_overview_row(domain, identifier, level, url, status, date, path),
                )

            connection.commit()
            connection.close()

        else:
            with open(self.overview_path, "a+") as f:
                f.write(f"{domain}\t{identifier}\t{level}\t{url}\t{status}\t{self.crawler_session_date}\t{date}\t{path}\n")

    def __save_to_disk(self, path, contents):
        """
        Save all data to disk.
        """
        # create folder if it doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # write raw contents to file
        with open(path, "wb") as f:
            f.write(contents)

    @staticmethod
    def _build_page_relative_path(domain: str, url: str, current_date: str) -> str:
        """Build a stable relative path for one crawled page within its domain zip."""
        parsed = urlparse(url)
        netloc = parsed.netloc.replace("www.", "")
        if url.endswith("/"):
            leaf = url[:-1].split("/")[-1]
        else:
            leaf = url.split("/")[-1]
        if leaf == "":
            leaf = "index"
        rel = f"{domain}/{netloc}/{current_date}/{leaf}"
        return rel.replace(" ", "_")

    def _archive_domain_folder_sync(self, domain: str) -> None:
        """Zip one domain folder from fast storage and archive the zip to final storage."""
        base_url_folder = self.base_temp_path / domain
        if not base_url_folder.exists():
            return

        zip_fast = self.base_path / f"{domain}.zip"
        zip_archive = self.base_archive_path / f"{domain}.zip"
        zip_fast.parent.mkdir(parents=True, exist_ok=True)
        zip_archive.parent.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_fast, "w", zipfile.ZIP_LZMA, allowZip64=True) as zf:
            for dirname, _subdirs, files in os.walk(base_url_folder):
                for filename in files:
                    file_path = os.path.join(dirname, filename)
                    arcname = os.path.relpath(file_path, base_url_folder)
                    zf.write(file_path, arcname)

        shutil.rmtree(base_url_folder)

        if zip_fast.resolve() != zip_archive.resolve():
            try:
                if zip_archive.exists():
                    zip_archive.unlink()
                shutil.move(str(zip_fast), str(zip_archive))
            except Exception:
                # Keep local zip when archive move fails to avoid data loss.
                pass


    def __get_current_date(self, fmt="%Y-%m-%d"):
        # return current day in format "YYYY-MM-DD"
        return datetime.datetime.now().strftime(fmt)

    def _parse_retry_after_seconds(self, value):
        """Parse Retry-After header value into seconds (best-effort)."""
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if raw.isdigit():
            return float(raw)
        try:
            dt = parsedate_to_datetime(raw)
        except Exception:
            return None
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(datetime.timezone.utc)
        delta = (dt - now).total_seconds()
        if delta <= 0:
            return 0.0
        return float(delta)

    def _status_code(self, status):
        """Return integer status code when possible, else ``None``."""
        value = str(status).strip()
        if not value:
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    def _update_domain_wait(self, wait_key, status, retry_after_seconds=None):
        """
        Update per-domain backoff wait using response outcome.

        Strategy:
        - 429: strong backoff (use Retry-After when provided)
        - 403: medium backoff
        - transient errors/5xx: modest backoff
        - success/other statuses: decay wait so queues recover quickly
        """
        current_wait = float(self.waits.get(wait_key, 0.0))
        code = self._status_code(status)
        status_text = str(status).lower()

        if code == 429:
            target = retry_after_seconds
            if target is None:
                target = max(self.domain_wait_on_429, current_wait * 2.0)
            new_wait = min(self.max_domain_wait, max(0.0, float(target)))
        elif code == 403:
            new_wait = min(
                self.max_domain_wait,
                max(self.domain_wait_on_403, current_wait * 1.6 + 0.25),
            )
        elif code is not None and code >= 500:
            new_wait = min(
                self.max_domain_wait,
                max(self.domain_wait_on_transient_error, current_wait * 1.3 + 0.15),
            )
        elif (
            "timeout" in status_text
            or "connection failed" in status_text
            or "request failed" in status_text
            or "dns lookup failed" in status_text
        ):
            new_wait = min(
                self.max_domain_wait,
                max(self.domain_wait_on_transient_error, current_wait * 1.2 + 0.1),
            )
        else:
            new_wait = max(0.0, current_wait * self.domain_wait_decay)

        self.waits[wait_key] = new_wait

            
    async def __fetch_one_url(self, domain, url, identifier, level, state_key=None):
        if state_key is None:
            state_key = domain
        wait_key = domain
        domain_sem = self.domain_semaphores.get(wait_key)
        if domain_sem is None:
            domain_sem = asyncio.Semaphore(self.max_concurrency_per_domain)
            self.domain_semaphores[wait_key] = domain_sem

        async with domain_sem:
            async with self.sem_pages:
                flag_download = self.classifier(
                    url,
                    level,
                    self.url_regex_mail,
                    self.negative_regex,
                    self.url_regex,
                    self.allowed_extensions,
                    self.blocked_extensions,
                )

                # classify url to see if it should be crawled
                if not flag_download:
                    return []

                # Respect adaptive per-domain wait (non blocking).
                current_wait = self.waits.get(wait_key, 0.0)
                if current_wait > 0:
                    await asyncio.sleep(current_wait)

                try_number = 0
                status = "One URL: Too many errors in domain"
                path = ""

                while (try_number < len(self.retry_waits)) and (self.errors_website[state_key] < 20):
                    if try_number == 0:
                        urls, status, path, retry_after_seconds = await self.__fetch_one_url_wrapped(
                            domain, url, identifier, level, self.session, state_key=state_key
                        )
                    else:
                        wait_before_retry = max(
                            self.retry_waits[try_number],
                            float(self.waits.get(wait_key, 0.0)),
                        )
                        if wait_before_retry > 0:
                            await asyncio.sleep(wait_before_retry)

                        # If failure, give an individual session
                        async with ClientSession(
                            headers=self.headers,
                            trust_env=True,
                            connector=TCPConnector(
                                limit=1,  # number of websites/request in parallel
                                ssl=self.verify_ssl,
                                ttl_dns_cache=0,
                                force_close=True,
                            ),
                            timeout=ClientTimeout(total=None, sock_connect=self.sock_connect, sock_read=300),
                        ) as session:
                            urls, status, path, retry_after_seconds = await self.__fetch_one_url_wrapped(
                                domain, url, identifier, level, session, state_key=state_key
                            )

                    self._update_domain_wait(
                        wait_key,
                        status,
                        retry_after_seconds=retry_after_seconds,
                    )
                    try_number += 1

                    # No error
                    if urls is not None:
                        self.__update_overview_file(domain, level, url, identifier, status, path)
                        return urls

                # Error (also by domain)
                self.__update_overview_file(domain, level, url, identifier, status, path)
                return []

         
    async def __fetch_one_url_wrapped(self, domain, url, identifier, level, session, state_key=None):
        """Fetch one URL and return discovered links, status, and optional file path."""
        if state_key is None:
            state_key = domain
        # flag_download = await self.loop.run_in_executor(
        #     self.io_executor,
        #     functools.partial(self.classifier, url, level))        

        urls = []    
        path = ""
        status = "-9"
        retry_after_seconds = None
        try:
            async with session.get(url) as response:
                r = await response.read()
                status = response.status
                if status == 429:
                    retry_after_seconds = self._parse_retry_after_seconds(
                        response.headers.get("Retry-After")
                    )
                    if retry_after_seconds is not None:
                        path = f"Retry-After={retry_after_seconds:.2f}s"
                if status == 200:
                    self.count_downloads += 1
                    # If the chunk size is reached, move the old file outside of temp, create a new file
                    if (self.extractor_unit is not None) and ((self.count_downloads % self.chunk_size) == 0):
                        src = Path(self.file_res)
                        dst = Path(self.file_res_path) / src.name
                        if src.exists() and (src.resolve() != dst.resolve()):
                            shutil.move(str(src), str(dst))

                    # parse the contents and extract URLS
                    urls = await self.loop.run_in_executor(
                        self.cpu_executor, functools.partial(self.get_urls, r, url, domain, level, identifier))
                    
                    if self.save_html:
                        rel = self._build_page_relative_path(
                            domain=domain,
                            url=url,
                            current_date=self.__get_current_date(),
                        )
                        path = str(self.base_archive_path / rel)
                        temp_path = str(self.base_temp_path / rel)
                        # Save raw contents through the IO executor to avoid
                        # blocking the event loop with filesystem writes.
                        await self.loop.run_in_executor(
                            self.io_executor,
                            functools.partial(self.__save_to_disk, temp_path, r),
                        )
                    else:
                        path = ""

                #self.__update_overview_file(domain, level, url, identifier, status, path)
                
                return urls, status, path, retry_after_seconds

        except Exception as e:
            error_text = str(e).lower()
            if "timed out" in error_text or isinstance(e, asyncio.TimeoutError):
                status = "Request timeout"
            elif (
                "nodename nor servname" in error_text
                or "name or service not known" in error_text
            ):
                status = "DNS lookup failed"
            elif "cannot connect to host" in error_text:
                status = "Connection failed"
            else:
                status = "Request failed"
            path = "One wrapped: " + str(e)
            self.errors_website[state_key] += 1
            return None, status, path, retry_after_seconds
            
    async def __test_domain_robots(self, url, domain, identifier):
        """
        Build a normalized base URL and try to fetch robots.txt.
        """
        url = str(url).strip()
        if not url:
            return None

        if url.startswith("http://") or url.startswith("https://"):
            candidates = [url]
            if url.startswith("https://"):
                candidates.append(f"http://{url[len('https://'):]}")
        else:
            base = url.lstrip("/").rstrip("/")
            candidates = [f"https://{base}", f"http://{base}"]

        fallback = None
        last_error = None
        rp_allow_all = Protego.parse("User-agent: *\nDisallow: \n")
        retry_waits = (0, 1.0)

        for candidate in candidates:
            parsed = urlparse(candidate)
            if not parsed.scheme or not parsed.netloc:
                continue
            domain_return = (parsed.hostname or domain).replace("www.", "")
            if fallback is None:
                fallback = (candidate, domain_return)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

            for wait in retry_waits:
                if wait > 0:
                    await asyncio.sleep(wait)
                try:
                    async with self.session.get(robots_url) as response:
                        if response.status >= 400:
                            return candidate, domain_return, rp_allow_all
                        robots_bytes = await response.read()
                        rp = Protego.parse(robots_bytes.decode("utf-8", "ignore"))
                        return candidate, domain_return, rp
                except (CancelledError, Exception) as exc:
                    last_error = exc
                    continue

        if fallback is None:
            status = "Website not found in __test_domain_robots"
            path = "Domain robots URL: invalid URL"
            self.__update_overview_file(domain, 0, url, identifier, status, path)
            return None

        # If robots.txt is temporarily unavailable, proceed with an allow-all policy
        # and keep the original preferred scheme instead of downgrading to HTTP.
        # This avoids false "not found" cascades where robots retrieval fails but
        # page fetching still works.
        if last_error is not None:
            return fallback[0], fallback[1], rp_allow_all

        return fallback[0], fallback[1], rp_allow_all
          
                        
    async def __fetch_one_base_url(self, domain, url, identifier):
        """
        Crawl the base url up max_level. Save html to file.

        :param domain: domain (id in the disk)
        :param url: url string to visit for base url
        """
        
        async with self.sem_num_comps:
            # test if domain is accessible
            domain = await self.__test_domain_robots(url, domain, identifier)
            
            if domain is None:
                # save problem with the request for robots.txt (usually page doesn't exist)
                #self.__update_overview_file(domain, 0, url, identifier, status, path)

                return None
            else:
                url, domain, rp = domain
            state_key = f"{domain}|{identifier}|{clean_url(url)}"
            try:
                self.waits.setdefault(domain, 0.0)
                self.errors_website[state_key] = 0
                
                level = 0

                records = [url]
                seen_records = {clean_url(url)}

                pages_downloaded = 0
                
                # Breath first search algorithm from urls
                while (len(records) > 0) and (level < self.max_level) and (pages_downloaded < self.max_pages_per_domain):
                    records_found = []
                    # fetch urls asynchronously in bounded batches
                    for batch in _iter_chunks(records, self.page_batch_size):
                        tasks = []
                        for url in batch:
                            # check if we can actually download it in robots
                            if rp.can_fetch(url, "*"):
                                task = asyncio.create_task(
                                    self.__fetch_one_url(
                                        domain, url, identifier, level=level, state_key=state_key
                                    )
                                )
                                tasks.append(task)

                                pages_downloaded += 1
                                if pages_downloaded > self.max_pages_per_domain:
                                    break
                        if tasks:
                            records_found.extend(await asyncio.gather(*tasks))
                        if pages_downloaded > self.max_pages_per_domain:
                            break
                    
                    # flatten list python
                    records_found = [
                        item
                        for sublist in records_found
                        if sublist is not None
                        for item in sublist
                    ]

                    # remove urls already downloaded
                    records = []
                    for next_url in records_found:
                        url_cleaned = clean_url(next_url)
                        if url_cleaned in seen_records:
                            continue
                        seen_records.add(url_cleaned)
                        records.append(next_url)

                    level += 1
                
                if self.save_html:
                    # Zip/archive on executor to avoid blocking the event loop.
                    await self.loop.run_in_executor(
                        self.io_executor,
                        functools.partial(self._archive_domain_folder_sync, domain),
                    )

                self.errors_website.pop(state_key, None)

            except Exception as e:
                status = "Base URL crawl failed"
                path = "One base URL: " + str(e)

                # save problem with the request for robots.txt (usually page doesn't exist)
                self.__update_overview_file(domain, 0, url, identifier, status, path)


    async def __fetch_all_base_urls(self, records):
        """
        Fetch all urls in records up to a level max_level. Save html to file.

        :param records: List of all level 0 urls to visit
        """
        self.loop = asyncio.get_running_loop()

        self.num_urls = 0

        # Check if the URLs have been downloaded
        urls_downloaded = self.__get_downloaded_domains()
        filtered_records = []

        # Build list first so we can show total progress and batch safely.
        for record in records:
            identifier = None
            if isinstance(record, str):
                url = str(record).strip()
            elif isinstance(record, (tuple, list)):
                if len(record) == 0:
                    continue
                first = str(record[0]).strip() if record[0] is not None else ""
                second = (
                    str(record[1]).strip()
                    if (len(record) > 1 and record[1] is not None)
                    else ""
                )
                if first.startswith(("http://", "https://")):
                    url = first
                    identifier = second or None
                elif second.startswith(("http://", "https://")):
                    # Backward compatibility for legacy tuples (domain, url).
                    url = second
                    identifier = first or None
                else:
                    url = first
                    identifier = second or None
            else:
                continue

            url = _normalize_base_url(url)
            if not url:
                continue

            domain = _registered_domain(url)
            if not identifier:
                identifier = domain

            # Do not retry the base urls that failed in the last days window
            if clean_url(url) in urls_downloaded:
                continue
            filtered_records.append((domain, url, identifier))

        self.num_urls = len(filtered_records)
        if self.num_urls == 0:
            return []

        self.sem_num_comps = asyncio.Semaphore(min(self.concurrency_base_urls, self.num_urls))
        self.sem_pages = asyncio.Semaphore(self.concurrency_pages)

        # create HTTP client
        async with ClientSession(
            headers=self.headers,
            trust_env=True,
            timeout=ClientTimeout(total=None, sock_connect=self.sock_connect, sock_read=self.sock_connect),
            connector=TCPConnector(
                limit=self.threads_download, #number of websites/request in parallel
                ssl=self.verify_ssl, 
                ttl_dns_cache=600, #maintain dns cache to speed up
                # limit_per_host=1, #only one request per website simultaneously, not a good idea, waits are better
                force_close=False,
            )

        ) as self.session:
            progress = []
            with tqdm.tqdm(total=self.num_urls, miniters=1) as pbar:
                for batch in _iter_chunks(filtered_records, self.base_url_batch_size):
                    tasks = [
                        asyncio.create_task(self.__fetch_one_base_url(domain, url, identifier))
                        for domain, url, identifier in batch
                    ]
                    for task in asyncio.as_completed(tasks):
                        progress.append(await task)
                        pbar.update(1)

            return progress

    def _run_fetch_loop(self, urls):
        """Run crawler async loop in a fresh event loop context."""
        asyncio.run(self.__fetch_all_base_urls(urls))

  
    def crawl_base_urls(self, urls):
        """
        Create initial asynchronous task to fetch all urls

        :param urls: List of all level 0 urls to visit
        """
        start = time()
        self._open_duckdb_overview_writer()
        try:
            with ThreadPoolExecutor(max_workers=self.threads_bs4) as self.cpu_executor, ThreadPoolExecutor(max_workers=4) as self.io_executor:
                try:
                    running_loop = asyncio.get_running_loop()
                except RuntimeError:
                    running_loop = None

                if running_loop and running_loop.is_running():
                    # Jupyter/async contexts already own the current event loop.
                    with ThreadPoolExecutor(max_workers=1) as loop_executor:
                        loop_executor.submit(self._run_fetch_loop, urls).result()
                else:
                    self._run_fetch_loop(urls)
        finally:
            self._close_duckdb_overview_writer()
            self.io_executor = None

        # Move last results file
        if (self.count_downloads > 0) and (self.extractor_unit is not None):
            src = Path(self.file_res)
            dst_dir = Path(self.file_res_path)
            dst = dst_dir / src.name
            if src.exists() and (src.resolve() != dst.resolve()):
                if dst.exists():
                    dst.unlink()
                shutil.move(str(src), str(dst))

        # Per-domain throttling state is only needed during a crawl run.
        self.waits.clear()
        self.domain_semaphores.clear()

        print(
            f"Crawled {self.count_downloads} pages from {self.num_urls} urls to level {self.max_level} in {time() - start:2.1f} seconds."
        )

    def __get_downloaded_domains(self):
        if self.overview_backend in {"sqlite", "duckdb"}:
            min_date = datetime.date.today() - datetime.timedelta(days=self.min_days_between_crawls)
            min_date = min_date.strftime("%Y-%m-%d")

            if self.overview_backend == "duckdb" and self._duckdb_connection is not None:
                self._flush_duckdb_overview_batch(force=True)
                cursor = self._duckdb_connection.cursor()
                cursor.execute(
                    "SELECT url FROM Overview WHERE level = 0 AND session_date > ? AND status = '200';",
                    (min_date,),
                )
                urls = [clean_url(_[0]) for _ in cursor.fetchall()]
            else:
                connection = self._connect_overview_db()
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT url FROM Overview WHERE level = 0 AND session_date > ? AND status = '200';",
                    (min_date,),
                )
                urls = [clean_url(_[0]) for _ in cursor.fetchall()]
                connection.commit()
                connection.close()
        else:
            with open(self.overview_path, "r", newline='') as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)

                urls = []
                # Iterate through rows in .tsv file
                for row in reader:
                    # Extract values from the row
                    level = row[2]
                    if level != "0":
                        continue
                    url = row[3]
                    status = row[4]
                    min_date = datetime.date.today() - datetime.timedelta(days=self.min_days_between_crawls)
                    session_date = datetime.datetime.strptime(row[5], "%Y-%m-%d").date()
                    if (session_date > min_date) and _status_is_success(status):
                        urls.append(clean_url(url))

        return set(urls)

    def crawl_complement_base_urls(self, complement_date):
        """Re-crawl failed level-0 URLs from a specific crawl ``session_date``."""
        complement_date_str = str(complement_date)
        # Track per-url outcome in the selected session so we only retry URLs
        # that have no successful row and at least one retryable failure row.
        by_url = {}

        if self.overview_backend in {"sqlite", "duckdb"}:
            if self.overview_backend == "duckdb" and self._duckdb_connection is not None:
                self._flush_duckdb_overview_batch(force=True)
                cursor = self._duckdb_connection.cursor()
                cursor.execute(
                    "SELECT url, identifier, level, status, session_date FROM Overview WHERE session_date = ?;",
                    (complement_date_str,),
                )
                rows = cursor.fetchall()
            else:
                connection = self._connect_overview_db()
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT url, identifier, level, status, session_date FROM Overview WHERE session_date = ?;",
                    (complement_date_str,),
                )
                rows = cursor.fetchall()
                connection.commit()
                connection.close()

            for row in rows:
                url = str(row[0]).strip()
                identifier = str(row[1]).strip() if row[1] is not None else ""
                level = str(row[2]).strip() if row[2] is not None else ""
                status = row[3]
                session_date = str(row[4]).strip() if row[4] is not None else ""
                if session_date != complement_date_str or level != "0":
                    continue
                if str(status).strip() == "":
                    continue
                entry = by_url.setdefault(
                    clean_url(url),
                    {
                        "url": url,
                        "identifier": identifier or None,
                        "has_success": False,
                        "has_retryable_failure": False,
                    },
                )
                if _status_is_success(status):
                    entry["has_success"] = True
                elif not _status_is_permanent_not_found(status):
                    entry["has_retryable_failure"] = True
        else:
            with open(self.overview_path, "r", newline='') as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)

                # Iterate through rows in .tsv file
                for row in reader:
                    # Extract values from the row
                    level = row[2]
                    url = row[3]
                    status = row[4]
                    session_date = row[5]
                    identifier = row[1]

                    # Re-crawl failed base URLs from this crawl date.
                    if session_date != complement_date_str or level != "0":
                        continue
                    if status.strip() == "":
                        continue
                    normalized = clean_url(url)
                    entry = by_url.setdefault(
                        normalized,
                        {
                            "url": url,
                            "identifier": identifier or None,
                            "has_success": False,
                            "has_retryable_failure": False,
                        },
                    )
                    if _status_is_success(status):
                        entry["has_success"] = True
                    elif not _status_is_permanent_not_found(status):
                        entry["has_retryable_failure"] = True

        urls = [
            (entry["url"], entry["identifier"])
            for entry in by_url.values()
            if (not entry["has_success"]) and entry["has_retryable_failure"]
        ]

        if len(urls) == 0:
            print(f"No failed level-0 URLs found for session_date={complement_date_str}.")
            return

        self.crawl_base_urls(urls)

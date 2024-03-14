"""This module provides the Crawler model-controller."""
import csv
# TODO: Temporary, remove!
import warnings
warnings.filterwarnings("ignore")
from pathlib import Path
from typing import Any, Dict, List, NamedTuple
import asyncio
from asyncio import CancelledError
from aiohttp.client_exceptions import ClientConnectorError
import datetime
import functools
import ndjson
# monkeypatch to avoid "Can not load response cookies" 
import http.cookies
import os
import re
import orjsonl
import shutil
import zipfile
import sqlite3 as sql
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
# import hashlib
from time import time
from urllib.parse import urljoin, urlparse
import tldextract
import tqdm
import tqdm.asyncio
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from protego import Protego

http.cookies._is_legal_key = lambda _: True

try:
    from extractor import Extractor
    from utils import clean_url, set_regex, classify_url
except:
    from ..extractor.extractor import Extractor
    from ..utils.utils import clean_url, set_regex, classify_url

class Crawler:
    def __init__(
        self,
        target_folder_path,
        save_html=True,
        max_level=3,
        classification_file_path=None,
        verify_ssl=False,
        concurrency_base_urls=1000,
        threads_bs4=10,
        threads_download=500,
        use_sqlite=True,
        sock_connect=180,
        extract=False,
        headers=None,
        file_extractor=None,
        max_pages_per_domain=50,
        min_days_between_crawls=30
    ):
        self.target_folder_path = Path(target_folder_path)
        self.use_sqlite = use_sqlite
        self.base_path = self.target_folder_path / "crawled_data"
        if save_html:
            Path(self.base_path).parent.mkdir(parents=True, exist_ok=True)

        if use_sqlite:
            self.overview_path = f"{self.target_folder_path}/overview_urls.db"
        else:
            self.overview_path = f"{self.target_folder_path}/overview_urls.tsv"

        self.save_html = save_html
        self.max_level = max_level
        self.max_pages_per_domain = max_pages_per_domain
        self.min_days_between_crawls = min_days_between_crawls

        # Create file tracking downloaded packages
        self.__create_overview_file()

        # Avoid error in SSL certificates
        self.verify_ssl = verify_ssl
        self.url_regex_mail, self.negative_regex, self.url_regex, self.report_regex = set_regex(classification_file_path = classification_file_path)
        self.classifier = classify_url


        # Base urls processed in parallel
        self.sem_num_comps = asyncio.Semaphore(concurrency_base_urls)
        self.threads_bs4 = threads_bs4
        self.threads_download = threads_download
        self.sock_connect = sock_connect

        self.waits = dict()
        self.errors_website = dict()
        
        if headers is None:
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/110.0',
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
        self.count_downloads = 0


        # extract data at the same time (no saving raw data)
        if not extract:
            self.extractor_unit = None
        else:
            self.extractor_unit = Extractor(
                target_folder_path = Path(target_folder_path),
                file_extractor=file_extractor)
            # save json to file (right now one big file)
            self.file_res = (
                self.target_folder_path
                / "extracted_data"
                / f"extracted_data_{self.crawler_session_date}.ndjson"
            )
            Path(self.file_res).parent.mkdir(parents=True, exist_ok=True)
            


    def get_urls(self, r, url, domain, level, identifier):
        """
        Parse code and return content and urls. Defined it here to be able to pickle it and process it in a thread pool.
        """

        # resp.content is a byte array, convert to string
        contents = r.decode("utf-8", "ignore")

        # parse
        soup = BeautifulSoup(contents, "lxml")
        
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
            if tldextract.extract(url_found).registered_domain
            == domain # compare to domain, allows for redirects
        ]

        # remove query string # bol.com/nl/producten/product/...?p=1
        urls = [urlparse(url_found)._replace(query="", fragment="").geturl() for url_found in urls]

        if self.extractor_unit is not None:
            date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            json_dict = self.extractor_unit._create_results([base_url, identifier, level, url, date, soup])
            
            orjsonl.append(self.file_res, [json_dict])

        return urls

    def __create_overview_file(self):
        """
        This function creates an overview file with the required headers if it doesn't exist.
        If the `use_sqlite` attribute of the object is set to `True`, it creates a SQLite database with the 
        required table and indexes, and if it's set to `False`, it creates a tab-separated file with the headers.
        
        Parameters:
            self (object): The object instance.
        
        Returns:
            None.
        """
        if self.use_sqlite:
            connection = sql.connect(self.overview_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS Overview
                (domain TEXT, identifier TEXT, level INT, url TEXT, status TEXT, session_date TEXT, crawl_date TEXT, path TEXT, 
                UNIQUE (domain, url, session_date, status));
                """
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS index_date ON Overview (crawl_date);")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS index_status ON Overview (status);"
            )
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
            domain = tldextract.extract(url).registered_domain.replace("www.", "")

        if self.use_sqlite:
            # opening the file is fast (0.00x per query), minimum gain to keep it open (and potential trouble with threading)
            connection = sql.connect(self.overview_path)
            cursor = connection.cursor()
            
            cursor.execute("INSERT OR IGNORE INTO Overview VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (domain, identifier, level, url, status, self.crawler_session_date, date, path))

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


    def __get_current_date(self):
        # return current day in format "YYYY-MM-DD"
        return datetime.datetime.now().strftime("%Y-%m-%d")

            
    async def __fetch_one_url(self, domain, url, identifier, level):
        
        flag_download = self.classifier(url, level, self.url_regex_mail, self.negative_regex, self.url_regex, self.report_regex)
        
        # classify url to see if it should be crawled
        if not flag_download:  # self.classifier(url, level):
            # add to file, without path
            #self.__update_overview_file(id, level, url, -9, "")
            return []
        
        # be nice and wait a bit per url (non blocking)
        self.waits[domain] += 0.1
        await asyncio.sleep(self.waits[domain])
 
        
        try_number = 0
        status = "One URL: Too many errors in website"
        path = ""

        while (try_number < 3) and (self.errors_website[domain] < 20):
            if try_number == 0:
                urls, status, path = await self.__fetch_one_url_wrapped(domain, url, identifier, level, self.session)
            else:
                await asyncio.sleep(20)

                #If failure, give an individual session
                async with ClientSession(headers=self.headers, 
                                         trust_env=True, 
                                         connector=
                                         TCPConnector(limit=1, #number of websites/request in parallel
                                            ssl=self.verify_ssl, 
                                            ttl_dns_cache=0,
                                            force_close=True), #a bit slower but more reliable
                                            timeout=ClientTimeout(total=None, sock_connect=300, sock_read=300)) as session:
                    urls, status, path = await self.__fetch_one_url_wrapped(domain, url, identifier, level, session)

            try_number += 1
            
            # No error
            if urls is not None:
                self.__update_overview_file(domain, level, url, identifier, status, path)
                return urls
        
        # Error (also by domain)
        self.__update_overview_file(domain, level, url, identifier, status, path)
        return []

         
    async def __fetch_one_url_wrapped(self, domain, url, identifier, level, session):
        """
        Fetch one url, save the html, and return the list of urls found on the page.
        """
        # flag_download = await self.loop.run_in_executor(
        #     self.io_executor,
        #     functools.partial(self.classifier, url, level))        

        urls = []    
        path = ""
        status = "-9"
        try:
            async with session.get(url) as response:
                await asyncio.sleep(0.001)
                r = await response.read()
                status = response.status
                if status == 200:
                    self.count_downloads += 1

                    # parse the contents and extract URLS
                    urls = await self.loop.run_in_executor(
                        self.cpu_executor, functools.partial(self.get_urls, r, url, domain, level, identifier))
                    

                    if self.save_html:
                        # create path www.google.com/something/ --> something
                        if url[-1] == "/":
                            path = f"{self.base_path}/{domain}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{url[:-1].split('/')[-1]}"
                        else:  # Create path www.google.com/something --> something
                            path = f"{self.base_path}/{domain}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{url.split('/')[-1]}"
                        path = path.replace(" ", "_")

                        # save raw contents to file
                        self.__save_to_disk(path, r)
                    else:
                        path = ""

                #self.__update_overview_file(domain, level, url, identifier, status, path)
                
                return urls, status, path

        except Exception as e:
            status = "Website not found"
            path = "One wrapped: " + str(e)
            #self.__update_overview_file(domain, level, url, identifier, status, path)
            self.errors_website[domain] += 1
            return None, status, path
            
    async def __test_domain_robots(self, url, domain, identifier):
        """
        Test if domain robots is accessible. If not, return None.
        """
        if not url.startswith('http://') and not url.startswith('https://'):
            url_test = f'https://{url}'
        else:
            url_test = f'{url}'

        domain_return = None
        
        async with ClientSession(headers=self.headers, 
                                 trust_env=True, 
                                 connector=
                                 TCPConnector(limit=1, #number of websites/request in parallel
                                    ssl=self.verify_ssl, 
                                    ttl_dns_cache=0,
                                    force_close=True
                                    ), #a bit slower but more reliable
                                    timeout=ClientTimeout(total=None, sock_connect=300, sock_read=300)) as session:
            # Try HTTPS
            try:
                async with session.get(url_test) as response:
                    await asyncio.sleep(0.001)
                    r = await response.read()
                    status = response.status
                    if status == 200:
                        domain_return = response.url.host.replace("www.", "")
            except (CancelledError, Exception) as e:
                # Try HTTP
                try:
                    if (not url.startswith('http://')) and (not url.startswith('https://')):
                        url_test = f'http://{url}/'
                        async with session.get(url_test) as response:
                            await asyncio.sleep(0.001)
                            r = await response.read()
                            status = response.status
                            if status == 200:
                                domain_return = response.url.host.replace("www.", "")
                    else:
                        return None
                        
                # Quit domain
                except (CancelledError, Exception) as e:
                    status = "Website not found in __test_domain_robots"
                    path = "Domain robots URL: " + str(e)

                    # save problem with the request for robots.txt (usually page doesn't exist)
                    self.__update_overview_file(domain, 0, url, identifier, status, path)

                    return None

            # Try getting robots.txt
            try:
                async with session.get(f"{url_test}/robots.txt") as response:
                    #try reading robots
                    await asyncio.sleep(0.001)
                    r = await response.read()
                    rp = Protego.parse(r.decode("utf-8", "ignore")) 
            except Exception as e:
                # allow everything
                rp = Protego.parse("User-agent: *\nDisallow: \n")
            return url_test, domain_return, rp
          
                        
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
                return None
            else:
                url, domain, rp = domain
            try:
                self.waits[domain] = 0
                self.errors_website[domain] = 0
                
                level = 0

                all_records = [url]
                records = [url]
                
                # get list of dates when url was crawled
                sourcepath = f'{self.base_path}/{domain}/{urlparse(url).netloc.replace("www.","")}'
                if Path(sourcepath).exists():
                    crawl_dates = [
                        datetime.datetime.strptime(
                            str(path).rsplit("/", 1)[1], "%Y-%m-%d"
                        ).date()
                        for path in Path(sourcepath).iterdir()
                        if path.is_dir()
                    ]
                    # check if most recent crawldate is within threshold and if so, stop crawling for this base url
                    if (datetime.date.today() - max(crawl_dates)).days < self.min_days_between_crawls:
                        return

                # Breath first search algorithm from urls
                while (len(records) > 0) and (level < self.max_level):
                    tasks = []
                    # fetch urls asynchroneously
                    for url in records:
                        # check if we can actually download it in the robots
                        if rp.can_fetch(url, "*"):
                            task = asyncio.create_task(
                                self.__fetch_one_url(domain, url, identifier, level=level)
                            )
                            tasks.append(task)

                    records = await asyncio.gather(*tasks)
                    #records)
                    # flatten list python and remove duplicates
                    records = [
                        item
                        for sublist in records
                        if sublist is not None
                        for item in sublist
                    ]
                    
                    
                    # speed up search using a set (and remove www to avoid downloading twice the same url)
                    temp_all_records = set(
                        [clean_url(url) for url in all_records]
                    )

                    # make sure the scraper doesn't run forever 
                    if len(temp_all_records) > self.max_pages_per_domain:
                        break

                    # remove urls already downloaded
                    records = list(
                        set(
                            [
                                url
                                for url in records
                                if clean_url(url) not in temp_all_records
                            ]
                        )
                    )
                    
                    # add new urls to list
                    all_records += records
                    level += 1

                    # reset waits for next level
                    self.waits[domain] = 0
                
                if self.extractor_unit is None:
                    # zip the folder
                    base_url_folder = self.base_path  / f'{domain}'
                    zip_url_folder = self.base_path /  f'{domain}.zip'

                    zf = zipfile.ZipFile(zip_url_folder, "w", zipfile.ZIP_LZMA, allowZip64=True)
                    for dirname, subdirs, files in os.walk(base_url_folder):
                        for filename in files:
                            file_path = os.path.join(dirname, filename)
                            arcname = os.path.relpath(file_path, base_url_folder)
                            zf.write(file_path, arcname)
                    zf.close()
                    shutil.rmtree(base_url_folder)

                    self.waits.pop(domain)
                    self.errors_website.pop(domain)

            except Exception as e:
                status = "Website not found"
                path = "One base URL: " + str(e)

                # save problem with the request for robots.txt (usually page doesn't exist)
                self.__update_overview_file(domain, 0, url, identifier, status, path)


    async def __fetch_all_base_urls(self, records):
        """
        Fetch all urls in records up to a level max_level. Save html to file.

        :param records: List of all level 0 urls to visit
        """

        tasks = []

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
                force_close=True,  #slower but more reliable
            )

        ) as self.session:
            # for each url, create asynchronous task to fetch base url and append to tasks list
            for url in records:
                if not isinstance(url, str): #if tuple/list of form: (url, ID)
                    identifier = url[1]
                    url = url[0].strip()
                    domain = tldextract.extract(url).registered_domain.replace("www.", "")
                else: #if only url use the domain as ID
                    domain = tldextract.extract(url).registered_domain.replace("www.", "")
                    identifier = domain
                task = asyncio.create_task(self.__fetch_one_base_url(domain, url, identifier))
                tasks.append(task)

            # create future and group tasks
            progress = [
                await f
                for f in tqdm.tqdm(
                    asyncio.as_completed(tasks),
                    total=len(tasks),
                    miniters=1,
                )
            ]

            return progress

  
    def crawl_base_urls(self, urls):
        """
        Create initial asynchronous task to fetch all urls

        :param urls: List of all level 0 urls to visit
        """

        start = time()
        
        with ThreadPoolExecutor(max_workers=self.threads_bs4) as self.cpu_executor, \
             ThreadPoolExecutor(max_workers=1) as self.io_executor:
            self.loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.__fetch_all_base_urls(urls))
            self.loop.run_until_complete(future)

        print(
            f"Crawled {self.count_downloads} pages from {len(urls)} urls to level {3} in {time() - start:2.1f} seconds."
        )


    def crawl_complement_base_urls(self, complement_date):
        if self.use_sqlite:
            connection = sql.connect(self.overview_path)
            cursor = connection.cursor()

            cursor.execute("SELECT domain, url FROM Overview WHERE session_date = ? AND status != 200 AND status != -9 AND status != '' AND status != 'Website not found';", (complement_date,))
            urls = cursor.fetchall()
            
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
                    domain = row[0]
                    url = row[2]
                    status = row[3]
                    session_date = row[4]

                    # Check if session_date matches the complement_date and status is not 200, '', or 'Website not found'
                    if session_date == str(complement_date) and status not in ["200", "", "-9", "Website not found"]:
                        urls.append((domain, url))

                
        self.crawl_base_urls(urls)



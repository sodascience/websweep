"""This module provides the Scraper model-controller."""

from pathlib import Path
from cpscraper.utils.utils import Worker
from re import S
from typing import Any, Dict, List, NamedTuple

# TODO: Temporary, remove!
import warnings

warnings.filterwarnings("ignore")

import asyncio
import tqdm
import tqdm.asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.parse import urljoin, urlparse
from protego import Protego
import functools

from aiohttp import ClientSession, TCPConnector, ClientTimeout
from bs4 import BeautifulSoup
from pathlib import Path

# import hashlib
from time import time
import datetime
import tldextract
import os
import sqlite3 as sql

# monkeypatch to avoid "Can not load response cookies" 
import http.cookies
http.cookies._is_legal_key = lambda _: True


class Scraper(Worker):
    def __init__(
        self,
        target_folder_path,
        save_html=True,
        max_level=3,
        classifier=lambda url, level: True,
        verify_ssl=False,
        concurrency_companies=1000,
        threads_bs4=10,
        threads_download=1000,
        use_sqlite=False,
    ):
        self.target_folder_path = target_folder_path
        self.base_path = self.target_folder_path / "data"
        self.use_sqlite = use_sqlite
        Path(self.base_path).parent.mkdir(parents=True, exist_ok=True)

        if use_sqlite:
            self.overview_path = f"{self.target_folder_path}/overview_urls.db"
        else:
            self.overview_path = f"{self.target_folder_path}/overview_urls.tsv"

        self.save_html = save_html
        self.max_level = max_level

        # Create file tracking downloaded packages
        self.__create_overview_file()

        # Avoid error in SSL certificates
        self.verify_ssl = verify_ssl
        self.classifier = classifier

        # Companies processed in parallel
        self.sem_num_comps = asyncio.Semaphore(concurrency_companies)
        self.threads_bs4 = threads_bs4
        self.threads_download = threads_download

        self.waits = dict()
        self.errors_website = dict()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:98.0) Gecko/20100101 Firefox/98.0",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": "cookielawinfo-checkbox-necessary=yes; cookielawinfo-checkbox-functional=no; cookielawinfo-checkbox-performance=no; cookielawinfo-checkbox-analytics=no; cookielawinfo-checkbox-advertisement=no; cookielawinfo-checkbox-others=no; CookieLawInfoConsent=eyJuZWNlc3NhcnkiOnRydWUsImZ1bmN0aW9uYWwiOmZhbHNlLCJwZXJmb3JtYW5jZSI6ZmFsc2UsImFuYWx5dGljcyI6ZmFsc2UsImFkdmVydGlzZW1lbnQiOmZhbHNlLCJvdGhlcnMiOmZhbHNlfQ==; viewed_cookie_policy=yes; optiMonkClientId=f299334f-0413-e0e3-489b-d0ae48a7beb5",
            "Upgrade-Insecure-Requests": "1",
        }


        # self.start = time()
        self.count_downloads = 0

        # print("Execution type:", exc_type)
        # print("Execution value:", exc_value)
        # print("Traceback:", traceback)

    def get_urls(r, url):
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
        # TODO: do we want to keep mailto? mailto:info@frieslandbouwdetachering.nl?
        urls = [urljoin(base_url, url_found) for url_found in urls]

        # keep only the urls found within the same domain
        urls = [
            url_found
            for url_found in urls
            if tldextract.extract(url_found).registered_domain
            == tldextract.extract(url).registered_domain
        ]

        # remove query string # bol.com/nl/producten/product/...?p=1
        urls = [urlparse(url_found)._replace(query="").geturl() for url_found in urls]

        return urls

    def test_package():
        return "Hello, this is a return from the main.py file in the cpscraper package"

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
                """CREATE TABLE IF NOT EXISTS Overview
                (id TEXT, domain TEXT, level INT, url TEXT, status TEXT, date TEXT, path TEXT);"""
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS index_date ON Overview (date);")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS index_status ON Overview (status);"
            )
            connection.commit()
            connection.close()

        else:
            # Check if overview file exists, if not create it
            if not Path(self.overview_path).is_file():
                with open(self.overview_path, "w") as f:
                    f.write("id\tdomain\tlevel\turl\tstatus\tdate\tpath\n")

    def __update_overview_file(self, id, level, url, status, path):

        date = self.__get_current_date()
        if ":" in url[:6]:  # tel: or mailto:
            domain = url
        else:
            domain = urlparse(url).netloc.replace("www.", "")

        if self.use_sqlite:
            # opening the file is fast (0.00x per query), minimum gain to keep it open (and potential trouble with threading)
            connection = sql.connect(self.overview_path)
            cursor = connection.cursor()

            cursor.execute(
                f"INSERT INTO Overview VALUES ('{id}', '{domain}', {level}, '{url}', '{status}', '{date}', '{path}')"
            )
            connection.commit()
            connection.close()

        else:
            with open(self.overview_path, "a+") as f:
                f.write(f"{id}\t{domain}\t{level}\t{url}\t{status}\t{date}\t{path}\n")

    def __save_to_disk(self, path, contents):
        """
        Save all data to disk.
        """
        self.count_downloads += 1

        # create folder if it doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # write raw contents to file
        with open(path, "wb") as f:
            f.write(contents)

    def __get_current_date(self):
        # return current day in format "YYYY-MM-DD"
        return datetime.datetime.now().strftime("%Y-%m-%d")

    
            
    async def __fetch_one_url(self, url, kvk, level):
        # be nice and wait a bit per url (non blocking)
        self.waits[kvk] += 0.1
        await asyncio.sleep(self.waits[kvk])

        try_number = 0
        while (try_number < 3) and (self.errors_website[kvk] < 20):
            if try_number == 0:
                urls = await self.__fetch_one_url_wrapped(url, kvk, level, self.session)
            else:
                await asyncio.sleep(20)

                #If failure, give an individual session
                async with ClientSession(headers=self.headers, 
                                         trust_env=True, 
                                         connector=TCPConnector(limit=1, #number of websites/request in parallel
                                         ssl=self.verify_ssl, 
                                         ttl_dns_cache=0,
                                         force_close=True), #a bit slower but more reliable
                        timeout=ClientTimeout(total=None, sock_connect=300, sock_read=300)) as session:
                    urls = await self.__fetch_one_url_wrapped(url, kvk, level, session)

            try_number += 1
            
            if urls is not None:
                return urls
        
        return []

         

    async def __fetch_one_url_wrapped(self, url, kvk, level, session):
        """
        Fetch one url, save the html, and return the list of urls found on the page.
        """
        # flag_download = await self.loop.run_in_executor(
        #     self.io_executor,
        #     functools.partial(self.classifier, url, level))


        flag_download = self.classifier(url, level)
        # print(url, flag_download)
        # classify url to see if it should be crawled
        if not flag_download:  # self.classifier(url, level):
            # add to file, without path
            #self.__update_overview_file(kvk, level, url, -9, "")
            return []

        

        urls = []
        # create path www.google.com/something/ --> something
        if url[-1] == "/":
            path = f"{self.base_path}/{kvk}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{url[:-1].split('/')[-1]}"
        else:  # Create path www.google.com/something --> something
            path = f"{self.base_path}/{kvk}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{url.split('/')[-1]}"
        path = path.replace(" ", "_")
    
  

        try:
            async with session.get(url) as response:
                await asyncio.sleep(0.001)
                r = await response.read()
                status = response.status
                if status == 200:
                    # parse the contents and extract URLS
                    urls = await self.loop.run_in_executor(
                        self.cpu_executor, functools.partial(self.get_urls,r, url))

                    if self.save_html:
                        # save raw contents to file
                        self.__save_to_disk(path, r)


                else:
                    path = ""
                
                self.__update_overview_file(kvk, level, url, status, path)
                
                return urls

        except Exception as e:
            status = str(type(e)) + str(e)
            path = ""
            self.__update_overview_file(kvk, level, url, status, path)
            self.errors_website[kvk] += 1
            return None
            

    async def __fetch_one_company(self, url):
        """
        Crawl the website of a company up max_level. Save html to file.

        :param url: Kvk number and url string to visit for company
        """

        async with self.sem_num_comps:
            try:
                # print(f"{url} started at {time()-self.start:2.1f} seconds")
                # start = time()

                # name and url
                kvk, url = url

                self.waits[kvk] = 0
                self.errors_website[kvk] = 0


                level = 0
                all_records = [url]
                records = [url]

                # get list of dates when url was crawled
                # TODO: threshold variable
                # TODO: enhance removing www and http leaders
                # TODO: make this a called method

                sourcepath = "data/scraped_data/{}/{}".format(
                    kvk,
                    url.replace("www.", "")
                    .replace("http://", "")
                    .replace("https://", ""),
                )
                if Path(sourcepath).exists():
                    crawl_dates = [
                        datetime.datetime.strptime(
                            str(path).rsplit("/", 1)[1], "%Y-%m-%d"
                        ).date()
                        for path in Path(sourcepath).iterdir()
                        if path.is_dir()
                    ]
                    # check if most recent crawldate is within threshold and if so, log finding and stop crawling for this company
                    if (datetime.date.today() - max(crawl_dates)).days < 30:
                        return

                # Read the robots
                async with self.session.get(f"{url}/robots.txt") as response:
                    await asyncio.sleep(0.001)
                    r = await response.read()
                    rp = Protego.parse(r.decode("utf-8", "ignore"))

                # Breath first search algorithm from urls
                while (len(records) > 0) and (level < self.max_level):
                    tasks = []

                    # fetch urls asynchroneously
                    for url in records:
                        # check if we can actually download it in the robots
                        if rp.can_fetch(url, "*"):

                            task = asyncio.create_task(
                                self.__fetch_one_url(url, kvk=kvk, level=level)
                            )
                            tasks.append(task)

                    records = await asyncio.gather(*tasks)

                    # flatten list python and remove duplicates
                    records = [
                        item
                        for sublist in records
                        if sublist is not None
                        for item in sublist
                    ]

                    # speed up search using a set (and remove www to avoid downloading twice the same url)
                    temp_all_records = set(
                        [url.replace("www.", "") for url in all_records]
                    )

                    # make sure the scraper doesn't run forever (TODO: make sure it's 100 downloaded, not 100 found)
                    if len(temp_all_records) > 100:
                        break

                    # remove urls already downloaded
                    records = list(
                        set(
                            [
                                url
                                for url in records
                                if url.replace("www.", "") not in temp_all_records
                            ]
                        )
                    )

                    # add new urls to list
                    all_records += records
                    level += 1

                    # reset waits for next level
                    self.waits[kvk] = 0

            except Exception as e:
                status = str(type(e)) + str(e)                    
                path = ""

                # save problem with the request for robots.txt (usually page doesn't exist)
                self.__update_overview_file(kvk, 0, f"{url}/robots.txt", status, path)

            self.waits.pop(kvk)
            self.errors_website.pop(kvk)


    async def __fetch_all(self, records):
        """
        Fetch all urls in records up to a level max_level. Save html to file.

        :param records: List of all level 0 urls to visit
        """

        tasks = []

        # create HTTP client
        async with ClientSession(
            headers=self.headers,
            trust_env=True,
            connector=TCPConnector(limit=self.threads_download, #number of websites/request in parallel
                                   ssl=self.verify_ssl, 
                                   ttl_dns_cache=600, #maintain dns cache to speed up
                                   # limit_per_host=1, #only one request per website simultaneously, not a good idea, waits are better
                                   force_close=True), #slower but more reliable
        ) as self.session:
            # for each url, create asynchronous task to fetch company and append to tasks list
            for url in records:
                task = asyncio.create_task(self.__fetch_one_company(url))
                tasks.append(task)
            # create future and group tasks


            # create future and group tasks
            progress = [
                await f
                for f in tqdm.tqdm(
                    asyncio.as_completed(tasks),
                    total=len(tasks),
                    leave=True,
                    miniters=1,
                )
            ]

            return progress

  
    def scrape_companies(self, urls):
        """
        Create initial asynchronous task to fetch all urls

        :param urls: List of all level 0 urls to visit
        """

        start = time()

        # print(f'Scraper received {len(urls)} urls')


        with ThreadPoolExecutor(
            max_workers=self.threads_bs4
        ) as self.cpu_executor, ThreadPoolExecutor(max_workers=1) as self.io_executor:
            self.loop = asyncio.get_event_loop()
            future = asyncio.create_task(self.__fetch_all(urls))
            self.loop.run_until_complete(future)


        # Read what we did

        print(
            f"Downloaded {self.count_downloads} pages from {len(urls)} urls to level {3} in {time() - start:2.1f} seconds."
        )

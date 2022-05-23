"""This module provides the Scraper model-controller."""

from pathlib import Path
from typing import Any, Dict, List, NamedTuple
from scraper import DB_READ_ERROR, ID_ERROR, config

import asyncio
from urllib.parse import urljoin, urlparse

from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup
from pathlib import Path

import hashlib
from time import time
import datetime
import tldextract
import logging

import os
import re

logging.basicConfig(filename="logs/scraper.log", level=logging.INFO)
logger = logging.getLogger()

class Cacher:
    def __init__(self, target_folder_path, save_html=True, max_level=3, verify_ssl=False, concurrency=20, classifier=lambda url, level: True):
        self.target_folder_path = target_folder_path
        self.base_path = self.target_folder_path / "data"
        self.save_html = save_html
        self.max_level = max_level

        # Error in SSL certificates
        self.verify_ssl = verify_ssl
        self.classifier = classifier

        # Companies processed in parallel
        self.sem_num_comps = asyncio.Semaphore(concurrency)

        self.waits = dict()
        self.headers =   {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:98.0) Gecko/20100101 Firefox/98.0",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": "cookielawinfo-checkbox-necessary=yes; cookielawinfo-checkbox-functional=no; cookielawinfo-checkbox-performance=no; cookielawinfo-checkbox-analytics=no; cookielawinfo-checkbox-advertisement=no; cookielawinfo-checkbox-others=no; CookieLawInfoConsent=eyJuZWNlc3NhcnkiOnRydWUsImZ1bmN0aW9uYWwiOmZhbHNlLCJwZXJmb3JtYW5jZSI6ZmFsc2UsImFuYWx5dGljcyI6ZmFsc2UsImFkdmVydGlzZW1lbnQiOmZhbHNlLCJvdGhlcnMiOmZhbHNlfQ==; viewed_cookie_policy=yes; optiMonkClientId=f299334f-0413-e0e3-489b-d0ae48a7beb5",
            "Upgrade-Insecure-Requests": "1"}

        self.start = time()


    def __get_current_date(self):
        # return current day in format "YYYY-MM-DD"
        return datetime.datetime.now().strftime("%Y-%m-%d")
            

    async def __fetch_one_url(self, url, kvk, level):
        """
        Fetch one url, save the html, and return the list of urls found on the page.
        """

        # classify url to see if it should be crawled
        if not self.classifier(url, level):
            # add to file, without path
            with open("{}/overview_urls.tsv".format(self.target_folder_path), "a+") as f:
                f.write(f"{kvk}\t{urlparse(url).netloc.replace('www.','')}\t{level}\t{url}\t{-9}\t{self.__get_current_date()}\t\n")
            return []

        # be nice and wait a second per url (non blocking)
        self.waits[kvk] += 1
        await asyncio.sleep(self.waits[kvk])

        urls = []
        # hash url to give an ID (collisions are possible)
        hash_url = hashlib.sha1(url.encode()).hexdigest()

        #Create path
        if url[-1] == "/":
            path = f"{self.base_path}/{kvk}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{hash_url}_{url[:-1].split('/')[-1]}"
        else:
            path = f"{self.base_path}/{kvk}/{urlparse(url).netloc.replace('www.','')}/{self.__get_current_date()}/{hash_url}_{url.split('/')[-1]}"
        path = path.replace(" ","_")

        try:
            async with self.session.get(url) as response:
                r = await response.read()
                status = response.status
                if status == 200:
                    # resp.content is a byte array, convert to string
                    contents = r.decode("utf-8", "ignore")

                    if self.save_html:
                        # create folder if it doesn't exist
                        Path(path).parent.mkdir(parents=True, exist_ok=True)

                        # write raw contents to file
                        with open(path, "w") as f:
                            f.write(contents)
                    
                    # parse
                    soup = BeautifulSoup(contents, 'html.parser')

                    # extract urls from html code in beautiful soup
                    urls = [a.attrs.get('href') for a in soup.select('a[href]')]
                    #print(urls)
                    
                    # filter out urls from other domains
                    # create base url
                    url_parsed = urlparse(url)
                    base_url = url_parsed.scheme + "://" + url_parsed.netloc

                    # add netloc/schema when missing
                    # TODO: do we want to keep mailto? mailto:info@frieslandbouwdetachering.nl?
                    urls = [urljoin(base_url, url_found) for url_found in urls]

                    # keep only the urls found within the same domain
                    urls = [url_found for url_found in urls if tldextract.extract(url_found).registered_domain == tldextract.extract(url).registered_domain]

                    # remove query string
                    urls = [urlparse(url_found)._replace(query="").geturl() for url_found in urls]


                    if len(urls) == 0:
                        logging.debug(f'scraper finished for {url}')
                    else:
                        logging.debug(f'scraper {len(urls)} urls found for {url}')
                        

                else:
                    logging.error(f'scraper failed to scrape "{url}"\nResponse status: {status}')
                    path = ""

        except Exception as e:
            logging.error(f'scraper failed to scrape "{url}"\nException: {str(e)}')
            status = str(e)
            path = ""

        #print(f"{kvk}\t{urlparse(url).netloc}\t{level}\t{url}\t{status}\t{self.__get_current_date()}\t{path}")
        # save records to file
        with open("{}/overview_urls.tsv".format(self.target_folder_path), "a+") as f:
            f.write(f"{kvk}\t{urlparse(url).netloc.replace('www.','')}\t{level}\t{url}\t{status}\t{self.__get_current_date()}\t{path}\n")

        return urls


    async def __fetch_one_company(self, url):
        """
        Crawl the website of a company up max_level. Save html to file.
        """
        async with self.sem_num_comps:
            print(f"{url} started at {time()-self.start:2.1f} seconds")
            start = time()

            # name and url
            kvk, url = url

            self.waits[kvk] = 0

            level = 0
            all_records = [url]
            records = [url]
            

            # Breath first search algorithm from urls
            while (len(records) > 0) and (level < self.max_level):
                tasks = []

                # fetch urls asynchroneously
                for url in records:
                    task = asyncio.ensure_future(self.__fetch_one_url(url, kvk=kvk, level=level))
                    tasks.append(task) 

                records = await asyncio.gather(*tasks) 

                # flatten list python and remove duplicates
                records = [item for sublist in records  if sublist is not None for item in sublist]

                # speed up search using a set (and remove www to avoid downloading twice the same url)
                temp_all_records = set([url.replace("www.","") for url in all_records])

                # make sure the scraper doesn't run forever
                if len(temp_all_records) > 100:
                    logging.warn(f'scraper downloaded over 100 subpages of "{url}"')
                    break

                # remove urls alrady downloaded 
                records = list(set([url for url in records if url.replace("www.","") not in temp_all_records]))

                # add new urls to list
                all_records += records
                level += 1

                # reset waits for next level
                self.waits[kvk] = 0


            logging.info(f'scraper for {all_records[0]} finished in {time()-start:2.0f} seconds with {len(temp_all_records)} processed and {len(all_records)} links found.')
            #return all_records


    async def __fetch_all(self, records):
        """
        Fetch all urls in records up to a level max_level. Save html to file.
        
        """
        tasks = []
        async with ClientSession(headers=self.headers, trust_env=True, connector=TCPConnector(limit=100, ssl=self.verify_ssl)) as self.session:
            for url in records:
                task = asyncio.ensure_future(self.__fetch_one_company(url))
                tasks.append(task) 
            await asyncio.gather(*tasks) 
        #return _
    

    def scrape_companies(self, urls):
        logging.info(f'scraper received {len(urls)} urls')
        loop = asyncio.get_event_loop() 

        future = asyncio.ensure_future(self.__fetch_all(urls)) 
        loop.run_until_complete(future) 

        # # flatten list python
        # r = [item for sublist in r  if sublist is not None for item in sublist]

        # # save to file
        # with open("data/test_classifier.tsv", "w+") as f:
        #     for url in set(r):
        #         f.write(f"{url}\n")

        #return r



class Scraper:
    def __init__(self, working_dir=None, website=None, phone=None, email=None, kvk=None,
                 adres=None, zip_code=None, fax=None, btw=None):
        self.website = website
        self.email = email
        self.phone = phone
        self.kvk = kvk
        self.fax = fax
        self.btw = btw
        self.zip_code = zip_code
        self.adres = adres
        self.working_dir = working_dir


    def run_loops(self):
        for file in os.scandir(self.working_dir):
            open_file = open(file, "r", encoding="UTF-8")
            file_text = open_file.read()
            self.scrape_address(file_text)
            self.scrape_zip(file_text)
            self.scrape_phone(file_text)
            self.scrape_email(file_text)
            self.scrape_fax(file_text)
            self.scrape_kvk(file_text)
            self.scrape_btw(file_text)
            open_file.close()

        self.clean_email()
        self.jsonize()
        print(f"finished off {self.website}")

    def scrape_address(self, file):
        if self.adres is None:
            self.adres = set()
        return

    def scrape_zip(self, file):
        if self.zip_code is None:
            self.zip_code = set()
        pattern = re.compile(r"""
                                (\b\d{4}\s?[a-zA-Z]{2}\b)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.zip_code.add(item)
        return

    def scrape_btw(self, file):
        if self.btw is None:
            self.btw = set()
        pattern = re.compile(r"""
                                (btw|BTW)(.+)(\bNL[0-9-_.]{9,12}B\d{2}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.btw.add(item[2])
        return

    def scrape_kvk(self, file):
        if self.kvk is None:
            self.kvk = set()
        pattern = re.compile(r"""
                                (kvk|KvK|K.v.K.|k.v.k.)(.+)(\b\d{8})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.kvk.add(item[2])
        return

    def scrape_phone(self, file):
        if self.phone is None:
            self.phone = set()
        pattern = re.compile(r"""
                                (Tel:\s*|
                                tel:\s*|
                                telefoon:\s*|
                                Telefoon:\s*|
                                T:\s*|
                                t:\s)
                                (\+?(\s?\d-*){10,11})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.phone.add(item[1])
        return

    def scrape_fax(self, file):
        if self.fax is None:
            self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                (\b(\d-*){10,})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.fax.add(item[1])
        return

    def scrape_email(self, file):
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]+       # again, grab any number or letter
                        \.                  # the dot in the email
                        [a-zA-Z-.]+      # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""", re.VERBOSE)
        emails = set(re.findall(pattern, file))
        if self.email is None:
            self.email = emails
        else:
            self.email.update(emails)

    def clean_email(self):
        """
        The used RegEx occasionally grabs the '.' at the end of an email, (while it's signing the end of a sentence),
        this function removes that dot (as well as providing an easy-to-extend way to further add some email-cleaning)
        """
        temp_emails = set()
        for email in self.email:
            if email[-1] == ".":
                temp_emails.add(email[:-1])
            else:
                temp_emails.add(email)
        self.email = temp_emails

    def jsonize(self):
        self.email = list(self.email)
        self.phone = list(self.phone)
        self.kvk = list(self.kvk)
        self.fax = list(self.fax)
        self.btw = list(self.btw)
        self.zip_code = list(self.zip_code)
        self.adres = list(self.adres)

    def mistake_warning(self):
        for key, value in self.__dict__.items():
            if not value:
                print(f"{bcolors.FAIL} No value could be found for {key} at {self.working_dir}{bcolors.RESET}")

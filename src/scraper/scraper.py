import asyncio
from urllib.parse import urljoin, urlparse

from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup
from pathlib import Path

import hashlib
import logging
from time import time
import datetime
import tldextract


# logging
logging.basicConfig(filename="logs/scraper.log", level=logging.INFO)
logger = logging.getLogger()

# if os.environ.get('scraper_logging_level', 'error') == "info":
#     logger.setLevel(logging.INFO)
# else:
#     logger.setLevel(logging.ERROR)


class Scraper:
    def __init__(self, save_html=True, max_level=3, base_path="data/scraped_data", classifier=lambda url,level: True, verify_ssl=False, concurrency=20):
        self.save_html = save_html
        self.base_path = base_path
        self.max_level = max_level

        self.start = time()

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
            with open("data/overview_urls.tsv", "a+") as f:
                f.write(f"{kvk}\t{urlparse(url).netloc.replace('www.','')}\t{level}\t{url}\t{-9}\t{self.__get_current_date()}\t\n")
            return []

        # be nice and wait a second per url (non-blocking)
        self.waits[kvk] += 1
        await asyncio.sleep(self.waits[kvk])

        urls = []
        # hash url to give an ID (collisions are possible)
        hash_url = hashlib.sha1(url.encode()).hexdigest()

        #Create path www.google.com/something --> data/[getal]/google.com/date/[getal]_something
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
                        logger.debug(f'scraper finished for {url}')
                    else:
                        logger.debug(f'scraper {len(urls)} urls found for {url}')
                        

                else:
                    logger.error(f'scraper failed to scrape "{url}"\nResponse status: {status}')
                    path = ""

        except Exception as e:
            logger.error(f'scraper failed to scrape "{url}"\nException: {str(e)}')
            status = str(e)
            path = ""

        #print(f"{kvk}\t{urlparse(url).netloc}\t{level}\t{url}\t{status}\t{self.__get_current_date()}\t{path}")
        # save records to file
        with open("data/overview_urls.tsv", "a+") as f:
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

                # fetch urls asynchronously
                for url in records:
                    task = asyncio.ensure_future(self.__fetch_one_url(url, kvk=kvk, level=level))
                    tasks.append(task) 

                records = await asyncio.gather(*tasks) 

                # flatten list python and remove duplicates
                records = [item for sublist in records if sublist is not None for item in sublist]

                # speed up search using a set (and remove www to avoid downloading twice the same url)
                temp_all_records = set([url.replace("www.","") for url in all_records])

                # make sure the scraper doesn't run forever
                if len(temp_all_records) > 100:
                    logger.warn(f'scraper downloaded over 100 subpages of "{url}"')
                    break

                # remove urls alrady downloaded 
                records = list(set([url for url in records if url.replace("www.","") not in temp_all_records]))

                # add new urls to list
                all_records += records
                level += 1

                # reset waits for next level
                self.waits[kvk] = 0


            logger.info(f'scraper for {all_records[0]} finished in {time()-start:2.0f} seconds with {len(temp_all_records)} processed and {len(all_records)} links found.')
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
        logger.info(f'scraper received {len(urls)} urls')
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


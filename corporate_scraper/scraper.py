import asyncio
from urllib.parse import urljoin, urlparse


from aiohttp import ClientSession
from bs4 import BeautifulSoup
from pathlib import Path

import hashlib
import logging
from time import time



# Download:
# - Take URL 
#   --> Save HTML
#   --> Find URLs and save them (which ones?)
#      --> over ons (main)
#      --> duurzaamheid / susteinability
#      --> annual report
#      --> privacy
#      --> contact / find kvk
#   --> Corporate group: https://api.opencorporates.com/documentation/API-Reference
#   --> https://opencorporates.com/api_accounts/open_data

# logging
logging.basicConfig(filename="logs/scraper.log", level=logging.INFO)
logger = logging.getLogger()
# if os.environ.get('scraper_logging_level', 'error') == "info":
#     logger.setLevel(logging.INFO)
# else:
#     logger.setLevel(logging.ERROR)

def clean_url(url):
    # clean url
    url_parsed = urlparse(url)
    # remove query string
    return url_parsed._replace(query="").geturl()

def fix_url(url, base_url):
    """
    Keeps only urls from the same domain.
    Adds the base domain
    """
    if url.startswith('/'):
        return clean_url(urljoin(base_url, url))
    elif url.startswith(base_url):
        return clean_url(url)
    else:
        return None

def get_current_date():
    # print current day in format "YYYY-MM-DD"
    import datetime
    return datetime.datetime.now().strftime("%Y-%m-%d")




async def fetch_one_url(url, session, kvk="", save_html=True):
    # be nice and wait a second per url
    await asyncio.sleep(1)
    urls = []
    # hash url
    hash_url = hashlib.sha1(url.encode()).hexdigest()

    try:
        async with session.get(url) as response:
            r = await response.read()
            status = response.status
            if status == 200:

                # resp.content is a byte array, convert to string
                contents = r.decode("utf-8", "ignore")

                if save_html:
                    path = f"data/scraped_data/{kvk}/{urlparse(url).netloc}/{get_current_date()}/{hash_url}_{url.split('/')[-1]}"
                    # create folder if it doesn't exist
                    Path(path).parent.mkdir(parents=True, exist_ok=True)

                    # write raw contents to file
                    with open(path, "w") as f:
                        f.write(contents)
                
                # parse
                soup = BeautifulSoup(contents, 'html.parser')
                # extract urls from html code in beautiful soup
                urls = [a.attrs.get('href') for a in soup.select('a[href]')]
                # filter out urls with the same base
                urls = [fix_url(url_found, url) for url_found in urls]
                urls = [url for url in urls if url is not None]


                if len(urls) == 0:
                    logger.info(f'scraper finished for {url}')
                else:
                    logger.info(f'scraper {len(urls)} extra urls found for {url}')
                    
                

            else:
                logger.error(f'scraper failed to scrape "{url}"\nResponse status: {response.status}')

    except Exception as e:
        logger.error(f'scraper failed to scrape "{url}"\nException: {str(e)}')
        status = str(e)

    # save records to file
    with open("data/overview_urls.tsv", "a") as f:
        f.write(f"{kvk}\t{urlparse(url).netloc}\t{url}\t{status}\t{get_current_date()}\t{hash_url}_{url.split('/')[-1]}\n")

    return urls

async def fetch_one_company(url, session, max_level=3, save_html=True):
    # name and url
    kvk, url = url

    level = 0
    all_records = [url]
    records = [url]


    # Breath first search algorithm from urls
    while (len(records) > 0) and (level < max_level):
        tasks = []
        level += 1

        # fetch urls asynchroneously
        for url in records:
            task = asyncio.ensure_future(fetch_one_url(url, session, save_html=save_html, kvk=kvk))
            tasks.append(task) 
        records = await asyncio.gather(*tasks) 
        # flatten list python and remove duplicates
        records = set([item for sublist in records  if sublist is not None for item in sublist])

        # speed up search using a set
        temp_all_records = set(all_records)
        # removing alrady downloaded urls
        records = [url for url in records if url not in temp_all_records]

        # adding new urls to list
        all_records += records

    logger.info(f'scraper for {url} finished with {len(all_records)} downloaded')
    return all_records

async def fetch_all(records, max_level=3, save_html=True):
    tasks = []
    async with ClientSession() as session:
        for url in records:
            task = asyncio.ensure_future(fetch_one_company(url, session, max_level=max_level, save_html=save_html))
            tasks.append(task) 
        _ = await asyncio.gather(*tasks) 
    return _

def handler(urls, max_level=3, save_html=True):
    logger.info(f'scraper received {len(urls)} urls')
    loop = asyncio.get_event_loop() 
    future = asyncio.ensure_future(fetch_all(urls, max_level=max_level, save_html=save_html)) 
    r = loop.run_until_complete(future) 

    #flatten list python
    r = [item for sublist in r  if sublist is not None for item in sublist]

    # save to file
    with open("data/test_classifier.tsv", "w+") as f:
        for url in set(r):
            f.write(f"{url}\n")

    return len(r)

if __name__ == '__main__':
    # Add schema to url


    # urls = [
    #     (66939682, 'http://10printendruk.nl/'),
    #     (67265006, 'https://www.123afval.nl/'),
    #     (71801693, 'https://www.123ragers.nl/'),
    #     (71473300, 'http://101bhvshop.nl/')
    # ]

    start = time()
    max_level = 1
    save_html = True

    with open("/Users/garci061/ownCloud/Shared/2022_backbone/sidn_test.csv", "r") as f:
        f.readline() #header
        urls = [line.split(",") for line in f.readlines()]        
        urls = [(kvk.strip(), f"http://{url}") for url, kvk in urls]


    num_links = handler(urls, max_level=max_level, save_html=save_html)

    with open("data/overview_urls.tsv") as f:
        count = 0
        for line in f:
            if line.split("\t")[3] == "200":
                count += 1

    print(f"Downloaded {count} pages from {len(urls)} urls to level {max_level} in {time() - start:2.1f} seconds. Found {num_links} links")
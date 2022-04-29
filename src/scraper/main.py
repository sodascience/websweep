
import re
from time import time
from urllib.parse import urlparse
from scraper import Scraper


def classify_url(url, level):
    """
    Classify url based on level
    """

    #Avoid mailto and tel
    if re.search(r"mailto:|tel:", url):
        return False
    
    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path
    
    
    # Maybe if there are many links in one level we can skip it

    # Download first level and important sites of the secodn level (level 0 = root website)
    if level == 0:
        return True
    elif level == 1:
        # Cloudfare protection --> reject
        # regex = re.compile( "|".join(map(re.escape, keywords)))
        # TODO: Exclude *xml$
        regex = re.compile(r'pdf$|jpeg$|gif$|svg$|jpg$|png$|collections|email\-protection|product|aanbod|assortiment|voorraad|koop|shop|artikelen|merken|wintersport|bouw|zoeken|search')
        if re.search(regex, url):
            return False
        # If only numbers and characters (e.g. https:/www.horstingkilder.nl/553-504") --> reject
        elif re.search("^[^a-zA-Z]+$",url):
            return False
        else:
            return True
    elif level == 2:
        # Keep only if it seems important
        regex = re.compile(r'over\-ons|contact|duurzaamheid|index\.php|algemene\-voorwaarden|vacatures|disclaimer|klantenservice|privacy\-policy|cookie\-policy|cookies|cookie|cookie\-beleid|over|overons|blogs|privacyverklaring|about|about\-us')
        if re.search(regex, url):
            return True
        else:
            return False
    else:
        return False

#/Users/garci061/miniforge3/envs/backbone/bin/python -m cProfile -o /tmp/tmp.prof /Users/garci061/pCloud_sync/backbone/corporate_scraper/src/scraper/main.py
#snakeviz /tmp/tmp.prof

#kernprof -o /tmp/tmp.lprof -l  /Users/garci061/pCloud_sync/backbone/corporate_scraper/src/scraper/main.py
#python -m line_profiler /tmp/tmp.lprof

#pprofile  --out /tmp/temp.txt src/scraper/main.py
#pprofile  --format callgrind --out /tmp/cachegrind.out.threads src/scraper/main.py
#qcachegrind /tmp/cachegrind.out.threads
if __name__ == "__main__":
    # urls = [
    #     (66939682, 'http://10printendruk.nl/'),
    #     (67265006, 'https://www.123afval.nl/'),
    #     (71801693, 'https://www.123ragers.nl/'),
    #     (71473300, 'http://101bhvshop.nl/')
    # ]

    start = time()
    
    with open("data/tmp_data/sidn_test.csv", "r") as f:
        f.readline() #header
        urls = [line.split(",") for line in f.readlines()]        
        urls = sorted([(kvk.strip(), f"https://www.{url}/") for url, kvk in urls])
    print(len(urls))

    # Run scraper
    # Start scraper, downloading 20 companies in parallel
    # Depending on the internet connection and CPU, the concurrency of downloading (threads_download, concurrency_companies) should be increased, or the concurrency of processing (threads_bs4) should be increased
    scraper = Scraper(save_html=True, max_level=3, base_path="data/scraped_data", 
                      classifier=classify_url, concurrency_companies=100, 
                      threads_download=100, threads_bs4=10)
    scraper.scrape_companies(urls[:100])

    #Read what we did
    with open("data/overview_urls.tsv") as f:
        count = 0
        for line in f:
            if line.split("\t")[4] == "200":
                count += 1
    print(f"Downloaded {count} pages from {len(urls)} urls to level {3} in {time() - start:2.1f} seconds.")


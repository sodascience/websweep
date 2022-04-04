
import re
from time import time
from urllib.parse import urlparse
from scraper import Scraper
import cProfile

def classify_url(url, level):
    """
    Classify url based on level
    """

    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path
    
    # Download first level and important sites of the secodn level (level 0 = root website)
    if level == 0:
        return True
    elif level == 1:
        # Cloudfare protection --> reject
        # regex = re.compile( "|".join(map(re.escape, keywords)))
        regex = re.compile(r'collections|email\-protection|product|aanbod|assortiment|voorraad|koop|shop|artikelen|merken|wintersport|bouw|zoeken|search')
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
    # Start scraper
    scraper = Scraper(save_html=True, max_level=3, base_path="data/scraped_data", classifier=classify_url)
    scraper.scrape_companies(urls)

    #Read what we did
    with open("data/overview_urls.tsv") as f:
        count = 0
        for line in f:
            if line.split("\t")[4] == "200":
                count += 1
    print(f"Downloaded {count} pages from {len(urls)} urls to level {3} in {time() - start:2.1f} seconds.")

    # Run the scraper with batchs of 100 urls:
    if 0:
        # split urls into batches of 100
        urls_batches = [urls[i:i+100] for i in range(0, len(urls), 100)]


        for urls_batch in urls_batches:
            # Start scraper
            scraper = Scraper(save_html=False, max_level=3, base_path="data/scraped_data", classifier=classify_url)
            scraper.scrape_companies(urls_batch)


            #Read what we did
            with open("data/overview_urls.tsv") as f:
                count = 0
                for line in f:
                    if line.split("\t")[4] == "200":
                        count += 1
            print("Batch done")
            print(f"Downloaded {count} pages from {len(urls_batch)} urls to level {3} in {time() - start:2.1f} seconds.")


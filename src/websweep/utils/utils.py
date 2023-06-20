import re
import sqlite3 as sql
from pathlib import Path
from urllib.parse import urlparse


def classify_url(url, level, classification_file_path = None):
    """
    Classify url based on level
    """

    if level == 0:
        return True

    # Taking the path (next step) will remove this part, we need to catch it before
    regex = re.compile(r"^mailto:|^tel:", re.IGNORECASE)
    if re.search(regex, url):
        return False

    # Detect annual report, download them!
    pass

    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path

    # Don't download these
    regex = re.compile(
        r"png$|jpg$|jpeg$|pdf$|collections|email\-protection|product|aanbod|assortiment|voorraad|koop|shop|artikelen|merken|wintersport|bouw|zoeken|search",
        re.IGNORECASE,
    )
    if re.search(regex, url):
        return False
    # Maybe if there are many links in one level we can skip it

    # Download first level and important sites of the secodn level (level 0 = root website)
    if level == 1:
        # Cloudfare protection --> reject
        # If only numbers and characters (e.g. https:/www.horstingkilder.nl/553-504") --> reject
        if re.search("^[^a-zA-Z]+$", url):
            return False
        else:
            return True
    if level == 2:
        # Keep only if it seems important

        if classification_file_path != None:
            # Open the text file
            with open(classification_file_path, 'r') as file:
                # Read the file contents
                content = file.read()

            # Split the content into delimited items
            items = content.split(';')
            items = [item.replace(' ', r'.*').lower() for item in items]

            # Escape special characters and join items into a regex string
            regex_string = '|'.join(items)

        else:
            regex_string = r"over\-ons|contact|duurzaamheid|index\.php|algemene\-voorwaarden|vacatures|disclaimer|klantenservice|privacy\-policy|cookie\-policy|cookies|cookie|cookie\-beleid|over|overons|blogs|privacyverklaring|about|about\-us"

        regex = re.compile(regex_string, re.IGNORECASE,)
        report_regex = re.compile(r"""
                            financiele.?rapportage|annual.?report|jaarrekening|jaar.?verslag|jaarrapport|boekhouding.?rapportage|boekhouding.?rapport|financial.?performance|investor.?relations|investeerder.?relaties|financial.?results|financial.?statement
                            """, re.VERBOSE | re.IGNORECASE)
        
        if re.search(regex, url) or re.search(report_regex, url):
            return True
        else:
            return False
    else:
        return False

def clean_url(url):
    return re.sub(r"(https?://)?(www\.)?", "", url)
import re
import sqlite3 as sql
from pathlib import Path
from urllib.parse import urlparse
import json


def classify_url(url, level, classification_file_path = None) -> bool:
    """
    Classify url based on level
    """

    if level == 0:
        return True

    # Taking the path (next step) will remove this part, we need to catch it before
    url_regex = re.compile(r"^mailto:|^tel:", re.IGNORECASE)
    if re.search(url_regex, url):
        return False

    # Detect annual report, download them!
    pass

    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path

    # Don't download these
    url_regex = re.compile(
        r"png$|jpg$|jpeg$|pdf$|collections|email\-protection|product|aanbod|assortiment|voorraad|koop|shop|artikelen|merken|wintersport|bouw|zoeken|search",
        re.IGNORECASE,
    )
    if re.search(url_regex, url):
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

        if classification_file_path == None:
            classification_file_path = Path(__file__).with_name('default_regex.json')

        with open(classification_file_path, 'r') as file:
            content = file.read()
        data = json.loads(content)

        url_keywords = [keyword.replace(' ', r'.*').lower() for keyword in data['url']['url_keywords']]
        url_keywords = [keyword.strip() for keyword in url_keywords]
        url_regex = data['url']['url_regex']
        if url_keywords and url_regex != "":
            url_regex += '|' + '|'.join(url_keywords)
        elif url_regex == "":
            url_regex = '|'.join(url_keywords)

        report_keywords = [keyword.replace(' ', r'.*').lower() for keyword in data['report']['report_keywords']]
        report_keywords = [keyword.strip() for keyword in report_keywords]
        report_regex = data['report']['report_regex']
        if report_keywords and report_regex != "":
            report_regex += '|' + '|'.join(report_keywords)
        elif report_regex == "":
            report_regex = '|'.join(report_keywords)

        url_regex = re.compile(url_regex, re.IGNORECASE,)
        report_regex = re.compile(report_regex,re.IGNORECASE)
        
        if re.search(url_regex, url) or re.search(report_regex, url):
            return True
        else:
            return False
    else:
        return False

def clean_url(url):
    return re.sub(r"(https?://)?(www\.)?", "", url)
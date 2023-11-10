import regex as re
import sqlite3 as sql
from pathlib import Path
from urllib.parse import urlparse
import json

def create_regex_pattern(keywords, regex):
    keywords = [keyword.replace(' ', r'.*').lower() for keyword in keywords]
    keywords = [keyword.strip() for keyword in keywords]

    if keywords and regex != "":
        regex += '|' + '|'.join(keywords)
    elif regex == "":
        regex = '|'.join(keywords)

    return re.compile(regex, re.IGNORECASE)


def set_regex(classification_file_path = None):
    url_regex_mail = re.compile(r"^mailto:|^tel:", re.IGNORECASE)

    # Load the default regex expressions
    if classification_file_path is None:
            classification_file_path = Path(__file__).with_name('default_regex.json')

    with open(classification_file_path, 'r') as file:
        content = file.read()
    default_regex_data = json.loads(content)

    # Regex to not download
    negative_regex = create_regex_pattern(default_regex_data['negative']['negative_keywords'], default_regex_data['negative']['negative_regex'])
    
    # Only download sometimes
    url_regex = create_regex_pattern(default_regex_data['url']['url_keywords'], default_regex_data['url']['url_regex'])
    report_regex = create_regex_pattern(default_regex_data['report']['report_keywords'], default_regex_data['report']['report_regex'])
        
    return url_regex_mail, negative_regex, url_regex, report_regex

def classify_url(url, level, url_regex_mail, negative_regex, url_regex, report_regex) -> bool:
    """
    Classify url based on level
    """

    ##TODO: Create classify_url from crawler, reading default_regex.json (modfiying it so it's by level)

    if level == 0:
        return True

    # Taking the path (next step) will remove this part, we need to catch it before
    if re.search(url_regex_mail, url):
        return False

    # Detect annual report, download them!
    pass

    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path

    # Don't download these    
    if re.search(negative_regex, url):
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
        # Keep only important
        if re.search(url_regex, url) or re.search(report_regex, url):
            return True
        else:
            return False
    else:
        return False

def clean_url(url):
    return re.sub(r"(https?://)?(www\.)?", "", url)
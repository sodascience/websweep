import re
from urllib.parse import urlparse

def classify_url(url, level):
    """
    Classify url based on level
    """

    # Keep the path only (avoid to reject websites such as "awesomeshop.nl/important_information")
    url = urlparse(url).path

    # Maybe if there are many links in one level we can skip it

    # Download first level and important sites of the secodn level (level 0 = root website)
    if level == 0:
        return True
    elif level == 1:
        # Cloudfare protection --> reject
        # regex = re.compile( "|".join(map(re.escape, keywords)))
        regex = re.compile(r'^mailto:|^tel:|pdf$|collections|email\-protection|product|aanbod|assortiment|voorraad|koop|shop|artikelen|merken|wintersport|bouw|zoeken|search')
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
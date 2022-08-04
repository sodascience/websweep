import os
import re
import typer
from xmlrpc.client import Boolean
from tika import parser

class Extractor:
    def __init__(self, info):
        self.metadata = dict()
        self.metadata["id"], self.metadata["domain"], self.metadata["level"], self.metadata["website"], self.metadata["date"], self.metadata["path"] = info    

    def extracting(self):
        # Get metadata
        self.extract_metadata(self.path)

        # TODO: Log this behavior
        if self.text is None:
            return self.metadata

        self.scrape_zip()
        self.scrape_adres()
        self._zipcode_warning()

        self.scrape_kvk()
        self.scrape_btw()
        
        self._clean_html(self.text)

        # Phone/emails/fax can be found in the HTML?
        with open(self.path, "r", encoding="UTF-8") as file:
            self.text = file.read()

        self.scrape_phone()
        self.scrape_email()
        self.scrape_fax()
 
        
        if self.mistake_warning():
            print(f"finished {self.website}")
        else:
            typer.secho(
                    f'No values could be found for {self.website}',
                    fg=typer.colors.RED,
                )
        
        return self.metadata

    def scrape_adres(self) -> None:
        """
        Scrape the adres from the input file, and add found adres to self.adres in set form
        """
        add_found = []
        for zipcode in self.metadata["postcode"]:
            pattern = r'(((\b[a-zA-ZÀ-ÿ]+)\s+[0-9][0-9-_a-z,]*)(\s+(?=(' + zipcode + r'))|(?=(' + zipcode + '))))'
            findall = re.findall(pattern, self.text)
            add_found += [item[1] for item in findall]
        
        self.metadata["address"] = add_found

    def scrape_zip(self) -> None:
        """
        Scrape the zipcode from the input file, and add found zipcodes to self.zip_code in set form
        """
    
        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """, re.VERBOSE)
        zip_codes = list(set(re.findall(pattern, self.text)))
        
        self.metadata["postcode"] = zip_codes
        
    def scrape_btw(self) -> None:
        """
        Scrape the BTW number from the input file, and add found BTW Numbers (usually only 1) to self.btw in list form
        """
        self.btw = set()

        pattern = re.compile(r"""
                                (btw|BTW|VAT|vat)(.+)(\bNL\s*[0-9-_.]{9,12}\s*[B 0-9\.]{0,5}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.btw.add(item[2])
        
        return list(self.btw)

    def scrape_kvk(self) -> None:
        """
        Scrape the KVK number from the input file, and add found KVK number to self.kvk in set form
        """
        
        self.kvk = set()
        
        pattern = re.compile(r"""
                                k\.?v\.?k.{0,12}?(\b\d{8}) | (\b\d{8}).{0,5}?k\.?v\.?k
                                """, re.VERBOSE | re.IGNORECASE)
        result_list = re.findall(pattern, self.text)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            for subitem in item:
                if len(subitem) > 0:
                    self.kvk.add(subitem)

        self.metadata["kvk"] = list(self.kvk)
        
    def scrape_phone(self) -> None:
        """
        Scrape the phone number from the input file, and add found phone numbers to self.phone in set form
        """
        
        self.phone = set()
        pattern = re.compile(r"""
                                (Tel:\s{0,4}|
                                tel:\s{0,4}|
                                telefoon:\s{0,4}|
                                Telefoon:\s{0,4}|
                                T:\s{0,4}|
                                t:\s{0,4}|
                                T\s{0,4})
                                (&nbsp;|/{0,2})?
                                ((\+?|\"?)(\d|\s|\(|\)|-){9,22})\d
                                """, re.VERBOSE)
                                # Phone numbers can be indicated by a variety of different ways, this regex tries to incorporate all of those as a possibillity
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            temp = item[0] + ' ' + item[2]
            self.phone.add(temp)
        self.metadata["phone"] = list(self.phone)
        
    def scrape_fax(self) -> None:
        """
        Scrape the fax number from the input file, and add found fax numbers to self.fax in set form
        """
        
        self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                ((\+?|\"?)(\d|\s|\(|\)|-){10,22})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.fax.add(item[1])
        
        self.metadata["fax"] = list(self.fax)

    def scrape_email(self) -> None:
        """
        Scrape the Email adress from the input file, and adds the found email adress to self.email in set form
        """
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]{1,25}  # again, grab any number or letter
                        \.                  # the dot in the email
                        (?!png)(?!jpg)[a-zA-Z-.]{1,8}     # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""", re.VERBOSE)
        self.email = set(re.findall(pattern, self.text))
        emails = [email[:-1] if email[-1] == '.' else email for email in self.email]
        self.metadata["email"] = emails
        
    def mistake_warning(self) -> "Boolean":
        if not self.__dict__['email'] and not self.__dict__['kvk'] and not self.__dict__['phone'] and not self.__dict__['btw'] and not self.__dict__['fax'] and not self.__dict__['zip_code']:
            return False
        else:
            return True

    def extract_metadata(self, file_path) -> None:
        """        
        This function is used to extract the metadata from the file, and return it as a dictionary.
        # Example of metadata
        {'Content-Encoding': 'U
TF-8',
        'Content-Language': 'nl',
        'Content-Type': 'text/html; charset=UTF-8',
        'Content-Type-Hint': 'text/html; charset=UTF-8',
        'X-Parsed-By': ['org.apache.tika.parser.DefaultParser',
        'org.apache.tika.parser.html.HtmlParser'],
        'X-TIKA:content_handler': 'ToTextContentHandler',
        'X-TIKA:embedded_depth': '0',
        'X-TIKA:parse_time_millis': '18',
        'dc:title': 'Over Webo | WEBO Heftrucks B.V.',
        'description': "We schrijven begin jaren '50. Nederland is letterlijk een land in opbouw en mensen doen hun uiterste best om de enorme schade van de tweede wereldoorlog te hers...",
        'og:description': "We schrijven begin jaren '50. Nederland is letterlijk een land in opbouw en mensen doen hun uiterste best om de enorme schade van de tweede wereldoorlog te hers...",
        'og:locale': '_',
        'og:site_name': 'WEBO Heftrucks B.V.',
        'og:title': 'Over Webo | WEBO Heftrucks B.V.',
        'og:url': 'https://www.weboheftrucks.nl/over-webo/',
        'resourceName': "b'1a0b97173df88eceedc23673381c49afa3f2f927_over-webo'",
        'title': 'Over Webo | WEBO Heftrucks B.V.',
        'viewport': 'width=device-width, initial-scale=1.0'}
        """
        # TODO: check if tika is active, if it's not default to BS4
        # from bs4 import BeautifulSoup
        # self.text = BeautifulSoup(text, 'html.parser').text
        parsed = parser.from_file(file_path, requestOptions={'timeout': 120})
        if parsed["status"] != 200:
            # TODO: Add to log file
            return {}
        
        self.metadata.update(parsed["metadata"])
        self.text = parsed["content"]

    def mistake_warning(self) -> "Boolean":
        """
        Checks if the extractor has ANY values at all, if it found none, it will return False
        """
        if not self.metadata['email'] and not self.metadata['id'] and not self.metadata['phone'] and not self.metadata['btw'] and not self.metadata['fax'] and not self.metadata['postcode']:
            return False
        else:
            return True

    def _zipcode_warning(self) -> None:
        """
        If the extractor did find a zipcode, but not an address, there is probably a bug. This notifies the user.=
        """
        if (len(self.metadata["postcode"]) > 0) and (len(self.metadata["address"]) == 0):
            typer.secho(
                    f'Found zipcodes, but found no address for {self.website}, this is probably a bug',
                    fg=typer.colors.YELLOW,
                )

    def _clean_html(self, text) -> None:
        """
        Clean text extracted by tika
        """
        # keep only > 100 characters
        text = "\n".join([_ for _ in text.split("\n") if len(_) > 100])
        self.metadata["text"] = text
        
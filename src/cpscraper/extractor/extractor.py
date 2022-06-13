import os
import re
import typer
from tika import parser

class Extractor:
    def __init__(self, info):
        self.id, self.domain, self.level, self.website, self.date, self.path = info
        
    def run_loops(self):
        # Get metadata
        metadata = self.extract_metadata(self.path)
        metadata["id"] = self.id
        metadata["domain"] = self.domain
        metadata["level"] = self.level
        metadata["website"] = self.website
        metadata["date"] = self.date
        metadata["path"] = self.path

        # TODO: Log this behavior
        if self.text is None:
            return metadata

        metadata["address"] = self.scrape_adres()
        metadata["postcode"] = self.scrape_zip()
        metadata["kvk"] = self.scrape_kvk()
        metadata["btw"] = self.scrape_btw()
        metadata["text"] = self._clean_html(self.text)

        # Phone/emails/fax can be found in the HTML
        with open(self.path, "r", encoding="UTF-8") as file:
            self.text = file.read()

        metadata["phone"] = self.scrape_phone()
        metadata["email"] = self.scrape_email()
        metadata["fax"] = self.scrape_fax()
 
        
        if self.mistake_warning():
            print(f"finished {self.website}")
        else:
            typer.secho(
                    f'No values could be found for {self.website}',
                    fg=typer.colors.RED,
                )
        
        return metadata

    def scrape_adres(self):
        """
        Scrape the adres from the input file, and add found adres to self.adres in set form
        """
        return []
        

    def scrape_zip(self):
        """
        Scrape the zipcode from the input file, and add found zipcodes to self.zip_code in set form
        """
        
        self.zip_code = set()

        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.zip_code.add(item)

        return list(self.zip_code)


    def scrape_btw(self):
        """
        Scrape the BTW number from the input file, and add found BTW Numbers (usually only 1) to self.btw in list form
        """
        self.btw = set()

        pattern = re.compile(r"""
                                (btw|BTW|VAT|vat)(.+)(\bNL\s*[0-9-_.]{9,12}\s*[B 0-9\.]{0,4}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.btw.add(item[2])
        
        return list(self.btw)

    def scrape_kvk(self):
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
        return list(self.kvk)
        
  
        

    def scrape_phone(self):
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
                                ((\+?|\"?)(\d|\s|\(|\)|-){10,22})
                                """, re.VERBOSE)
                                # Phone numbers can be indicated by a variety of different ways, this regex tries to incorporate all of those as a possibillity
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            temp = item[0] + ' ' + item[2]
            self.phone.add(temp)
        
        return list(self.phone)

    def scrape_fax(self):
        """
        Scrape the fax number from the input file, and add found fax numbers to self.fax in set form
        """
        
        self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                (\b(\d-*){10,})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.fax.add(item[1])
        
        return list(self.fax)

    def scrape_email(self):
        """
        Scrape the Email adress from the input file, and adds the found email adress to self.email in set form
        """
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]{1,20}  # again, grab any number or letter
                        \.                  # the dot in the email
                        (?!png)(?!jpg)[a-zA-Z-.]{1,8}     # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""", re.VERBOSE)
        self.email = set(re.findall(pattern, self.text))
        
        return [email[:-1] if email[-1] == '.' else email for email in self.email]
 

    def mistake_warning(self):
        if not self.__dict__['email'] and not self.__dict__['kvk'] and not self.__dict__['phone'] and not self.__dict__['btw'] and not self.__dict__['fax'] and not self.__dict__['zip_code']:
            return False
        else:
            return True



    def extract_metadata(self, file_path):
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

        parsed = parser.from_file(file_path, requestOptions={'timeout': 120})
        if parsed["status"] != 200:
            # TODO: Add to log file
            return {}

        
        metadata = parsed["metadata"]
        self.text = parsed["content"]

        return metadata

    def _clean_html(self, text):
        """
        Clean text extracted by tika
        """
        # keep only > 100 characters
        text = "\n".join([_ for _ in text.split("\n") if len(_) > 100])
        return text 
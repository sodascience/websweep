
import os
import re
import typer
import tika

class Extractor:
    def __init__(self, working_dir=None, website=None, phone=None, email=None, kvk=None,
                 adres=None, zip_code=None, fax=None, btw=None):
        self.website = website
        self.email = email
        self.phone = phone
        self.kvk = kvk
        self.fax = fax
        self.btw = btw
        self.zip_code = zip_code
        self.adres = adres
        self.working_dir = working_dir


    def run_loops(self):
        for file in os.scandir(self.working_dir):
            open_file = open(file, "r", encoding="UTF-8")
            file_text = open_file.read()
            self.scrape_address(file_text)
            self.scrape_zip(file_text)
            self.scrape_phone(file_text)
            self.scrape_email(file_text)
            self.scrape_fax(file_text)
            self.scrape_kvk(file_text)
            self.scrape_btw(file_text)
            open_file.close()

        self.clean_email()
        self.jsonize()
        print(f"finished off {self.website}")

    def scrape_address(self, file):
        if self.adres is None:
            self.adres = set()
        return

    def scrape_zip(self, file):
        if self.zip_code is None:
            self.zip_code = set()
        pattern = re.compile(r"""
                                (\b\d{4}\s?[a-zA-Z]{2}\b)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.zip_code.add(item)
        return

    def scrape_btw(self, file):
        if self.btw is None:
            self.btw = set()
        pattern = re.compile(r"""
                                (btw|BTW)(.+)(\bNL[0-9-_.]{9,12}B\d{2}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.btw.add(item[2])
        return

    def scrape_kvk(self, file):
        if self.kvk is None:
            self.kvk = set()
        pattern = re.compile(r"""
                                (kvk|KvK|K.v.K.|k.v.k.)(.+)(\b\d{8})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.kvk.add(item[2])
        return

    def scrape_phone(self, file):
        if self.phone is None:
            self.phone = set()
        pattern = re.compile(r"""
                                (Tel:\s*|
                                tel:\s*|
                                telefoon:\s*|
                                Telefoon:\s*|
                                T:\s*|
                                t:\s)
                                (\+?(\s?\d-*){10,11})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.phone.add(item[1])
        return

    def scrape_fax(self, file):
        if self.fax is None:
            self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                (\b(\d-*){10,})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.fax.add(item[1])
        return

    def scrape_email(self, file):
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]+       # again, grab any number or letter
                        \.                  # the dot in the email
                        [a-zA-Z-.]+      # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""", re.VERBOSE)
        emails = set(re.findall(pattern, file))
        if self.email is None:
            self.email = emails
        else:
            self.email.update(emails)

    def clean_email(self):
        """
        The used RegEx occasionally grabs the '.' at the end of an email, (while it's signing the end of a sentence),
        this function removes that dot (as well as providing an easy-to-extend way to further add some email-cleaning)
        """
        temp_emails = set()
        for email in self.email:
            if email[-1] == ".":
                temp_emails.add(email[:-1])
            else:
                temp_emails.add(email)
        self.email = temp_emails

    def jsonize(self):
        self.email = list(self.email)
        self.phone = list(self.phone)
        self.kvk = list(self.kvk)
        self.fax = list(self.fax)
        self.btw = list(self.btw)
        self.zip_code = list(self.zip_code)
        self.adres = list(self.adres)

    def mistake_warning(self):
        for key, value in self.__dict__.items():
            if not value:
                typer.secho(
                    'No value could be found for {key} at {self.working_dir}',
                    fg=typer.colors.RED,
                )

    def extract_metadata(self, file_path):
        """        
        This function is used to extract the metadata from the file, and return it as a dictionary.

        # Example of metadata
        {'Content-Encoding': 'UTF-8',
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
    
        parsed = tika.parser.from_file(file_path, requestOptions={'timeout': 120})
        if parsed["status"] != 200:
            # TODO: Add to log file
            return {}

        text = self._clean_html(parsed["content"])
        metadata = parsed["metadata"]
        metadata["text"] = text

        return metadata
            
    def _clean_html(self, text):
        """
        Clean text extracted by tika
        """
        # keep only > 100 characters
        text = "\n".join([_ for _ in text.split("\n") if len(_) > 100])
        return text
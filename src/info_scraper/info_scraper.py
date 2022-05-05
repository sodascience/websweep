import logging
import os
import re
import time

# logging.basicConfig(filename="logs/info_scraper.log", level=logging.INFO)
# logger = logging.getLogger()
import timeit


class InfoScraper:
    def __init__(self, working_dir=None, phone_number: list = None, email: set = None, kvk: str = None,
                 adress: list = None, zip_code: list = None):
        self.zip_code = zip_code
        self.adress = adress
        self.kvk = kvk
        self.email = email
        self.phone_number = phone_number
        self.working_dir = working_dir

    def run_loops(self):
        for file in os.scandir(self.working_dir):
            open_file = open(file, "r", encoding="UTF-8")
            file_text = open_file.read()
            self.scrape_address(file_text)
            self.scrape_phone(file_text)
            self.scrape_BTW(file_text)
            self.scrape_email(file_text)
            open_file.close()

        self.clean_email()
        self.save_to_json()


    def scrape_address(self, file):
        return

    def scrape_phone(self, file):

        return

    def scrape_BTW(self, file):
        return

    def scrape_email(self, file):
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]+       # any 
                        \.
                        [a-zA-Z0-9-.]+
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

    def validate_phone(self):
        validate_phone_number_pattern = "^\\+?\\d{1,4}?[-.\\s]?\\(?\\d{1,3}?\\)?[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,9}$"
        temp_phones = set()
        for phone in self.phone_number:
            if re.match(validate_phone_number_pattern, phone):
                temp_phones.add(phone)
        self.phone_number = temp_phones



    def save_to_json(self):
        print(self.email, self.phone_number, self.kvk, self.adress, self.zip_code)
        return

import logging
import os
import re
from bcolors import bcolors
import time
import json
from datetime import date
from pathlib import Path

# logging.basicConfig(filename="logs/info_scraper.log", level=logging.INFO)
# logger = logging.getLogger()



class InfoScraper:
    def __init__(self, working_dir=None, phone=None, email=None, kvk=None,
                 adres=None, zip_code=None, fax=None, btw=None):
        self.zip_code = zip_code
        self.adres = adres
        self.kvk = kvk
        self.email = email
        self.phone = phone
        self.fax = fax
        self.btw = btw
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
        self.save_to_json()
        #self.mistake_warning()

    def scrape_address(self, file):
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
                                (btw|BTW)(.+)(\bNL\d{9}B\d{2}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.btw.add(item[2])
        return

    def scrape_kvk(self, file):
        if self.kvk is None:
            self.kvk = set()
        pattern = re.compile(r"""
                                (kvk|KvK)(.+)(\b\d{8})
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
                                Telefoon:\s*)
                                (\+?(\s?\d-*){10,})
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
                                fax:\s)
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
                        [a-zA-Z0-9-.]+      # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
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

    # TODO FIX THIS
    def save_to_json(self):
        # file_path = Path(__file__).parents[2] / 'data' / 'Information'
        # file_name = '\\' + str(date.today()) + '.json'
        # saved_json = str(file_path) + file_name
        # file = open(saved_json, 'a')
        # to_write_dict = {
        #     'Emails': list(self.email),
        #     'Phone Numbers': list(self.phone),
        #     'KVK Numbers': list(self.kvk),
        #     'Adresses': list(self.adress),
        #     'Zip codes': list(self.zip_code)
        # }
        # prettify = json.dumps(to_write_dict, indent=4)
        # file.write(prettify)
        # file.close()
        print(f"Email: {self.email}, \nTelefoon: {self.phone}, \nKVK: {self.kvk}, \nAdress: {self.adres, self.zip_code}, \nFax: {self.fax}, \nbtw: {self.btw} \n")
        return

    def mistake_warning(self):
        for key, value in self.__dict__.items():
            if not value:
                print(f"{bcolors.FAIL} No value could be found for {key} at {self.working_dir}{bcolors.RESET}")

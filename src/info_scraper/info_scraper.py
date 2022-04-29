import logging
import os
import re


# logging.basicConfig(filename="logs/info_scraper.log", level=logging.INFO)
# logger = logging.getLogger()

class InfoScraper:
    def __init__(self, working_dir=None, phone_number: list = None, email: list = None, kvk: str = None, adress: list = None, zip_code: list = None):
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

        self.save_to_json()

    def scrape_address(self, file):
        print(f"Adress is gescraped! from")

    def scrape_phone(self, file):
        print(f"Telefoon nummer is gescraped! from")

    def scrape_BTW(self, file):
        print(f"BTW Nummer is gescraped! from")

    def scrape_email(self, file):
        print(f"Email Adress is gescraped! from")

    def save_to_json(self):
        return

import logging


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

    def run_loops(self, current_working_file):
        self.scrape_address(current_working_file)
        self.scrape_phone(current_working_file)
        self.scrape_BTW(current_working_file)
        self.scrape_email(current_working_file)

        self.save_to_json()

    def scrape_address(self, file):
        print(f"Adress is gescraped! from {file}")

    def scrape_phone(self, file):
        print(f"Telefoon nummer is gescraped! from {file}")

    def scrape_BTW(self, file):
        print(f"BTW Nummer is gescraped! from {file}")

    def scrape_email(self, file):
        print(f"Email Adress is gescraped! from {file}")

    def save_to_json(self):
        return

import re
import typer
from xmlrpc.client import Boolean
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from datetime import date as datelib
from pathlib import Path
import sqlite3 as sql
import os
from multiprocess import Pool
import ndjson
from tqdm import tqdm
from shutil import rmtree


# Helper for extracting
def _create_results(path):
    [id, domain, level, url, date, path] = path
    #folder = _get_folder(path)

    start_time_file = time.perf_counter()
    #website_name = os.path.basename(Path(folder).parents[0])
    # TODO: integrate get scraper into _get_scraper method since now no checks are performed if target folders exist
    cached_corporate = Extractor([id, domain, level, url, date, path])
    metadata = cached_corporate.extracting()
    end_time_file = time.perf_counter()
    
    return ({path: end_time_file - start_time_file }, metadata)
    

class Extractor:
    def __init__(self, info, target_folder_path, use_sqlite = False, extractor_delete_files = False):
        self.target_folder_path = target_folder_path
        self.use_sqlite = use_sqlite
        self.extractor_delete_files = extractor_delete_files
        self.metadata = dict()
        self.metadata["id"], self.metadata["domain"], self.metadata["level"], self.metadata["website"], self.metadata["date"], self.metadata["path"] = info    

    def extract(self):
        start = time.time()

        file_res = self.target_folder_path  /  ('scraped_data_' + str(datelib.today()) + '.ndjson')
        pdf_file = self.target_folder_path  /  ('pdf_links_' + str(datelib.today()) + '.ndjson')
        pdf_list = []

        Path(file_res).parent.mkdir(parents=True, exist_ok=True)

        date_start = "2000-01-01"
        date_end = "3000-01-01"
        if self.use_sqlite:
            connection = sql.connect(os.path.join(self.target_folder_path,  "overview_urls.db"))
            cursor = connection.cursor()
            results = cursor.execute(f'''SELECT id, domain, level, url, date, path FROM Overview 
                            WHERE (date >= '{date_start}') 
                            AND (date <= '{date_end}') 
                            AND (status == "200")''').fetchall()
            connection.close()
        else:
            with open(os.path.join(self.target_folder_path, "overview_urls.tsv")) as f:
                f.readline() #header
                results = []
                for line in f:
                    id, domain, level, url, status, date, path = line.split("\t")
                    if (date >= date_start) and (date <= date_end) and (status == "200"):
                        results.append([id, domain, level, url, date, path.strip()])

        # Parallelize loop 
        with Pool() as pool, open(file_res, "w+", encoding='UTF-8') as f_res:
            writer_res = ndjson.writer(f_res, ensure_ascii=False)

            with tqdm.tqdm(total=len(results), leave = True, miniters=1) as pbar:
                for result in pool.imap_unordered(_create_results, results):
                    time_dict, json_dict = result
                    writer_res.writerow(json_dict)
                    pbar.update()
                    if json_dict["pdf_links"] != []:
                        print(json_dict['pdf_links'])
                        pdf_list.append(json_dict["pdf_links"])

        with open(pdf_file, "w+", encoding='UTF-8') as pdf_res:
            pdf_writer = ndjson.writer(pdf_res, ensure_ascii=False)
            pdf_writer.writerow(pdf_list)

        print(f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds.")

        if self.delete_processed_files:
            data_folder = os.path.join(self.target_folder_path, "data")
            for folder in os.listdir(data_folder):
                if os.path.isdir(folder):
                    rmtree(os.path.join(data_folder, folder))

    def extracting(self):

        # Phone/emails/fax can be found in the HTML
        with open(self.metadata["path"], "r", encoding="UTF-8") as file:
            self.text = file.read()
            self.soup = BeautifulSoup(self.text,"lxml")

        # Get metadata
        self.extract_metadata()

        #Extract the data
        self.extract_kvk()
        self.extract_btw()
        self.extract_phone()
        self.extract_email()
        self.extract_fax()
        self.extract_annual_report()

        #Zip and address can better be found in raw text
        self._clean_html()

        self.extract_zip()
        self.extract_address()

        return self.metadata

    def extract_address(self) -> None:
        """
        Scrape the adres from the input file, and add found adres to self.adres in set form
        """
        add_found = []
        for zipcode in self.metadata["zipcode"]:
            pattern = r'(((\b[a-zA-ZÀ-ÿ]+)\s+[0-9][0-9-_a-z,/]*)(\s+(?=(' + zipcode + r'))|(?=(' + zipcode + '))))'
            findall = re.findall(pattern, self.text)
            add_found += [item[1] for item in findall]
        
        self.metadata["address"] = add_found

    def extract_zip(self) -> None:
        """
        Scrape the zipcode from the input file, and add found zipcodes to self.zipcode in set form
        """
    
        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """, re.VERBOSE)
        zipcodes = list(set(re.findall(pattern, self.text)))
        
        self.metadata["zipcode"] = zipcodes
        
    def extract_btw(self) -> None:
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
        self.metadata['btw'] = list(self.btw)

    def extract_kvk(self) -> None:
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
        
    def extract_phone(self) -> None:
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
                                T\s{0,4}|
                                T\.\s{0,4})
                                (&nbsp;|/{0,2})?
                                ((\+?|\"?)(\d|\s|\(|\)|-){9,22}\d)
                                """, re.VERBOSE)
                                # Phone numbers can be indicated by a variety of different ways, this regex tries to incorporate all of those as a possibillity
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            temp = item[0] + ' ' + item[2]
            self.phone.add(temp)
        self.metadata["phone"] = list(self.phone)
        
    def extract_fax(self) -> None:
        """
        Scrape the fax number from the input file, and add found fax numbers to self.fax in set form
        """
        
        self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                ((\+?|\"?)(\d|\s|\(|\)|-){9,22}\d)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            self.fax.add(item[1])
        
        self.metadata["fax"] = list(self.fax)

    def extract_email(self) -> None:
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
        
    def extract_annual_report(self) -> None:
        """
        Look for and try to scrape the annual report of a website, if found add to #TODO where to add?
        """
        pdf_links = set()
        pattern = re.compile(r"""
                            financiele.?rapportage|annual.?report|jaarrekening|jaar.?verslag|jaarrapport|jaarrekening|boekhouding.?rapportage|boekhouding.?rapport|financial.?performance|investor|investeerder|financial.?results
                            """, re.VERBOSE | re.IGNORECASE)
        neg_pattern = re.compile(r"""
                            medewerker|studeren|slim|algemene.?voorwaarden|privacy|test|asbestos|website|mailto|CO2|webshopp|app|experience|opstellen|zoek|coronavirus|diensten|nieuwsbrief|ZZP|freelancers|wat.?is|vertalen
                            """, re.VERBOSE | re.IGNORECASE)

        for link in self.soup.find_all("a"):
            if re.search(pattern, str(link.get('href'))) and not re.search(neg_pattern, str(link.get('href'))):
                print(re.search(pattern, str(link.get('href'))))
                if link.get('href').startswith('/'):
                    url = urljoin(self.metadata['website'], link.get('href'))
                else:
                    url = link.get('href')
                pdf_links.add(str(url))

        self.metadata["pdf_links"] = list(pdf_links)

    def extract_metadata(self) -> None:
        """        
        This function is used to extract the metadata from the file, and return it as a dictionary.
        # Example of metadata
        """
        tags = self.soup('meta')
        
        lst = [value for item in tags for key, value in item.attrs.items() if not type(value) == list]

        it = iter(lst)
        metadata = dict(zip(it,it))
        self.metadata.update(metadata)

    def _zipcode_warning(self) -> None:
        """
        If the extractor did find a zipcode, but not an address, there is probably a bug. This notifies the user.=
        """
        if (len(self.metadata["zipcode"]) > 0) and (len(self.metadata["address"]) == 0):
            typer.secho(
                    f'Found zipcodes, but found no address for {self.metadata["website"]}, this is probably a bug',
                    fg=typer.colors.YELLOW,
                )

    def _clean_html(self) -> None:
        text=self.soup.get_text()
        self.text = text

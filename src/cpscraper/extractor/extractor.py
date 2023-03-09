from cpscraper.utils.utils import Worker
import re
import typer
from xmlrpc.client import Boolean
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from datetime import date as datelib
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
from pathlib import Path
import sqlite3 as sql
import os
from multiprocess import Pool
import ndjson
import tqdm
from shutil import rmtree


class Extractor(Worker):
    def __init__(self, target_folder_path, use_sqlite = False, extractor_delete_files = False):
        self.target_folder_path = target_folder_path
        self.use_sqlite = use_sqlite
        self.extractor_delete_files = extractor_delete_files


    def _create_results(self, path):
        [id, domain, level, url, date, path] = path
        #folder = _get_folder(path)

        start_time_file = time.perf_counter()
        #website_name = os.path.basename(Path(folder).parents[0])
        # TODO: integrate get scraper into _get_worker method since now no checks are performed if target folders exist
        metadata = FileExtractor([id, domain, level, url, date, path]).extracting()
        end_time_file = time.perf_counter()

        return ({path: end_time_file - start_time_file }, metadata)


    def extract_companies(self):

        start = time.time()

        #TODO: Link back to config data
        date_start = datelib.today()
        date_end = datelib.today()

        #TODO: TEMPORARY, CHECK IF USE SQL > Reset this to use the config data
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
        
        # chunking in 1M files
        n = 1000000

        # Parallelize loop 
        with Pool() as pool:
            with tqdm.tqdm(total=len(results), leave = True, miniters=1) as pbar:
                # chunk output in files of n lines
                for i in range(0, len(results), n):
                    file_res = self.target_folder_path / 'scraped_data' / ('scraped_data_' + str(datelib.today()) + f'_{i}-{i+n}.ndjson')
                    Path(file_res).parent.mkdir(parents=True, exist_ok=True)
                    file_rep = self.target_folder_path / 'scraped_data' / ('annual_reports_' + str(datelib.today()) + '.ndjson')
                    Path(file_rep).parent.mkdir(parents=True, exist_ok=True)
                    with open(file_res, "w+", encoding='UTF-8') as f_res, open(file_rep, "w+", encoding='UTF-8') as f_rep:
                        writer_res = ndjson.writer(f_res, ensure_ascii=False)
                        writer_rep = ndjson.writer(f_rep, ensure_ascii=False)

                        for result in pool.imap_unordered(self._create_results, results[i:i+n]):
                            time_dict, json_dict = result

                            writer_res.writerow(json_dict)
                            if json_dict["annual_reports"] != []:
                                writer_rep.writerow(json_dict["annual_reports"])

                            pbar.update()

        if self.extractor_delete_files:
            data_folder = os.path.join(self.target_folder_path, "data")
            for folder in os.listdir(data_folder):
                if os.path.isdir(folder):
                    rmtree(os.path.join(data_folder, folder))

        print(f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds.")
    

class FileExtractor:
    def __init__(self, info):
        self.metadata = dict()
        self.metadata["id"], self.metadata["domain"], self.metadata["level"], self.metadata["website"], self.metadata["date"], self.metadata["path"] = info    

    def extracting(self):

        # Phone/emails/fax can be found in the HTML
        with open(self.metadata["path"], "rb", encoding="UTF-8") as file:
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
        Extract and set the address(es) from the input file and add it to `self.metadata`.
        This function searches the text content of the input file for addresses based on the zipcode(s) present in the `self.metadata["zipcode"]` attribute. 
        The extracted addresses are added to the `self.metadata["address"]` attribute as a set.

        Parameters:
            self : object
                An instance of the class containing the `metadata` attribute with the required fields.

        Returns:
            None

        """
        add_found = []
        for zipcode in self.metadata["zipcode"]:
            pattern = r'(((\b[a-zA-ZÀ-ÿ]+)\s+[0-9][0-9-_a-z,/]*)(\s+(?=(' + zipcode + r'))|(?=(' + zipcode + '))))'
            findall = re.findall(pattern, self.text)
            add_found += [item[1] for item in findall]
        
        self.metadata["address"] = add_found

    def extract_zip(self) -> None:
        """
        Extracts zipcodes from the input file, and adds the found zipcodes to self.zipcode in set form.

        Parameters:
            self: object
                The current instance of the class.
            
        Returns:
            None
        
        """
    
        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """, re.VERBOSE)
        zipcodes = list(set(re.findall(pattern, self.text)))
        
        self.metadata["zipcode"] = zipcodes
        
    def extract_btw(self) -> None:
        """
        Extracts the BTW number from the input file, and adds found BTW numbers to self.metadata in list form.
        Sets self.btw as a set containing all the extracted BTW numbers.

        This method uses a regular expression pattern to match BTW number patterns. A BTW number consists of the letters
        'btw' or 'BTW' or 'VAT' or 'vat' followed by any characters, followed by an NL number with optional space(s) and/or
        dot(s), followed by a sequence of digits, dashes, underscores, dots, and/or whitespace, and optionally followed by
        either the letter 'B' or a space, followed by up to 5 digits and/or dots.

        Returns:
            None

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
        Extract the KVK number from the input file, and add found KVK number to self.kvk in set form.

        Parameters:
            None

        Returns:
            None

        """
        
        self.kvk = set()
        
        #Pattern 1
        pattern = re.compile(r"""
                                k\.?v\.?k.{0,12}?(\b\d{8}) | (\b\d{8}).{0,5}?k\.?v\.?k
                                """, re.VERBOSE | re.IGNORECASE)
        result_list = set(re.findall(pattern, self.text))
        for item in result_list:
            for subitem in item:
                if len(subitem) > 0:
                    self.kvk.add(subitem)

        #Pattern 2
        pattern2 = re.compile(r"""
                                (?<=kamer van koophandel).{0,50}(\d{8})
                                """, re.VERBOSE | re.IGNORECASE)
        result_list = set(re.findall(pattern2, self.text))
        for item in result_list:
            self.kvk.add(item)


        self.metadata["kvk"] = list(self.kvk)
        
    def extract_phone(self) -> None:
        """
        Extract the phone number from the input file, and add found phone numbers to self.phone in set form.

        Parameters:
            None

        Returns:
            None

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
        Extract the fax number from the input file, and add found fax numbers to self.fax in set form.

        Parameters:
            None

        Returns:
            None

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
        Exract the email address from the input file, and adds the found email adress to self.email in set form

        Parameters:
            None

        Returns:
            None

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
        Look for and try to scrape the annual report of a website.

        Parameters:
            None

        Returns:
            None

        """
        pdf_links = set()
        pattern = re.compile(r"""
                            financiele.?rapportage|annual.?report|jaarrekening|jaar.?verslag|jaarrapport|boekhouding.?rapportage|boekhouding.?rapport|financial.?performance|investor.?relations|investeerder.?relaties|financial.?results|financial.?statement
                            """, re.VERBOSE | re.IGNORECASE)
        neg_pattern = re.compile(r"""
                            medewerker|studeren|slim|algemene.?voorwaarden|privacy|test|asbestos|website|mailto|CO2|webshopp|app|experience|opstellen|zoek|coronavirus|diensten|nieuwsbrief|ZZP|freelancers|wat.?is|vertalen|wetboek
                            """, re.VERBOSE | re.IGNORECASE)

        for link in self.soup.find_all("a"):
            if re.search(pattern, str(link.get('href'))) and not re.search(neg_pattern, str(link.get('href'))):

                # print(re.search(pattern, str(link.get('href'))))
                if link.get('href').startswith('/'):
                    url = urljoin(self.metadata['website'], link.get('href'))
                else:
                    url = link.get('href')
                pdf_links.add(str(url))

        self.metadata["annual_reports"] = list(pdf_links)

    def extract_metadata(self) -> None:
        """        
        This function is used to extract the metadata from the file, and return it as a dictionary.
        
        """
        tags = self.soup('meta')
        
        lst = [value for item in tags for key, value in item.attrs.items() if not type(value) == list]

        it = iter(lst)
        metadata = dict(zip(it,it))
        self.metadata.update(metadata)

    def _zipcode_warning(self) -> None:
        """
        If the extractor did find a zipcode, but not an address, there is probably a bug. This notifies the user.

        """
        if (len(self.metadata["zipcode"]) > 0) and (len(self.metadata["address"]) == 0):
            typer.secho(
                    f'Found zipcodes, but found no address for {self.metadata["website"]}, this is probably a bug',
                    fg=typer.colors.YELLOW,
                )

    def _clean_html(self) -> None:
        text=self.soup.get_text()
        self.text = text

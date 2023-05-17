import asyncio
import os
import re
import shutil
import sqlite3 as sql
import time
import unicodedata
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date as datelib
from pathlib import Path
from shutil import rmtree
from urllib.parse import urljoin
from xmlrpc.client import Boolean

import ndjson
import tqdm
import typer
from bs4 import BeautifulSoup
from multiprocess import Pool


class FileExtractor:
    """
    A class for extracting data from one specific file.
    This class is used by the extractor pipeline. 
    Custom FileExtractor subclasses can be build, extending the data extracting functionalities.

    Parameters:
        info: tuple
            A tuple containing metadata about the file to extract data from, including the domain, id, level, website, date and path.

    Methods:
        extracting()
            Initiates the extracting of data from a file at the specified file path.
            Calls extracting_default_metadata() and extract_extended_metadata().
        extract_default_metadata()
            Defines methods that include the default extracting functionalities.
        extract_extended_metadata()
            Defines methods that include the extendable extracting functionalities in subclasses.
        
    """
    def __init__(self, info):
        self.metadata = dict()
        (
            self.metadata["domain"],
            self.metadata["id"],
            self.metadata["level"],
            self.metadata["website"],
            self.metadata["date"],
            self.metadata["path"],
        ) = info

        # Filter and include only those methods from self's attributes (dir(self))
        # that are callable (functions) and start with "_extract_", but exclude methods
        # defined in the FileExtractor class (dir(FileExtractor)), meaning that only the custom child methods are included
        self.child_methods = [method for method in dir(self) if callable(getattr(self, method)) and method.startswith('_extract_') and method not in [method for method in dir(FileExtractor) if callable(getattr(FileExtractor, method)) and method.startswith('_extract_')]]
        
        # Read HTML to parse
        with open(self.metadata["path"], "rb") as file:
            self.text = file.read().decode("utf-8", "ignore")
            self.soup = BeautifulSoup(self.text, "lxml")

    def extracting(self):
        # Get metadata
        self.metadata.update(self._extract_metadata()) #future self.metadata |= self._extract_metadata()

        # Clean the HTML to raw text
        self.text = self._clean_html()

        # Call the method which defines which actions should be taken to extract data
        self.extract_default_metadata()
        # Call all methods that are customly created in any child classes of this basic File Extractor class
        self.extract_custom_metadata()

        # Add the raw text to the metadata at last
        self.metadata["text"] = self.text

        
        return self.metadata

    def extract_default_metadata(self):
        # Extract the default data, these are all the modular extract methods

        self.metadata["phone"] = self._extract_phone()
        self.metadata["email"] = self._extract_email()
        self.metadata["fax"] = self._extract_fax()
        self.metadata["zipcode"] = self._extract_zipcode()
        self.metadata["address"] = self._extract_address()

    def extract_custom_metadata(self):
        # Execute all the methods that start with "_extract_" in the name in the child class

        for method in self.child_methods:
            self.metadata[method.split("_extract_")[1]] = getattr(self, method)()


    def _extract_phone(self) -> list:
        """
        Scrape the phone number from the input file, and add found phone numbers to self.phone in set form

        """
        pattern = re.compile(
            r"""
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
                                """,
            re.VERBOSE | re.DOTALL,
        )
        # Phone numbers can be indicated by a variety of different ways, this regex tries to incorporate all of those as a possibillity
        result_list = set(re.findall(pattern, self.text))
        phone = {item[2] for item in result_list}

        return list(phone)

    def _extract_email(self) -> list:
        """
        Scrape the Email adress from the input file, and adds the found email adress to self.email in set form
        """
        pattern = re.compile(
            r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]{1,25}  # again, grab any number or letter
                        \.                  # the dot in the email
                        (?!png)(?!jpg)[a-zA-Z-.]{1,8}     # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""",
            re.VERBOSE,
        )
        self.email = set(re.findall(pattern, self.text))
        emails = [email[:-1] if email[-1] == "." else email for email in self.email]

        return emails

    def _extract_fax(self) -> list:
        """
        Scrape the fax number from the input file, and add found fax numbers to self.fax in set form

        """

        pattern = re.compile(
            r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                ((\+?|\"?)(\d|\s|\(|\)|-){9,22}\d)
                                """,
            re.VERBOSE | re.DOTALL,
        )
        result_list = set(re.findall(pattern, self.text))
        fax = {item[1] for item in result_list}

        return list(fax)

    def _extract_zipcode(self) -> list:
        """
        Scrape the zipcode from the input file, and add found zipcodes to self.zipcode in set form
        """

        pattern = re.compile(
            r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """,
            re.VERBOSE,
        )
        zipcodes = list(set(re.findall(pattern, self.text)))

        return zipcodes


    def _extract_address(self) -> list:
        """
        Scrape the adres from the input file, and add found adres to self.adres in set form(
        TODO: Compile patterns available to all (in utils?). Use re.DOTALL in address

        """
        add_found = []
  
        for zipcode in self.metadata["zipcode"]:
            add, *_ = self.text.partition(zipcode)
            if _[0] == "":
                continue
    
            add = add[-100:].rstrip().rsplit("\n", 2)
            if (len(add[-1]) < 5) and (len(add)>1): #sometimes the postcode is NL-1933XX
                add = add[-2]
            else:
                add = add[-1] 

            pattern = (
                r"\b([ a-zA-ZÀ-ÿ]+\s+[\s0-9-_a-zA-Z]{1,9})" + #address part
                r"[\s\-,\|]{0,5}"
                )
            f = re.findall(pattern, add.strip())
            if len(f) > 0:
                add_found.append(f[-1])

        return add_found

    def _extract_metadata(self) -> dict:
        """
        This function is used to extract the metadata from the file, and return it as a dictionary.

        """
        # keep only the most used tags (>10% of a random subset of pages)
        options = {'og:title', 'article:publisher', 'msapplication-TileImage', 'robots', 'og:description', 'author', 'og:image:type', 
                   'format-detection', 'og:image:width', 'msapplication-TileColor', 'twitter:title', 'og:site_name', 'twitter:label1', 
                   'theme-color', 'og:type', 'twitter:description', 'og:image', 'generator', 'encoding', 'twitter:data1', 'og:url', 
                   'twitter:card', 'viewport', 'article:modified_time', 'keywords', 'description', 'og:locale', 'twitter:image', 
                   'google-site-verification', 'og:image:height'}

        metadata = dict()
        for el in self.soup("meta"):
            el = el.attrs
            encoding = el.get("charset")
            if encoding is not None:
                metadata["meta_encoding"] = encoding

            for type in ["name","property"]:
                nam = el.get(type) 
                if (nam is not None) and (nam in options):
                    cont = el.get("content")
                    if cont is not None:
                        metadata[f"meta_{nam}"] = cont
                        break #continue to next element, it won't have name and property

        return metadata

    def _clean_html(self) -> str:
        text = self.soup.get_text(separator="\n", strip=True)
        return unicodedata.normalize("NFKD", text)


class FirmBackBoneFileExtractor(FileExtractor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _extract_annual_report(self) -> list:
        """
        Look for and try to scrape the annual report of a website, if found add to #TODO where to add?
        """
        pdf_links = set()

        pattern = re.compile(
            r"""
                            financiele.?rapportage|annual.?report|jaarrekening|jaar.?verslag|jaarrapport|jaarrekening|boekhouding.?rapportage|boekhouding.?rapport|financial.?performance|investor|investeerder|financial.?results
                            """,
            re.VERBOSE | re.IGNORECASE,
        )
        neg_pattern = re.compile(
            r"""
                            medewerker|studeren|slim|algemene.?voorwaarden|privacy|test|asbestos|website|mailto|CO2|webshopp|app|experience|opstellen|zoek|coronavirus|diensten|nieuwsbrief|ZZP|freelancers|wat.?is|vertalen
                            """,
            re.VERBOSE | re.IGNORECASE,
        )

        for link in self.soup.find_all("a"):
            if re.search(pattern, str(link.get("href"))) and not re.search(
                neg_pattern, str(link.get("href"))
            ):


                # print(re.search(pattern, str(link.get('href'))))
                if link.get("href").startswith("/"):
                    url = urljoin(self.metadata["website"], link.get("href"))
                else:
                    url = link.get("href")
                pdf_links.add(str(url))

        return list(pdf_links)

    def _extract_kvk(self) -> list:
        """
        Scrape the KVK number from the input file, and add found KVK number to self.kvk in set form

        """
        pattern = re.compile(
                r"""
                k\.?v\.?k.{0,40}?(\b\d{8}) | 
                (\b\d{8}).{0,5}?k\.?v\.?k | 
                (?<=kamer van koophandel).{0,50}(\d{8})
                #k\.?v\.?k.{0,20}?(?:\b|[A-Z]{2})(\d{8}\b)    | 
                #(?:\b|[A-Z]{2})(\d{8}\b).{0,5}?k\.?v\.?k     | 
                #(?<=kamer van koophandel).{0,20}(?:\b|[A-Z]{2})(\d{8}\b)
                """,
                
            re.VERBOSE | re.IGNORECASE | re.DOTALL,
        )
        result_list = re.findall(pattern, self.text)

        kvk = set([subitem for item in result_list for subitem in item if len(subitem) > 0])

        return list(kvk)

    def _extract_btw(self) -> list:
        """
        Scrape the BTW number from the input file, and add found BTW Numbers (usually only 1) to self.btw in list form
        """

        pattern = re.compile(
                        r"""
                        (btw|BTW|VAT|vat)(.+)(\bNL\s*[0-9-_.]{9,12}\s*[B 0-9\.]{0,5}|\bNL\b[0-9-_.B]+)
                        """,
            re.VERBOSE | re.DOTALL,
        )
        result_list = set(re.findall(pattern, self.text))
        btw = {item[2] for item in result_list}

        return list(btw)


class Extractor:
    """
    A class for extracting data from files and storing it in the target folder.

    Parameters:
        target_folder_path: str
            The path to the folder where the extracted data is stored.
        use_sqlite: bool, optional
            Whether or not to use SQLite to store the extracted data. Default is False.
        extractor_delete_files: bool, optional
            Whether or not to delete the original files after extracting data. Default is False.
        file_extractor: FileExtractor, optional
            An custom instance of a FileExtractor class used to extract data from files. Default is None, in which case it will use the default FileExtractor class.

    Methods:
        _create_results(path)
            Extracts the data from one specific file
        extract_companies()
            Start the extracting of all data from the files
    """

    def __init__(
        self, target_folder_path, use_sqlite=True, extractor_delete_files=False, file_extractor: FileExtractor=None
    ):
        self.target_folder_path = target_folder_path
        self.use_sqlite = use_sqlite
        self.extractor_delete_files = extractor_delete_files
        self.file_extractor = file_extractor

    def _create_results(self, path):
        [domain, id, level, url, date, path] = path

        if self.file_extractor != None:
            metadata = self.file_extractor([domain, id, level, url, date, path]).extracting()
        else:
            metadata = FileExtractor([domain, id, level, url, date, path]).extracting()

        return metadata

    
    def extract_companies(self):
        start = time.time()

        # TODO: Link back to config data
        date_start = datelib.today()
        date_end = datelib.today()

        # TODO: TEMPORARY, CHECK IF USE SQL > Reset this to use the config data
        date_start = "2000-01-01"
        date_end = "3000-01-01"
        if self.use_sqlite:
            connection = sql.connect(
                os.path.join(self.target_folder_path, "overview_urls.db")
            )
            cursor = connection.cursor()
            results = cursor.execute(
                f"""SELECT domain, id, level, url, session_date, path FROM Overview 
                            WHERE (session_date >= '{date_start}') 
                            AND (session_date <= '{date_end}') 
                            AND (status == "200")"""
            ).fetchall()
            connection.close()
        else:
            with open(os.path.join(self.target_folder_path, "overview_urls.tsv")) as f:
                f.readline()  # header
                results = []
                for line in f:
                    domain, id, level, url, status, date, _, path = line.split("\t")
                    if (
                        (date >= date_start)
                        and (date <= date_end)
                        and (status == "200")
                    ):
                        results.append([domain, id, level, url, date, path.strip()])
        
        # chunking in 1M files
        n = 1000000

        # Parallelize loop
        with Pool() as pool:
            with tqdm.tqdm(total=len(results), leave=True, miniters=1) as pbar:
                # chunk output in files of n lines
                for i in range(0, len(results), n):
                    file_res = (
                        self.target_folder_path
                        / "scraped_data"
                        / (
                            "scraped_data_"
                            + str(datelib.today())
                            + f"_{i}-{i+n}.ndjson"
                        )
                    )
                    Path(file_res).parent.mkdir(parents=True, exist_ok=True)
                    file_rep = (
                        self.target_folder_path
                        / "scraped_data"
                        / ("annual_report_" + str(datelib.today()) + ".ndjson")
                    )
                    Path(file_rep).parent.mkdir(parents=True, exist_ok=True)
                    with open(file_res, "w+", encoding="UTF-8") as f_res, open(
                        file_rep, "w+", encoding="UTF-8"
                    ) as f_rep:
                        writer_res = ndjson.writer(f_res, ensure_ascii=False)
                        writer_rep = ndjson.writer(f_rep, ensure_ascii=False)


                        for json_dict in pool.imap_unordered(
                            self._create_results, results[i : i + n]
                        ):

                            writer_res.writerow(json_dict)
                            try:
                                if json_dict["annual_report"] != []:
                                    writer_rep.writerow(json_dict["annual_report"])
                            except:
                                pass

                            pbar.update()

        if self.extractor_delete_files:
            # Loop through all subdirectories in the given folder
            for root, dirs, files in os.walk(self.target_folder_path / "data"):
                # Delete all files in the current subdirectory
                for dir in dirs:
                    if re.match(r"\d{4}-\d{2}-\d{2}", dir) and dir >= date_start and dir <= date_end:
                        print(dir)
                        shutil.rmtree(os.path.join(root, dir))

        print(
            f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds."
        )

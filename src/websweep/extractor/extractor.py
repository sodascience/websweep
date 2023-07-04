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

import orjsonl
import zipfile
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
            A tuple containing metadata about the file to extract data from, including the domain, level, website, date and path.

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
            self.metadata["identifier"],
            self.metadata["level"],
            self.metadata["website"],
            self.metadata["date"],
            self.metadata["path"],
        ) = info

        # Filter and include only those methods from self's attributes (dir(self))
        # that are callable (functions) and start with "_extract_", but exclude methods
        # defined in the FileExtractor class (dir(FileExtractor)), meaning that only the custom child methods are included
        self.child_methods = [method for method in dir(self) if callable(getattr(self, method)) and method.startswith('_extract_') and method not in [method for method in dir(FileExtractor) if callable(getattr(FileExtractor, method)) and method.startswith('_extract_')]]

        if isinstance(info[-1], BeautifulSoup):
            self.soup = info[-1]
            self.metadata["path"] = ""
        else:
            # Read HTML to parse
            next_slash = self.metadata["path"].find("/", self.metadata["path"].find("/crawled_data/") + len("/crawled_data/"))
            with zipfile.ZipFile(self.metadata["path"][:next_slash] + ".zip", 'r') as zip_file:
                with zip_file.open(self.metadata["path"][next_slash + 1:]) as file:
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
        Extract the phone number from the input file, and add found phone numbers to self.phone in set form

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
        result_list = set(re.findall(pattern, self.soup.text))
        phone = {item[2] for item in result_list}

        return list(phone)

    def _extract_email(self) -> list:
        """
        Extract the Email adress from the input file, and adds the found email adress to self.email in set form
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
        self.email = set(re.findall(pattern, self.soup.text))
        emails = [email[:-1] if email[-1] == "." else email for email in self.email]

        return emails

    def _extract_fax(self) -> list:
        """
        Extract the fax number from the input file, and add found fax numbers to self.fax in set form

        """

        pattern = re.compile(
            r"""
                                (Fax:\s{0,4}|
                                fax:\s{0,4}|
                                FaxNumber:\s{0,4}|
                                Faxnummer:\s{0,4}|
                                f:\s{0,4}|
                                F:\s{0,4}|)
                                (&nbsp;|/{0,2})?
                                (?<!\S)
                                ((\+?|\"?)(\d|\s|\(|\)|-){9,22}\d)
                                (?!\d)
                                """,
            re.VERBOSE | re.DOTALL,
        )
        result_list = set(re.findall(pattern, self.soup.text))
        faxs = [item[2] for item in result_list]
        return faxs

    def _extract_zipcode(self) -> list:
        """
        Extract the zipcode from the input file, and add found zipcodes to self.zipcode in set form
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
        Extract the adres from the input file, and add found adres to self.adres in set form(
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

            matches = re.findall(pattern, add.strip())
            if len(matches) > 0:
                # Remove unwanted words from matches
                filtered_matches = [re.sub(r"\b(?:gevestigd|aan|te)\b", "", match, flags=re.IGNORECASE) for match in matches]
                filtered_matches = [match.strip() for match in filtered_matches if match.strip()]
                if filtered_matches:
                    add_found.append(filtered_matches[-1])

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
        extract_urls()
            Start the extracting of all data from the files
    """

    def __init__(
        self, target_folder_path, use_sqlite=True, extractor_delete_files=False, start_date="0000-01-01", end_date="9999-01-01", file_extractor: FileExtractor=None
    ):
        self.target_folder_path = target_folder_path
        self.use_sqlite = use_sqlite
        self.extractor_delete_files = extractor_delete_files
        self.file_extractor = file_extractor
        self.start_date = start_date
        self.end_date = end_date

    def _create_results(self, path):
        [domain, identifier, level, url, date, path] = path

        if self.file_extractor != None:
            metadata = self.file_extractor([domain, identifier, level, url, date, path]).extracting()
        else:
            metadata = FileExtractor([domain, identifier, level, url, date, path]).extracting()

        return metadata

    
    def extract_urls(self):
        start = time.time()

        if self.use_sqlite:
            connection = sql.connect(
                os.path.join(self.target_folder_path, "overview_urls.db")
            )
            cursor = connection.cursor()
            results = cursor.execute(
                f"""SELECT domain, identifier, level, url, session_date, path FROM Overview 
                            WHERE (session_date >= '{self.start_date}') 
                            AND (session_date <= '{self.end_date}') 
                            AND (status == "200")"""
            ).fetchall()
            connection.close()
        else:
            with open(os.path.join(self.target_folder_path, "overview_urls.tsv")) as f:
                f.readline()  # header
                results = []
                for line in f:
                    domain, identifier, level, url, status, date, _, path = line.split("\t")
                    if (
                        (date >= self.start_date)
                        and (date <= self.end_date)
                        and (status == "200")
                    ):
                        results.append([domain, identifier, level, url, date, path.strip()])

        # results_folder = (
        #         self.target_folder_path
        #         / "extracted_data"
        #         / ("extracted_data")
        #     )
        # Path(results_folder).mkdir(parents=True, exist_ok=True)
        # reports_folder = (
        #         self.target_folder_path
        #         / "extracted_data"
        #         / ("annual_reports")
        #     )
        # Path(reports_folder).mkdir(parents=True, exist_ok=True)
        
        # chunking in 1M files
        n = 1000000

        # Parallelize loop
        with Pool() as pool:
            with tqdm.tqdm(total=len(results), leave=True, miniters=1) as pbar:
                # chunk output in files of n lines
                for i in range(0, len(results), n):
                    file_res = (
                        self.target_folder_path
                        / "extracted_data"
                        / (
                            "extracted_data_"
                            + str(datelib.today())
                            + f"_{i}-{i+n}.ndjson.zip"
                        )
                    )
                    Path(file_res).parent.mkdir(parents=True, exist_ok=True)

                    file_rep = (
                        self.target_folder_path
                        / "extracted_data"
                        / ("annual_report_" 
                           + str(datelib.today()) 
                           + ".ndjson.zip")
                    )
                    Path(file_rep).parent.mkdir(parents=True, exist_ok=True)

                    for json_dict in pool.imap_unordered(self._create_results, results[i : i + n]):
                        orjsonl.append(file_res, [json_dict], compression_level = 9, compression_format = "gz")
                        
                        try:
                            if json_dict["annual_report"] != []:
                                orjsonl.append(file_rep, [json_dict["annual_report"]], compression_level = 9, compression_format = "gz")
                        except:
                            pass

                        pbar.update()
                        

        if self.extractor_delete_files:
            # Loop through all subdirectories in the given folder
            for root, dirs, files in os.walk(self.target_folder_path / "crawled_data"):
                # Delete all files in the current subdirectory
                for dir in dirs:
                    if re.match(r"\d{4}-\d{2}-\d{2}", dir) and dir >= self.start_date and dir <= self.end_date:

                        shutil.rmtree(os.path.join(root, dir))

        print(
            f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds."
        )

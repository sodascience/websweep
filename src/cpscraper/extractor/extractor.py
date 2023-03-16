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
import unicodedata


class Extractor(Worker):
    def __init__(
        self, target_folder_path, use_sqlite=False, extractor_delete_files=False
    ):
        self.target_folder_path = target_folder_path
        self.use_sqlite = use_sqlite
        self.extractor_delete_files = extractor_delete_files

    def _create_results(self, path):
        [id, domain, level, url, date, path] = path
        # folder = _get_folder(path)

        start_time_file = time.perf_counter()
        # website_name = os.path.basename(Path(folder).parents[0])
        # TODO: integrate get scraper into _get_worker method since now no checks are performed if target folders exist
        metadata = FileExtractor([id, domain, level, url, date, path]).extracting()
        end_time_file = time.perf_counter()

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
                f"""SELECT id, domain, level, url, date, path FROM Overview 
                            WHERE (date >= '{date_start}') 
                            AND (date <= '{date_end}') 
                            AND (status == "200")"""
            ).fetchall()
            connection.close()
        else:
            with open(os.path.join(self.target_folder_path, "overview_urls.tsv")) as f:
                f.readline()  # header
                results = []
                for line in f:
                    id, domain, level, url, status, date, path = line.split("\t")
                    if (
                        (date >= date_start)
                        and (date <= date_end)
                        and (status == "200")
                    ):
                        results.append([id, domain, level, url, date, path.strip()])
        
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
                        / ("annual_reports_" + str(datelib.today()) + ".ndjson")
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
                            if json_dict["annual_reports"] != []:
                                writer_rep.writerow(json_dict["annual_reports"])

                            pbar.update()

        if self.extractor_delete_files:
            data_folder = os.path.join(self.target_folder_path, "data")
            for folder in os.listdir(data_folder):
                if os.path.isdir(folder):
                    rmtree(os.path.join(data_folder, folder))

        print(
            f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds."
        )

#TODO allow users to change FileExtractor with their own function (base = scraper + some info; modules to get different info (e.g. doewnload corporates))
class FileExtractor:
    def __init__(self, info):
        self.metadata = dict()
        (
            self.metadata["id"],
            self.metadata["domain"],
            self.metadata["level"],
            self.metadata["website"],
            self.metadata["date"],
            self.metadata["path"],
        ) = info
        
        # Read HTML to parse
        with open(self.metadata["path"], "rb") as file:
            self.text = file.read().decode("utf-8", "ignore")
            self.soup = BeautifulSoup(self.text, "lxml")

    def extracting(self):
        # Get metadata
        self.extract_metadata()
        self._clean_html()

        # Extract the data
        self.extract_annual_report()
        self.extract_kvk()
        self.extract_btw()
        self.extract_phone()
        self.extract_email()
        self.extract_fax()
        
        self.metadata["text"] = self.text
        self.extract_zip()
        self.extract_address()

        return self.metadata

    def extract_address(self) -> None:
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

        self.metadata["address"] = add_found

    def extract_zip(self) -> None:
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

        self.metadata["zipcode"] = zipcodes

    def extract_btw(self) -> None:
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
        self.metadata["btw"] = list(btw)

    def extract_kvk(self) -> None:
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
        self.metadata["kvk"] = list(kvk)

    def extract_phone(self) -> None:
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
        self.metadata["phone"] = list(phone)

    def extract_fax(self) -> None:
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
        self.metadata["fax"] = list(fax)

    def extract_email(self) -> None:
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
        self.metadata["email"] = emails

    def extract_annual_report(self) -> None:
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

        self.metadata["annual_reports"] = list(pdf_links)

    def extract_metadata(self) -> None:
        """
        This function is used to extract the metadata from the file, and return it as a dictionary.
        # Example of metadata
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

        self.metadata.update(metadata)

    def _clean_html(self) -> None:
        text = self.soup.get_text(separator="\n", strip=True)
        self.text = unicodedata.normalize("NFKD", text)

    # # Helper for extracting
    # def _create_results(path):
    #     [id, domain, level, url, date, path] = path

    #     start_time_file = time.perf_counter()
    #     #website_name = os.path.basename(Path(folder).parents[0])
    #     # TODO: integrate get scraper into _get_scraper method since now no checks are performed if target folders exist
    #     cached_corporate = Extractor([id, domain, level, url, date, path])
    #     metadata = cached_corporate.extracting()
    #     end_time_file = time.perf_counter()

    #     return ({path: end_time_file - start_time_file }, metadata)

    # async def __extract_all(self, target_folder_path, results):
    #     """
    #     Fetch all urls in records up to a level max_level. Save html to file.

    #     :param records: List of all level 0 urls to visit
    #     """

    #     file_res = target_folder_path  /  ('scraped_data_' + str(datelib.today()) + '.ndjson')
    #     pdf_file = target_folder_path  /  ('pdf_links_' + str(datelib.today()) + '.ndjson')
    #     pdf_list = []

    #     Path(file_res).parent.mkdir(parents=True, exist_ok=True)

    #     # Parallelize loop
    #     with Pool() as pool, open(file_res, "w+", encoding='UTF-8') as f_res:
    #         writer_res = ndjson.writer(f_res, ensure_ascii=False)

    #         with tqdm.tqdm(total=len(results), leave = True, miniters=1) as pbar:
    #             for result in pool.imap_unordered(_create_results, results):
    #                 time_dict, json_dict = result
    #                 writer_res.writerow(json_dict)
    #                 pbar.update()
    #                 if json_dict["pdf_links"] != []:
    #                     print(json_dict['pdf_links'])
    #                     pdf_list.append(json_dict["pdf_links"])

    #     #TODO: change that pdf files are saved in same way as the JSON files in the extractor
    #     with open(pdf_file, "w+", encoding='UTF-8') as pdf_res:
    #         pdf_writer = ndjson.writer(pdf_res, ensure_ascii=False)
    #         pdf_writer.writerow(pdf_list)

    #     # tasks = []

    #     # # for each url, create asynchronous task to fetch company and append to tasks list
    #     # for result in results:
    #     #     task = asyncio.ensure_future(self.__extract_one_company(result))
    #     #     tasks.append(task)
    #     # # create future and group tasks

    #     # progress = [
    #     #     await f
    #     #     for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), leave = True, miniters=1)
    #     # ]

    #     # return progress

    # def extract_companies(self, target_folder_path, delete_raw_files):
    #     """
    #     Create initial asynchronous task to fetch all urls

    #     :param urls: List of all level 0 urls to visit
    #     """

    #     start = time()

    #     #TODO: TEMPORARY, CHECK IF USE SQL > Reset this to use the config data
    #     use_sqlite = False # this needs to be a parameter
    #     date_start = "2000-01-01"
    #     date_end = "3000-01-01"
    #     if use_sqlite:
    #         connection = sql.connect(os.path.join(target_folder_path,  "overview_urls.db"))
    #         cursor = connection.cursor()
    #         results = cursor.execute(f'''SELECT id, domain, level, url, date, path FROM Overview
    #                         WHERE (date >= '{date_start}')
    #                         AND (date <= '{date_end}')
    #                         AND (status == "200")''').fetchall()
    #         connection.close()
    #     else:
    #         with open(os.path.join(target_folder_path, "overview_urls.tsv")) as f:
    #             f.readline() #header
    #             results = []
    #             for line in f:
    #                 id, domain, level, url, status, date, path = line.split("\t")
    #                 if (date >= date_start) and (date <= date_end) and (status == "200"):
    #                     results.append([id, domain, level, url, date, path.strip()])

    #     with ThreadPoolExecutor(max_workers=self.threads_bs4) as self.cpu_executor, ThreadPoolExecutor(max_workers=1) as self.io_executor:
    #         self.loop = asyncio.get_event_loop()
    #         future = asyncio.ensure_future(self.__extract_all(target_folder_path, results))
    #         self.loop.run_until_complete(future)

    #     if delete_raw_files:
    #         data_folder = os.path.join(target_folder_path, "data")
    #         for folder in os.listdir(data_folder):
    #             if os.path.isdir(folder):
    #                 rmtree(os.path.join(data_folder, folder))

    #     print(f"Extracted data from {len(results)} pages in {time.time() - start:2.1f} seconds.")

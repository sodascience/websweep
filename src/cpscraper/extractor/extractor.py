import re
import typer
from xmlrpc.client import Boolean
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class Extractor:
    def __init__(self, info):
        self.metadata = dict()
        self.metadata["id"], self.metadata["domain"], self.metadata["level"], self.metadata["website"], self.metadata["date"], self.metadata["path"] = info    

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

    def extract_metadata(self, file_path) -> None:
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

import os
import re
import typer


class Extractor:
    def __init__(self, working_dir=None, website=None, phone=None, email=None, kvk=None,
                 adres=None, zip_code=None, fax=None, btw=None):
        self.website = website
        self.email = email
        self.phone = phone
        self.kvk = kvk
        self.fax = fax
        self.btw = btw
        self.zip_code = zip_code
        self.adres = adres
        self.working_dir = working_dir


    def run_loops(self):
        for file in os.scandir(self.working_dir):
            open_file = open(file, "r", encoding="UTF-8")
            file_text = open_file.read()
            self.scrape_adres(file_text)
            self.scrape_zip(file_text)
            self.scrape_phone(file_text)
            self.scrape_email(file_text)
            self.scrape_fax(file_text)
            self.scrape_kvk(file_text)
            self.scrape_btw(file_text)
            open_file.close()

        self.clean_email()
        self.jsonize()
        if self.mistake_warning():
            print(f"finished {self.website}")
        else:
            typer.secho(
                    f'No values could be found for {self.website}',
                    fg=typer.colors.RED,
                )
        

    def scrape_adres(self, file):
        """
        Scrape the adres from the input file, and add found adres to self.adres in set form
        """
        if self.adres is None:
            self.adres = set()
        

    def scrape_zip(self, file):
        """
        Scrape the zipcode from the input file, and add found zipcodes to self.zip_code in set form
        """
        if self.zip_code is None:
            self.zip_code = set()
        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)(?!px)(?!em)(?!rm)[A-Z]{2}\b)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.zip_code.add(item)

    def scrape_btw(self, file):
        """
        Scrape the BTW number from the input file, and add found BTW Numbers (usually only 1) to self.btw in list form
        """
        if self.btw is None:
            self.btw = set()
        pattern = re.compile(r"""
                                (btw|BTW|VAT|vat)(.+)(\bNL\s*[0-9-_.]{9,12}\s*[B 0-9\.]{0,4}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.btw.add(item[2])
        

    def scrape_kvk(self, file):
        """
        Scrape the KVK number from the input file, and add found KVK number to self.kvk in set form
        """
        if self.kvk is None:
            self.kvk = set()
        pattern = re.compile(r"""
                                (kvk|KvK|KVK|K.v.K.|K.V.K|k.v.k.)(.+)(\b\d{8})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            temp = item[0]+' '+item[2]
            self.kvk.add(temp)
        # Note that there are 2 patterns, one for [kvk] [number], and one for [number] [kvk]
        pattern2 = re.compile(r"""
                                (\b\d{8})(.{0,4})(kvk|KvK|K.v.K.|k.v.k.|KVK)
                                """, re.VERBOSE)
        result_list2 = set(re.findall(pattern2, file))
        for item in result_list2:
            temp = item[0]+' '+item[2]
            self.kvk.add(temp)
        

    def scrape_phone(self, file):
        """
        Scrape the phone number from the input file, and add found phone numbers to self.phone in set form
        """
        if self.phone is None:
            self.phone = set()
        pattern = re.compile(r"""
                                (Tel:\s{0,4}|
                                tel:\s{0,4}|
                                telefoon:\s{0,4}|
                                Telefoon:\s{0,4}|
                                T:\s{0,4}|
                                t:\s{0,4}|
                                T\s{0,4})
                                (&nbsp;|/{0,2})?
                                ((\+?|\"?)(\d|\s|\(|\)|-){10,22})
                                """, re.VERBOSE)
                                # Phone numbers can be indicated by a variety of different ways, this regex tries to incorporate all of those as a possibillity
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            temp = item[0] + ' ' + item[2]
            self.phone.add(temp)
        

    def scrape_fax(self, file):
        """
        Scrape the fax number from the input file, and add found fax numbers to self.fax in set form
        """
        if self.fax is None:
            self.fax = set()
        pattern = re.compile(r"""
                                (Fax:\s|
                                fax:\s|
                                F:\s|
                                f:\s)
                                (\b(\d-*){10,})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.fax.add(item[1])
        

    def scrape_email(self, file):
        """
        Scrape the Email adress from the input file, and adds the found email adress to self.email in set form
        """
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]{1,20}  # again, grab any number or letter
                        \.                  # the dot in the email
                        (?!png)(?!jpg)[a-zA-Z-.]{1,8}     # grab any combination of letters/numbers/dots, to ensure we also grab stuff like .co.uk
                        )""", re.VERBOSE)
        emails = set(re.findall(pattern, file))
        if self.email is None:
            self.email = emails
        else:
            self.email.update(emails)

    def clean_email(self):
        """
        Cleans the self.emails list, as Emails occasionally have an extra '.' at the end of the email adress.
        """
        temp_emails = set()
        for email in self.email:
            if email[-1] == ".":
                temp_emails.add(email[:-1])
            else:
                temp_emails.add(email)
        self.email = temp_emails

    def jsonize(self):
        self.email = list(self.email)
        self.phone = list(self.phone)
        self.kvk = list(self.kvk)
        self.fax = list(self.fax)
        self.btw = list(self.btw)
        self.zip_code = list(self.zip_code)
        self.adres = list(self.adres)

    def mistake_warning(self):
        if not self.__dict__['email'] and not self.__dict__['kvk'] and not self.__dict__['phone'] and not self.__dict__['btw'] and not self.__dict__['fax'] and not self.__dict__['zip_code']:
            return False
        else:
            return True

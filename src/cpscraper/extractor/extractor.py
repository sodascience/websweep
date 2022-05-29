
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
            self.scrape_address(file_text)
            self.scrape_zip(file_text)
            self.scrape_phone(file_text)
            self.scrape_email(file_text)
            self.scrape_fax(file_text)
            self.scrape_kvk(file_text)
            self.scrape_btw(file_text)
            open_file.close()

        self.clean_email()
        self.jsonize()
        print(f"finished {self.website}")

    def scrape_address(self, file):
        if self.adres is None:
            self.adres = set()
        return

    def scrape_zip(self, file):
        if self.zip_code is None:
            self.zip_code = set()
        pattern = re.compile(r"""
                                (\b\d{4}\s?(?!SS)(?!SD)(?!SA)[A-Z]{2}\b)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.zip_code.add(item)
        return

    def scrape_btw(self, file):
        if self.btw is None:
            self.btw = set()
        pattern = re.compile(r"""
                                (btw|BTW)(.+)(\bNL[0-9-_.]{9,12}B\d{2}|\bNL\b[0-9-_.B]+)
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            self.btw.add(item[2])
        return

    def scrape_kvk(self, file):
        if self.kvk is None:
            self.kvk = set()
        pattern = re.compile(r"""
                                (kvk|KvK|K.v.K.|k.v.k.)(.+)(\b\d{8})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            temp = item[0]+' '+item[2]
            self.kvk.add(temp)

        pattern2 = re.compile(r"""
                                (\b\d{8})(.{0,4})(kvk|KvK|K.v.K.|k.v.k.)
                                """, re.VERBOSE)
        result_list2 = set(re.findall(pattern2, file))
        for item in result_list2:
            temp = item[0]+' '+item[2]
            self.kvk.add(temp)
        return

    def scrape_phone(self, file):
        if self.phone is None:
            self.phone = set()
        pattern = re.compile(r"""
                                (Tel:\s*|
                                tel:\s*|
                                telefoon:\s*|
                                Telefoon:\s*|
                                T:\s*|
                                t:\s:
                                T\s)
                                (\+?(\s?\d-*){10,11})
                                """, re.VERBOSE)
        result_list = set(re.findall(pattern, file))
        for item in result_list:
            temp = item[0] + ' ' + item[1]
            self.phone.add(temp)
        return

    def scrape_fax(self, file):
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
        return

    def scrape_email(self, file):
        pattern = re.compile(r"""
                        ([a-zA-Z0-9_.+-]+   # one (or more) sets of all characters which numbers, letters or a subset of punctuations
                        @                   # needs a @    
                        [a-zA-Z0-9-]{1,20}   # again, grab any number or letter
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

    def jsonize(self):
        self.email = list(self.email)
        self.phone = list(self.phone)
        self.kvk = list(self.kvk)
        self.fax = list(self.fax)
        self.btw = list(self.btw)
        self.zip_code = list(self.zip_code)
        self.adres = list(self.adres)

    def mistake_warning(self):
        for key, value in self.__dict__.items():
            if not value:
                typer.secho(
                    'No value could be found for {key} at {self.working_dir}',
                    fg=typer.colors.RED,
                )

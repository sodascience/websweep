import re
from urllib.parse import urljoin
from .extractor import FileExtractor

class FirmBackBoneFileExtractor(FileExtractor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _extract_annual_report(self) -> list:
        """
        Look for and try to extract the annual report of a website, if found add to #TODO where to add?
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
        Extract the KVK number from the input file, and add found KVK number to self.kvk in set form

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
        Extract the BTW number from the input file, and add found BTW Numbers (usually only 1) to self.btw in list form
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
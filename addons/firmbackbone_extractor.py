"""Optional FIRMBACKBONE extraction add-on.

This module is intentionally kept outside ``src/websweep`` so it is not part
of the core install. Import and use it explicitly when you need these fields.
"""

from urllib.parse import urljoin

try:
    import re2 as re
except Exception:
    import regex as re

from websweep.extractor.extractor import FileExtractor


class FirmBackBoneFileExtractor(FileExtractor):
    """Add-on extractor with FIRMBACKBONE-specific fields."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _extract_phone(self) -> list:
        pattern = re.compile(
            r'(?s)(?i)(?:tel:|telefoon:|t:|t\s|t\.)[\s|&nbsp;|\/]{0,4}'
            r'([\+|\"]?[\d|\s|\(|\)|-]{9,15})\b'
        )
        return list(set(re.findall(pattern, str(self.soup))))

    def _extract_email(self) -> list:
        undesired_extensions = {
            "png",
            "jpg",
            "jpeg",
            "gif",
            "pdf",
            "doc",
            "docx",
            "xls",
            "xlsx",
            "txt",
            "rtf",
            "zip",
            "mp3",
            "mp4",
            "wav",
            "avi",
            "mov",
            "psd",
            "tif",
            "tiff",
        }
        pattern = re.compile(
            r"([A-Za-z0-9\.\+_-]+@[a-zA-Z0-9-_\.]{1,25}\.[a-zA-Z-]{1,7})"
        )
        potential_emails = pattern.findall(str(self.soup))
        return [
            email
            for email in set(potential_emails)
            if email.split(".")[-1].lower() not in undesired_extensions
        ]

    def _extract_fax(self) -> list:
        pattern = re.compile(
            r"(?is)\b(?:faxnumber|fax|f)\b[^0-9\+]{0,12}"
            r"([\+]?[0-9][0-9\-\s\(\)]{7,20})\b"
        )
        faxs = set(re.findall(pattern, str(self.soup)))
        return list(set([_.strip() for _ in faxs]))

    def _extract_annual_report(self) -> list:
        """Extract annual report links if present."""
        pdf_links = set()

        pattern = re.compile(
            r"(?i)financiele.?rapportage|annual.?report|jaarrekening|jaar.?verslag|"
            r"jaarrapport|jaarrekening|boekhouding.?rapportage|boekhouding.?rapport|"
            r"financial.?performance|investor|investeerder|financial.?results"
        )
        neg_pattern = re.compile(
            r"(?i)medewerker|studeren|slim|algemene.?voorwaarden|privacy|test|asbestos|"
            r"website|mailto|CO2|webshopp|app|experience|opstellen|zoek|coronavirus|"
            r"diensten|nieuwsbrief|ZZP|freelancers|wat.?is|vertalen"
        )

        for link in self.soup.find_all("a"):
            href = str(link.get("href"))
            if re.search(pattern, href) and not re.search(neg_pattern, href):
                if href.startswith("/"):
                    url = urljoin(self.metadata["website"], href)
                else:
                    url = href
                pdf_links.add(str(url))

        return list(pdf_links)

    def _extract_kvk(self) -> list:
        pattern = re.compile(
            r"(?s)(?i)k\.?v\.?k.{0,40}?(\b\d{8}\b)|kamer van koophandel.{0,50}?(\b\d{8}\b)"
        )
        result_list = re.findall(pattern, self.text)
        kvk = {subitem for item in result_list for subitem in item if subitem}
        return list(kvk)

    def _extract_btw(self) -> list:
        pattern = re.compile(
            r"(?s)(?i)(btw|BTW|VAT|vat)(.{1,20})(\bNL\s*[0-9-_.]{9,12}\s*[B 0-9\.]{0,5}|\bNL\b[0-9-_.B]+)"
        )
        return list({item[2] for item in re.findall(pattern, self.text)})

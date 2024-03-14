import dataclasses
from collections import Counter
from typing import List, Generator, Dict, Any
import orjson
import re2 as re
from itertools import islice
from pathlib import Path
from tqdm import tqdm
from gevent import monkey, Timeout
monkey.patch_all(thread=False)

# Constants
COLUMNS_KEEP = [
    "domain", "identifier", "level", "website", "date", "path",
    "phone", "email", "fax", "zipcode", "address", "text"
]
CONTACT = ["phone", "email", "fax", "zipcode", "address"]

@dataclasses.dataclass
class Domain:
    """
    A data class representing a domain with various attributes.

    Attributes:
        domain (str): The domain name.
        identifier (str): The identifier of the domain.
        phone (Counter): A counter for phone numbers.
        email (Counter): A counter for email addresses.
        fax (Counter): A counter for fax numbers.
        zipcode (Counter): A counter for zip codes.
        address (Counter): A counter for addresses.
        kvk (Counter): A counter for KVK numbers.
        btw (Counter): A counter for BTW numbers.
        text (str): The text associated with the domain.
    """
    domain: str
    identifier: str
    phone: Counter
    email: Counter
    fax: Counter
    zipcode: Counter
    address: Counter
    kvk: Counter
    btw: Counter
    text: str

    def __add__(self, other):
        """
        Adds two Domain objects together, combining their attributes.

        Args:
            other (Domain): Another Domain object to add.

        Returns:
            Domain: A new Domain object with combined attributes.

        Raises:
            ValueError: If the domains of the two objects are different.
        """
        if self.domain != other.domain:
            raise ValueError("Cannot add domains with different names")
        return Domain(
            domain=self.domain,
            identifier=self.identifier,
            phone=self.phone + other.phone,
            email=self.email + other.email,
            fax=self.fax + other.fax,
            zipcode=self.zipcode + other.zipcode,
            address=self.address + other.address,
            kvk=self.kvk + other.kvk,
            btw=self.btw + other.btw,
            text=self.text + " " + other.text,
        )

    def __post_init__(self):
        """
        Initializes counters for the Domain object.
        """
        self.phone = Counter(self.phone)
        self.email = Counter(self.email)
        self.fax = Counter(self.fax)
        self.zipcode = Counter(self.zipcode)
        self.address = Counter(self.address)
        self.kvk = Counter(self.kvk)
        self.btw = Counter(self.btw)

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Domain object into a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the Domain object.
        """
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        """
        Creates a Domain object from a dictionary.

        Args:
            d (Dict[str, Any]): A dictionary containing Domain attributes.

        Returns:
            Domain: A new Domain object created from the dictionary.
        """
        return cls(**d)

class Consolidator:
    """
    A class to process domain information from ndjson files.

    Methods:
        read_ndjson_in_chunks: Reads ndjson file in chunks.
        create_domain_info: Creates domain information from sorted chunks.
        merge_sorted_files: Merges sorted files line by line.
    """
    
    def __init__(self, input_file: str, chunk_size: int = 10000):
        """
        Initializes the DomainProcessor with an input file path and chunk size.

        Args:
            input_file (str): The path to the input ndjson file.
            chunk_size (int, optional): The size of each chunk to be read from the file. Default is 10000.
        """
        self.input_file = input_file
        self.chunk_size = chunk_size

    
    def save_orjson_loads(self, line: str) -> Dict[str, Any]:
        """
        Loads a line from an ndjson file using orjson.

        Args:
            line (str): A line from an ndjson file.

        Returns:
            Dict[str, Any]: A dictionary representing the line.
        """
        try:
            return orjson.loads(line)
        except:
            return None

    def read_ndjson_in_chunks(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Reads an ndjson file in chunks.

        Yields:
            Generator[List[Dict[str, Any]], None, None]: A generator that yields lists of dictionaries, each representing a line in the ndjson file.
        """
        with open(self.input_file, 'rb') as f:
            while True:
                lines_gen = list(islice(f, self.chunk_size))
                if not lines_gen:
                    break
                jsons = [self.save_orjson_loads(line) for line in lines_gen]
                yield [_ for _ in jsons if _ is not None]
    

    def create_domain_info(self, chunk: List[Dict[str, Any]], output_file: str):
        """
        Creates domain information from sorted chunks and writes to an output file with a maximum of 1 second per domain processing using gevent.

        Args:
            chunk (List[Dict[str, Any]]): A list of dictionaries, each representing a domain.
            output_file (str): The path to the output file where the domain information will be written.
        """
        domain = None
        domain_info = None

        with open(output_file, "wb") as f:
            for site in tqdm(sorted(chunk, key=lambda d: self._clean_domain(d["domain"]))):
                new_domain = self._clean_domain(site["domain"])
                
                if domain is None or new_domain != domain:
                    if domain_info is not None:
                        self._dump_domain_to_file(domain_info, f)

                    domain = new_domain
                    domain_info = self._initialize_domain_counters(site)

                self._update_domain_counters(domain_info, site)

                # except Exception as e:
                #     # Handle the timeout exception, perhaps by logging it
                #     print(f"Processing of {site['domain']} timed out.")

            if domain_info is not None:
                self._dump_domain_to_file(domain_info, f)

    def merge_domain_files(self, input_files: List[str], final_output: str):
        """
        Merges multiple domain files into a single file.

        Args:
            input_files (List[str]): A list of file paths to be merged.
            final_output (str): Path to the final output file.
        """
        files = [open(file, "rb") for file in input_files]
        domains_objects = [Domain.from_dict(orjson.loads(file.readline())) for file in files]
        domains = [domain.domain for domain in domains_objects]
        smallest_value = "__first_domain"
        d = None

        with open(final_output, "wb") as f:
            while domains:
                new_smallest_value = min(domains)
                indexes = [i for i, x in enumerate(domains) if x == new_smallest_value]

                if new_smallest_value != smallest_value:
                    if smallest_value != "__first_domain":
                        f.write(orjson.dumps(d) + b"\n")
                        
                    smallest_value = new_smallest_value
                    
                    smallest_index = indexes[0]
                    
                    d = domains_objects[smallest_index]
                    if len(indexes) > 1:
                        for i in indexes[1:]:
                            d += domains_objects[i]

                else:
                    for i in indexes:
                        d += domains_objects[i]

                # Read new lines for those indexes and update domains and domains_objects
                for i in indexes[::-1]:
                    line = files[i].readline()
                    if line:
                        domains_objects[i] = Domain.from_dict(orjson.loads(line))
                        domains[i] = domains_objects[i].domain
                    else:
                        domains.pop(i)
                        domains_objects.pop(i)
                        files[i].close()
                        files.pop(i)
                                
            f.write(orjson.dumps(d) + b"\n")

        # # Delete temporary files
        for file in input_files:
            Path(file).unlink()


    def consolidate(self, final_output: str):
        # Read NDJSON in chunks and organize per domain
        for i, chunk in enumerate(self.read_ndjson_in_chunks()):
            self.create_domain_info(chunk, f"temp_chunk{i}.ndjson")

        # Merge sorted files
        self.merge_domain_files([f"temp_chunk{_}.ndjson" for _ in range(i+1)], final_output=final_output)


    def _clean_domain(self, domain: str) -> str:
        """
        Cleans a domain name.

        Args:
            domain (str): The domain name to clean.

        Returns:
            str: The cleaned domain name.
        """
        return ".".join(re.split("\.|/|@", domain)[-2:])
    
    def _initialize_domain_counters(self, site: Dict[str, Any]) -> Domain:
        """
        Initializes counters for a new domain.

        Args:
            site (Dict[str, Any]): A dictionary representing a site.

        Returns:
            Domain: A new Domain object with initialized counters.
        """
        
        return Domain(
            domain=self._clean_domain(site["domain"]), #TODO: Make sure it works for domains such as .co.uk
            identifier=site["identifier"],
            phone=Counter(),
            email=Counter(),
            fax=Counter(),
            zipcode=Counter(),
            address=Counter(),
            kvk=Counter(),
            btw=Counter(),
            text=""
        )

    def _update_domain_counters(self, domain: Domain, site: Dict[str, Any]):
        """
        Updates the counters of a domain with information from a site.

        Args:
            domain (Domain): The Domain object to update.
            site (Dict[str, Any]): A dictionary representing a site.
        """
        domain.phone.update(site["phone"])
        domain.email.update(site["email"])
        domain.fax.update(site["fax"])
        domain.zipcode.update(site["zipcode"])
        domain.address.update(site["address"])
        domain.address.update(site["btw"])
        domain.address.update(site["kvk"])
        
        domain.text += " " + site["text"]

    def _dump_domain_to_file(self, domain: Domain, file_object):
        """
        Writes a domain's information to a file.

        Args:
            domain (Domain): The Domain object to write to the file.
            file_object: The file object to write to.
        """
        file_object.write(orjson.dumps(domain) + b"\n")


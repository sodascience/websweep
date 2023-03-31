# FastScraper


FastScraper is a web scraping Python package, focused on simplicity, modularity and speed. Given a list of domains, FastScraper will crawl the domains and extract relevant information. 
 


## Installation

Use the package manager [pip]() to install cpscraper.

```bash
$ pip install cpscraper
```

The package can also be used as a script without installation.
When using windows, remember to use cd \ followed by the drive letter to change drives

```bash
cd ~/home/[wherever the folder is located]/corporate_scraper/src
$ python(3) -m cpscraper init
```

You can also install the library from source. You need to have poetry installed (``pip install poetry``), do remember to either restart your pc or add poetry to your PATH variables. If you get a warning that dependencies are not up to date, run ``poetry update`` first.

```bash
cd to the cpscraper folder
$ poetry install
$ poetry build
$ pip install [PATH].whl
```

## Usage 

### As a library
```python
example here
```

### Command line interface
```bash
# see all commands
$ cpscraper --help

# initialise the scraper (scraper will not run without configuration)
$ cpscraper init [--headless]

# after initialization, the scraper can both scrape websites, and extract information from files
$ cpscraper scrape
$ cpscraper extract
```

## Testing

The system includes a variety of buildin test cases to test the extracter.

```
# cd to the tests folder
$ cd ~home/[wherever the folder is located]/corporatescraper/tests

# run the unittesting
$ python3 -m unittest test.py
```
The output should look something like this:
```
.......
----------------------------------------------------------------------
Ran 7 tests in 0.006s

OK

```


## License

[MIT](https://choosealicense.com/licenses/mit/)


## Contributing

Contributions are what make the open source community an amazing place
to learn, inspire, and create. Any contributions you make are **greatly
appreciated**.

Please refer to the
[CONTRIBUTING](https://github.com/sodascience/corporate_scraper/blob/main/CONTRIBUTING.md)
file for more information on issues and pull requests.

## License and citation

The package `cpscraper` is published under an MIT license. When using `cpscraper` for academic work, please cite:

    XXX


## Contact

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-data.nl/nl/soda/) team and the [FIRMBACKBONE](https://firmbackbone.nl/) Project.

<img src="soda_logo.png" alt="SoDa logo" width="250px"/>


FIRMBACKBONE is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research. Once it is ready, it will become available for researchers and students affiliated with member universities in the Netherlands through [ODISSEI](https://odissei-data.nl/nl/), the Open Data Infrastructure for Social Science and Economic Innovations.

FIRMBACKBONE is an initiative of Utrecht University and the Vrije Universiteit Amsterdam funded by [PDI-SSH](https://pdi-ssh.nl/nl/home/), the Platform Digital Infrastructure-Social Sciences and Humanities, for the period 2020-2025.


Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact the team via
https://odissei-data.nl/en/using-soda/.

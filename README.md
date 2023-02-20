# Corporate Scraper - FIRMBACKBONE Project

FIRMBACKBONE's Corporate Scraper - cpscraper - is a Python package, used for web scraping purposes, which is specifically developed for the [FIRMBACKBONE Project](https://firmbackbone.nl). Besides scraping the provided URLs this web scraper also recognizes different attributes (for example addresses, phone numbers and registration numbers for the Dutch chambers of commerce) and stores those separately.

FIRMBACKBONE is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research. Once it is ready, it will become available for researchers and students affiliated with member universities in the Netherlands through [ODISSEI](https://odissei-data.nl/nl/), the Open Data Infrastructure for Social Science and Economic Innovations.

FIRMBACKBONE is an initiative of Utrecht University and the Vrije Universiteit Amsterdam funded by [PDI-SSH](https://pdi-ssh.nl/nl/home/), the Platform Digital Infrastructure-Social Sciences and Humanities, for the period 2020-2025.

## Installation (LOCAL - PIP)

Use the package manager [pip]() to install cpscraper.

You need to have poetry installed (``pip install poetry``), do remember to either restart your pc or add poetry to your PATH variables.
If you get a warning that dependencies are not up to date, run ``poetry update`` first.

```bash
cd to the cpscraper folder
$ poetry install
$ poetry build
$ pip install [PATH].whl
```

## Installation (LOCAL - SCRIPT)

Package can also be used as a script without installation.
When using windows, remember to use cd \ followed by the drive letter to change drives

```bash
cd ~/home/[wherever the folder is located]/corporate_scraper/src
$ python(3) -m cpscraper init
```

## Installation (PIP)

Use the package manager [pip]() to install cpscraper.

```bash
$ pip install cpscraper
```

## Usage

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
Anything else means the tests failed. If this happens, a reinstall is required

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)

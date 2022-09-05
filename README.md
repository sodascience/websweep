# Corporate Scraper - Firm Backbone Project

Corporate Scraper - cpscraper - is a Python package, used for web scraping purposes.
Developed for the [Firm Backbone Project](https://firmbackbone.nl)

## Installation (LOCAL - PIP)

Use the package manager [pip]() to install cpscraper.

You need to have poetry installed (```pip install poetry```), do remember to either restart your pc or add poetry to your PATH variables.
If you get a warning that dependencies are not up to date, run ```poetry update``` first.


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
cd ~/home/[wherever the folder is located]/corporate_scraper
$ python(3) -m src.cpscraper init
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

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)

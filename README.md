# Corporate Scraper - Firm Backbone Project

Corporate Scraper - cpscraper - is a Python package, used for web scraping purposes. 
Developed for the [Firm Backbone Project](https://firmbackbone.nl)


## Installation (LOCAL - PIP)

Use the package manager [pip]() to install cpscraper.
You need to have poetry installed: ```pip install poetry```

```bash
cd to the cpscraper folder
$ poetry install
$ poetry build
$ pip install [PATH].whl
```


## Installation (LOCAL - SCRIPT)

Package can also be used as a script without installation.

```bash
cd to the source folder, containing the cpscraper module
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
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)

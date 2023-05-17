# WebSweep: A User-Friendly and High-Speed Web Scraping Library 

WebSweep is a powerful Python library crafted for web scraping projects that prioritize simplicity, modularity, and impressive speed. 

Real-World Use Cases
- Tracking Corporate Climate Responsibility: With a list of corporate websites, you can use WebSweep to efficiently analyze how frequently and positively they mention green energies, helping you gauge their commitment to climate responsibility.
- Analyzing Academic Collaboration Networks: WebSweep can be utilized to extract data from university websites and research databases, allowing you to identify patterns in academic collaboration, map research networks, and discover emerging interdisciplinary research fields.
- Tracking Public Health Information: By scraping data from government websites, health organizations, and medical journals, WebSweep can help you monitor the spread of diseases, evaluate the effectiveness of public health campaigns, and analyze the impact of healthcare policies on population health.

## Side-by-side comparison of WebSweep and Scrapy
 
- Are you looking to download lots of information from one domain --> You may want to use [Scrapy](https://github.com/scrapy/scrapy)
- Are you looking to download information from websites that require JavaScript --> You may want to use [selenium](https://pypi.org/project/selenium/)
- Are you looking to download and analyze HTML code from many pages --> FastScraper is for you


|                                       | WebSweep                                         | Scrapy                                                        |
|---------------------------------------|-----------------------------------------------------|---------------------------------------------------------------|
| Main use case                         | Download full HTML of many (up to 10,000,000) sites | Download specific elements of few websites (e.g. crawl Ebay)  |
| Intended use                          | Research                                            | Any                                                           |
| Use as beginner                    | Simple                                              | Complicated                                                   |
| Processing of HTML                    | After crawling                                      | Typically during crawling                                     |
| Asynchronous                          |  Yes                                                | Yes                                                           |
| Speed (consumer laptop/home internet) | 50,000-100,000 pages/hour                           | ?                                                             |
| JavaScript allowed                    | No                                                  | No (but extensions exist)                                     |
| Consolidates results at domain level  | Yes                                                 | No                                                            |



## Installation

Use the package manager [pip]() to install cpscraper.

```bash
$ pip install cpscraper
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


## License

[MIT](https://choosealicense.com/licenses/mit/)


## Contributing

Contributions are what make the open source community an amazing place
to learn, inspire, and create. Any contributions you make are **greatly
appreciated**.

Please refer to the
[DEVELOPMENT](https://github.com/sodascience/corporate_scraper/blob/main/DEVELOPMENT.md)
file for more information on how to run the library without installing and how to install it from source.

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

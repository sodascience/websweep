
The package can also be used as a script without installation.
When using windows, remember to use cd \ followed by the drive letter to change drives

```bash
cd ~/home/[wherever the folder is located]/corporate_scraper/src
$ python(3) -m websweep init
```

You can also install the library from source. You need to have poetry installed (``pip install poetry``), do remember to either restart your pc or add poetry to your PATH variables. If you get a warning that dependencies are not up to date, run ``poetry update`` first.

```bash
cd to the websweep folder
$ poetry install
$ poetry build
$ pip install [PATH].whl
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

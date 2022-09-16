import os
import sys
from tomlkit import boolean

#This mumbo jumbo is required to properly import the Extractor class, I don't quite understand how it works, but it does
sys.path.append( os.path.join( os.path.dirname( __file__ ), '..', 'src', 'cpscraper', 'extractor' ))
from extractor import Extractor as E

def phones_pass(text) -> boolean: 
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = {'T.  0318 - 57 50 70', 'tel: +31511472025', 'tel: +0511472025', 'Tel:  0318 - 57 50 70', 'Telefoon:  038 8211 455 01 01'}
    tester.scrape_phone()
    if tester.phone == expected_result:
        return True
    else:
        return False

def kvk_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = {'01051548', '11030697'}
    tester.scrape_kvk()
    if tester.kvk == expected_result:
        return True
    else:
        return False

def btw_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = {'NL.0069.34.857.B.01'}
    tester.scrape_btw()
    if tester.btw == expected_result:
        return True
    else:
        return False

def email_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = {'info@applebee.nl', 'info@staalbouw-barneveld.nl', 'info@bosgra.nl', 'info@advocaatruis.nl', 'info@ijsselkern.nl'}
    tester.scrape_email()
    if tester.email == expected_result:
        return True
    else:
        return False

def fax_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = {'0648515987'}
    tester.scrape_fax()
    if tester.fax == expected_result:
        return True
    else:
        return False

def zip_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = ['3927 GH', '8013 AG', '3437ZE', '8021 CD', '3403 AE', '9254 DD', '7941LX']
    tester.scrape_zip()
    res = tester.metadata["postcode"]

    if len(res) != len(expected_result):
        return False

    expected_result.sort()
    res.sort()
    if res == expected_result:
        return True
    else:
        return False

def adress_pass(text) -> boolean:
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = text
    expected_result = ['Ceintuurbaan 106', 'Hofstraat 5,', 'Hortensiastraat 72,', 'Rijksstraatweg 137/139', 'Overeem 4']
    tester.scrape_zip()
    tester.scrape_adres()
    res = tester.metadata["address"]

    if len(res) != len(expected_result):
        return False

    expected_result.sort()
    res.sort()

    if res == expected_result:
        return True
    else:
        return False



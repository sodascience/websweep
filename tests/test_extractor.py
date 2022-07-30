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
    expected_result = {'T:  0631313737 ', 'tel: +316-3131-3737 ', 'Tel:  06-31-31-37-37 ', 't:  +31631313737'}
    tester.scrape_phone()
    if tester.phone == expected_result:
        return True
    else:
        return False

    
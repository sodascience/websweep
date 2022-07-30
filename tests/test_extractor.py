import os
import sys
script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, '..', 'src', 'cpscraper', 'extractor' )
sys.path.append( mymodule_dir )
print(mymodule_dir)
from extractor import Extractor as E

def phones_pass():
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = "Tel: +316313737 T:0631313737 t: 06-09-02-03-05-088 wat random tekst, tel\"4861321  en daarna nog gwn 051341698"
    expected_result = ""
    tester.scrape_phone()
    print(tester.phone)
    if tester.phone == expected_result:
        return True
    else:
        return False

def phones_fail():
    aList = ["id", "domain", "level", "url", "date", "path"]
    tester = E(aList)
    tester.text = "Tel: +316313737 T:0631313737 t: 06-09-02-03-05-088 wat random tekst, tel\"4861321  en daarna nog gwn 051341698"
    
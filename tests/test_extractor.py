import os
import sys
script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, '..', 'src', 'cpscraper', 'extractor' )
sys.path.append( mymodule_dir )
print(mymodule_dir)
from extractor import Extractor as E

def test_phone():
    to_test = "+31313737 0631313737 06-09-02-03-05-088"
    expected_result = ""


    tester = E()
    tester.text = "+31313737 0631313737 06-09-02-03-05-088"
    tester.scrape_phone()
    
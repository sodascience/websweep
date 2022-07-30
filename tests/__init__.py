import unittest
import test_extractor as TE
import os


class TestRegMethods(unittest.TestCase):

    def test_phones(self):
        with open("tests/units.txt", "r") as file:
            text = file.read()
            self.assertTrue(TE.phones_pass(text))




if __name__ == "__main__":
    unittest.main()

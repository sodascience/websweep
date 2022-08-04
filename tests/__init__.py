import unittest
import test_extractor as TE


class TestRegMethods(unittest.TestCase):

    def setUp(self):
        self.testfile = open("tests/units.txt", "r")
        self.text = self.testfile.read()

    def test_phones(self) -> None:
        self.assertTrue(TE.phones_pass(self.text))

    def test_kvk(self) -> None:
        self.assertTrue(TE.kvk_pass(self.text))

    def test_btw(self) -> None:
        self.assertTrue(TE.btw_pass(self.text))

    def tearDown(self) -> None:
        self.testfile.close()
        return super().tearDown()


if __name__ == "__main__":
    unittest.main()

import unittest
import test_extractor as TE

class TestRegMethods(unittest.TestCase):

    def test_phones(self):
        self.assertTrue(TE.test_phone())
        # self.assertFalse(test_extractor.test_phone)





if __name__ == "__main__":
    unittest.main()

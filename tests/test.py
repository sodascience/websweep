# import unittest
# import test_extractor as TE


# class TestRegMethods(unittest.TestCase):

#     def setUp(self):
#         self.testfile = open("units.txt", "r")
#         self.text = self.testfile.read()

#     def test_phones(self) -> None:
#         self.assertTrue(TE.phones_pass(self.text))

#     def test_kvk(self) -> None:
#         self.assertTrue(TE.kvk_pass(self.text))

#     def test_btw(self) -> None:
#         self.assertTrue(TE.btw_pass(self.text))

#     def test_email(self) -> None:
#         self.assertTrue(TE.email_pass(self.text))   

#     def test_fax(self) -> None:
#         self.assertTrue(TE.fax_pass(self.text))

#     def test_zip(self) -> None:
#         self.assertTrue(TE.zip_pass(self.text))

#     def test_adress(self) -> None:
#         self.assertTrue(TE.adress_pass(self.text))

#     def tearDown(self) -> None:
#         self.testfile.close()
#         return super().tearDown()


# if __name__ == "__main__":
#     unittest.main()

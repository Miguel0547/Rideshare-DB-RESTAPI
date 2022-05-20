import json
import unittest
from tests.test_utils import *


class TestAccounts(unittest.TestCase):

    def test_00_accounts_rest1(self):
        expected_number_of_users = 20
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts')
        self.assertEqual(expected_number_of_users, len(actual))

    def test_01_account_rider_rest1(self):
        expected = [1, "mikeE06830", "Mike Easter", 3.3, "06830"]
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/riders?id=1')
        self.assertEqual(expected, actual)

    def test_02_account_rider_dne_rest1(self):
        expected = []
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/riders?id=10000')
        self.assertEqual(expected, actual)

    def test_03_account_driver_rest1(self):
        expected = [1, "tomM061598", "Tom Magliozzi", 4.1, "BMW", "X3", "30301"]
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=1')
        self.assertEqual(expected, actual)

    def test_04_account_driver_dne_rest1(self):
        expected = []
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=2000')
        self.assertEqual(expected, actual)

    def test_05_rides_rest1(self):
        expected_number_of_users = 11
        actual = get_rest_call(self, 'http://127.0.0.1:5000/rides')
        self.assertEqual(expected_number_of_users, len(actual))

    # def test_06_create_driver_rest1(self):
    #     expected = [10, "Maxie", "Maximus Prime", 0, "BMW", "MX6", "06807"]
    #     url1 = 'http://127.0.0.1:5000/accounts/drivers'
    #     hdr = {"content-type": "application/json"}
    #     driver = {"user_name": "Maxie", "password": "Maximus0", "name": "Maximus Prime", "zip_code": "06807",
    #               "car_make": "BMW", "car_model": "MX6"}
    #     jdata = json.dumps(driver)
    #     post_rest_call(self, url1, jdata, hdr)
    #     actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=10')
    #     self.assertEqual(expected, actual)
    #
    # def test_07_create_rider_rest1(self):
    #     expected = [14, "Primey", "Prime Time", 0, "06807"]
    #     url1 = 'http://127.0.0.1:5000/accounts/riders'
    #     hdr = {"content-type": "application/json"}
    #     rider = {"user_name": "Primey", "password": "Prime0", "name": "Prime Time", "zip_code": "06807"}
    #     jdata = json.dumps(rider)
    #     post_rest_call(self, url1, jdata, hdr)
    #     actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/riders?id=14')
    #     self.assertEqual(expected, actual)

    def test_08_rest2(self):
        print("\n\n\n REST2 TESTS IN ORDER OF TEST CASES ON SWEN344 REST PROJECT PAGE\n\n\n")
        print("\n\nTest: User login is successful")
        url0 = 'http://127.0.0.1:5000/login'
        hdr = {"content-type": "application/json"}
        user = {"user_type": "d", "user_name": "HokiePokie", "password": "Hoke0"}
        jdata = json.dumps(user)
        result = post_rest_call(self, url0, jdata, hdr)
        session_key0 = result["Successful login"]
        hdr = {"key": session_key0}
        actual = get_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=3', {}, hdr)
        print(str(actual) + "\n")

        print("Test01a: User login fail due to incorrect username")
        url0 = 'http://127.0.0.1:5000/login'
        hdr = {"content-type": "application/json"}
        user = {"user_type": "d", "user_name": "HokiePo", "password": "Hoke0"}
        jdata = json.dumps(user)
        result = post_rest_call(self, url0, jdata, hdr)
        print(result + "\n")

        print("Test01b: User login fail due to incorrect password")
        url0 = 'http://127.0.0.1:5000/login'
        hdr = {"content-type": "application/json"}
        user = {"user_type": "d", "user_name": "HokiePokie", "password": "hoke0"}
        jdata = json.dumps(user)
        result = post_rest_call(self, url0, jdata, hdr)
        print(result + "\n")

        print("Test02a: Add a new user with a password")
        url1 = 'http://127.0.0.1:5000/accounts/drivers'
        hdr = {"content-type": "application/json", "key": session_key0}
        driver = {"user_name": "Young-Guels", "password": "YoungGuelo", "name": "Miguel Reyes", "zip_code": "06807",
                  "car_make": "BMW", "car_model": "MX6"}
        jdata = json.dumps(driver)
        result = post_rest_call(self, url1, jdata, hdr)
        print(str(result))
        result = get_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=10')
        print(str(result) + "\n")

        print("Test02b: Add a user that already exists. This will fail and not create a user")
        url1 = 'http://127.0.0.1:5000/accounts/drivers'
        driver = {"user_name": "Young-Guels", "password": "YoungGuelo", "name": "Miguel Reyes", "zip_code": "06807",
                  "car_make": "BMW", "car_model": "MX6"}
        jdata = json.dumps(driver)
        result = post_rest_call(self, url1, jdata, hdr)
        print(result+ "\n")

        print("Test03a: Update a user. Update his name.")
        url1 = 'http://127.0.0.1:5000/accounts/riders'
        initial = get_rest_call(self, 'http://127.0.0.1:5000/accounts/riders?id=12', {}, hdr)
        rider = {"user_name": "VladTheV", "password": "Vlad0", "name": "Vlad", "zip_code": "06807"}
        jdata = json.dumps(rider)
        result = put_rest_call(self, url1, jdata, hdr)
        later = get_rest_call(self, 'http://127.0.0.1:5000/accounts/riders?id=12', {}, hdr)
        print(result)
        print(str(initial) + "\t" + str(later) + "\n")

        print("Test04b: Update a user that DNE. Will fail and return error message.")
        url1 = 'http://127.0.0.1:5000/accounts/riders'
        rider = {"user_name": "MikeLEE", "password": "Hi", "name": "Mike", "zip_code": "06807"}
        jdata = json.dumps(rider)
        result = put_rest_call(self, url1, jdata, hdr)
        print(result + "\n")

        print("Test05a: Remove the user that we created in Test03 - Young-Guels")
        result = delete_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=10', hdr)
        print(result + "\n")

        print("Test05b: Remove a user that DNE - Will Fail")
        result = delete_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=15', hdr)
        print(result + "\n")

        print("Test05c: Remove a user that exists but session key DNE - Will Fail")
        hdr = {"content-type": "application/json", "key": '04u40802420fj2ncc0204ifn0c24'}
        result = delete_rest_call(self, 'http://127.0.0.1:5000/accounts/drivers?id=10', hdr)
        print(result + "\n")











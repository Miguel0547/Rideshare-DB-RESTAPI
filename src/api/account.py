import psycopg2
from flask import request
from flask_restful import Resource, reqparse
from db import rideshare, authentication
import datetime


class Login(Resource):
    def post(self):
        """
              Login
         """
        parser = reqparse.RequestParser()
        parser.add_argument('user_type', type=str)
        parser.add_argument('user_name', type=str)
        parser.add_argument('password', type=str)
        args = parser.parse_args()
        user_type = args['user_type']
        user_name = args['user_name']
        password = args['password']
        hashed_password = authentication.hash_password(password)
        results = authentication.login(user_type, user_name, hashed_password)
        try:
            int(results, 16)
            message = "Successful login"
            login = {message: results}
            return login
        except ValueError:
            message = results
            return message


class Logout(Resource):
    def post(self):
        """
              Logout
         """
        parser = reqparse.RequestParser()
        parser.add_argument('user_type', type=str)
        parser.add_argument('user_name', type=str)
        parser.add_argument('password', type=str)
        args = parser.parse_args()
        user_type = args['user_type']
        user_name = args['user_name']
        password = args['password']
        hashed_password = authentication.hash_password(password)
        results = authentication.logout(user_type, user_name, hashed_password)
        try:
            int(results, 16)
            message = "Successful logout"
            logout = {message: results}
            return logout
        except ValueError:
            message = results
            return message


class Accounts(Resource):
    def get(self):
        accounts = rideshare.all_accounts()
        return list(accounts)


class Driver(Resource):
    def get(self):
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            driver_id = int(request.args.get('id'))
            driver = rideshare.get_account('drivers', driver_id)
            if driver is None:
                return []
            return list(driver)
        return "USER NOT LOGGED IN"

    def post(self):
        """
             Create a new driver
        """
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            parser = reqparse.RequestParser()
            parser.add_argument('user_name', type=str)
            parser.add_argument('password', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('zip_code', type=str)
            parser.add_argument('car_make', type=str)
            parser.add_argument('car_model', type=str)
            args = parser.parse_args()
            user_name = args['user_name']
            password = args['password']
            password = authentication.hash_password(password)
            name = args['name']
            zip_code = args['zip_code']
            car_make = args['car_make']
            car_model = args['car_model']
            try:
                rideshare.create_new_account_db2(user_name, password, name, datetime.datetime.today(), zip_code,
                                                 car_make,
                                                 car_model)
                return "User created successfully"
            except psycopg2.Error:
                return "USER ALREADY EXISTS"
        else:
            return "USER NOT LOGGED IN"

    def put(self):
        """
             Update driver
        """
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            parser = reqparse.RequestParser()
            parser.add_argument('user_name', type=str)
            parser.add_argument('password', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('zip_code', type=str)
            parser.add_argument('car_make', type=str)
            parser.add_argument('car_model', type=str)
            args = parser.parse_args()
            user_name = args['user_name']
            password = args['password']
            password = authentication.hash_password(password)
            name = args['name']
            zip_code = args['zip_code']
            car_make = args['car_make']
            car_model = args['car_model']
            result = rideshare.update_account_db2("d", user_name, password, name, password, zip_code, car_make,
                                                  car_model)
            if result is not None:
                return "User updated successfully"
            else:
                return "USER DOES NOT EXIST"
        else:
            return "USER NOT LOGGED IN"

    def delete(self):
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            driver_id = int(request.args.get('id'))
            driver = rideshare.remove_or_disable_account_db2(driver_id, "d", 1)
            if driver == 1:
                return "Delete Driver is successful"
            return "Delete Driver is unsuccessful. Driver does not exist"
        return "USER NOT LOGGED IN"


class Rider(Resource):
    def get(self):
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            rider_id = int(request.args.get('id'))
            rider = rideshare.get_account('riders', rider_id)
            if rider is None:
                return []
            return list(rider)
        return "USER NOT LOGGED IN"

    def post(self):
        """
        Create a new rider
        """
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            parser = reqparse.RequestParser()
            parser.add_argument('user_name', type=str)
            parser.add_argument('password', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('zip_code', type=str)
            args = parser.parse_args()
            user_name = args['user_name']
            password = args['password']
            password = authentication.hash_password(password)
            name = args['name']
            zip_code = args['zip_code']
            try:
                rideshare.create_new_account_db2(user_name, password, name, datetime.datetime.today(), zip_code)
                return "User created successfully"
            except psycopg2.Error:
                return "USER ALREADY EXISTS"
        else:
            return "USER NOT LOGGED IN"

    def put(self):
        """
             Update rider
        """
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            parser = reqparse.RequestParser()
            parser.add_argument('user_name', type=str)
            parser.add_argument('password', type=str)
            parser.add_argument('name', type=str)
            parser.add_argument('zip_code', type=str)
            args = parser.parse_args()
            user_name = args['user_name']
            password = args['password']
            password = authentication.hash_password(password)
            name = args['name']
            zip_code = args['zip_code']
            result = rideshare.update_account_db2("r", user_name, password, name, zip_code)
            if result is not None:
                return "User updated successfully"
            else:
                return "USER DOES NOT EXIST"
        else:
            return "USER NOT LOGGED IN"

    def delete(self):
        session_key = request.headers.get('Key')
        if authentication.check_session_key(session_key):
            driver_id = int(request.args.get('id'))
            driver = rideshare.remove_or_disable_account_db2(driver_id, "r", 1)
            if driver == 1:
                return "Delete Rider is successful"
            return "Delete Rider is unsuccessful. Driver does not exist"
        return "USER NOT LOGGED IN"

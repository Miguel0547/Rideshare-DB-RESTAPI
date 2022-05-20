from flask import Flask
from flask_restful import Api
from api.ride import Rides
from api.account import Accounts,Driver, Rider, Login
from db.init_db import load_db

app = Flask(__name__)
api = Api(app)

api.add_resource(Login, '/login')
api.add_resource(Rides, '/rides')
api.add_resource(Accounts, '/accounts')
api.add_resource(Driver, '/accounts/drivers')
api.add_resource(Rider, '/accounts/riders')

if __name__ == '__main__':
    load_db()
    app.run(debug=True)
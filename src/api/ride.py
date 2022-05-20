from flask_restful import Resource
from db import rideshare


class Rides(Resource):
    def get(self):
        trips = rideshare.get_trips()
        return list(trips)
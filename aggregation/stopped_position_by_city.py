from .base_aggregation import BaseAggregation
import os


class StoppedPositionByCity(BaseAggregation):
    def __init__(self, city):
        super().__init__('location')
        self.result = self.db['location'].aggregate([
            {'$match': {'status': 'stopped'}}, {'$lookup': {'from': os.getenv("COLLECTION_NAME"), 'localField': 'license_plate', 'foreignField': 'taxis.license_plate', 'as': 'taxi_info'}},
            {'$unwind': '$taxi_info'},
            {'$match': {'taxi_info.city': city}},
            {'$group': {'_id': '$taxi_info.city', 'lon': {'$first': '$lon'},'lat': {'$first': '$lat'}}},
        ])

    def get_result(self):
        return self.result


from .base_aggregation import BaseAggregation
import os


class StoppedCabsCitiesAsc(BaseAggregation):
    def __init__(self):
        super().__init__('location')
        self.result = self.db['location'].aggregate([
    {'$match': {'status': 'stopped'}},
    {
        '$lookup': {
            'from': os.getenv("COLLECTION_NAME"),
            'localField': 'license_plate',
            'foreignField': 'taxis.license_plate',
            'as': 'taxi_info'
        }
    },
    {'$unwind': '$taxi_info'},
    {
        '$group': {
            '_id': '$taxi_info.city',
            'totalStoppedCabs': {
                '$sum': 1
            }
        }
    },
    {'$sort': {'totalStoppedCabs': 1}}
])

    def get_result(self):
        return self.result


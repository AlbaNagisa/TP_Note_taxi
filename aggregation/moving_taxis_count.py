from datetime import datetime, timedelta, UTC
import os

from .base_aggregation import BaseAggregation


class MovingTaxisCount(BaseAggregation):
    def __init__(self):
        super().__init__('location')
        self.result = self.db['location'].aggregate([
    {
        '$match': {
            'timestamp': {
                '$gte': datetime.now(UTC) - timedelta(hours=1)
            },
            'status': {
                '$eq': 'moving'
            }
        }
    }, {
        '$lookup': {
            'from': os.getenv("COLLECTION_NAME"),
            'localField': 'license_plate',
            'foreignField': 'taxis.license_plate',
            'as': 'agency'
        }
    }, {
        '$unwind': '$agency'
    }, {
        '$group': {
            '_id': '$agency.city',
            'count': {
                '$sum': 1
            }
        }
    }
])
    def get_result(self):
        return self.result


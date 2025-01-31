import os
from datetime import datetime
from .base_aggregation import BaseAggregation


class CompanyFleetAverageAge(BaseAggregation):
    def __init__(self):
        super().__init__(os.getenv("COLLECTION_NAME"))
        self.result = self.db[os.getenv("COLLECTION_NAME")].aggregate([
            {'$unwind': '$taxis'},
            {
                '$group': {
                    '_id': '$name',
                    'city': {'$first': '$city'},
                    'average_age': {
                        '$avg': {'$subtract': [datetime.now().year, '$taxis.year_of_production']}
                    }
                }
            }
        ])

    def get_result(self):
        return self.result


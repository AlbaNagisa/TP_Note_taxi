from .base_aggregation import BaseAggregation
import os


class CompanyWithMostLevel5Autonomy(BaseAggregation):
    def __init__(self):
        super().__init__(os.getenv("COLLECTION_NAME"))
        self.result = self.db[os.getenv("COLLECTION_NAME")].aggregate([
        {'$unwind': '$taxis'},
        {'$match': {'taxis.autonomy_level': 5}},
        {'$group': {'_id': '$name','city': {'$first': '$city'}, 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {"$limit": 1}
    ])

    def get_result(self):
        return self.result


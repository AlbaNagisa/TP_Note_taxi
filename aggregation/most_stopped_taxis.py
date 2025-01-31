from .base_aggregation import BaseAggregation
import os


class MostStoppedTaxis(BaseAggregation):
    def __init__(self):
        super().__init__('location')
        self.result = self.db['location'].aggregate([
            {"$match": {"status": "stopped"}},
            {"$group": {"_id": "$license_plate", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
            {
                "$lookup": {
                    "from": os.getenv("COLLECTION_NAME"),
                    "localField": "_id",
                    "foreignField": "taxis.license_plate",
                    "as": "agency"
                }
            },
            {"$unwind": "$agency"},
            {
                '$lookup': {
                    'from': os.getenv("COLLECTION_NAME"),
                    'let': {
                        'licensePlate': 'AA-123-BB'
                    },
                    'pipeline': [
                        {
                            '$unwind': '$taxis'
                        }, {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$taxis.license_plate', '$$licensePlate'
                                    ]
                                }
                            }
                        }, {
                            '$replaceRoot': {
                                'newRoot': '$taxis'
                            }
                        }
                    ],
                    'as': 'taxi_info'
                }
            },
            {"$unwind": "$taxi_info"},
            {
                "$project": {
                    "_id": 0,
                    "license_plate": "$_id",
                    "count": 1,
                    "autonomy_level": "$taxi_info.autonomy_level",
                    "year_of_production": "$taxi_info.year_of_production",
                    "manufacturer": "$taxi_info.manufacturer",
                    "battery_capacity_kwh": "$taxi_info.battery_capacity_kwh",
                    "current_status": "$taxi_info.current_status",
                    "agency": {
                        "name": "$agency.name",
                        "city": "$agency.city",
                        "address": "$agency.address",
                        "founder_year": "$agency.founded_year",
                        "fleet_size": "$agency.fleet_size",
                    }
                }
            }
        ])
    def get_result(self):
        return self.result


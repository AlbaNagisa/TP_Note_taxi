from .base_aggregation import BaseAggregation
import os


class CurrentlyStoppedCabs(BaseAggregation):
    def __init__(self):
        super().__init__('location')
        self.db.create_view('location', 'currently_stopped_cabs', [
            {'$sort': {'timestamp': -1}},
            {
                '$group': {
                    '_id': '$license_plate',
                    'latestLocation': {
                        '$first': '$$ROOT'
                    }
                }
            },
            {
                '$replaceRoot': {
                    'newRoot': '$latestLocation'
                }
            },
            {'$match': {'status': 'stopped'}},
            {
                '$lookup': {
                    'from': os.getenv("COLLECTION_NAME"),
                    'localField': 'license_plate',
                    'foreignField': 'taxis.license_plate',
                    'as': 'agency'
                }
            },
            {'$unwind': '$agency'},
            {
                '$project': {
                    '_id': 0,
                    'last_status': '$status',
                    'last_timestamp': '$timestamp',
                    'taxi_id': '$license_plate',
                    'location': {
                        'type': 'Point',
                        'coordinates': [
                            '$lon', '$lat'
                        ]
                    },
                    'company_name': '$agency.name',
                    'company_city': '$agency.city',
                    'company_fleet_size': '$agency.fleet_size'
                }
            }
        ])


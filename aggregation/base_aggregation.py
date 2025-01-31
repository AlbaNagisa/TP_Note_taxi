from database import Database


class BaseAggregation:
    def __init__(self, collection_name):
        self.db = Database()
        self.db.connect()
        self.db.set_database('Alban_Girardi_Taxis')
        self.collection_name = collection_name



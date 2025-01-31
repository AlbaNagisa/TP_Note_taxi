from utils import Singleton
from pymongo import MongoClient

class Database(metaclass=Singleton):

    def __init__(self):
        self.client = None
        self.db = None

    def __getitem__(self, item):
        return self.db[item]

    def connect(self):
        if self.client is not None:
            return
        try:
            self.client = MongoClient('localhost', 27017)
            print("MongoDB connecting...")
        except Exception as e:
            print(e)



    def set_database(self, database_name):
        if self.client is None:
            return
        if self.db is not None:
            return
        print(f"Setting database {database_name}")
        self.db = self.client[database_name]

    def create_collection(self, collection_name, validator=None):
        if self.client is None:
            return
        if self.db is None:
            return
        if collection_name in self.db.list_collection_names():
            return
        print(f"Creating collection {collection_name}")
        self.db.create_collection(collection_name, validator=validator)

    def create_timeseries_collection(self, collection_name, timeseries):
        if self.client is None:
            return
        if self.db is None:
            return
        if collection_name in self.db.list_collection_names():
            return
        print(f"Creating timeseries collection {collection_name}")
        self.db.create_collection(collection_name, timeseries=timeseries)

    def seed_database(self, collection_name, data):
        if self.client is None:
            return
        if self.db is None:
            return
        if collection_name not in self.db.list_collection_names():
            return
        self.db[collection_name].insert_many(data)

    def insert_one(self, collection_name, data):
        if self.client is None:
            return
        if self.db is None:
            return
        if collection_name not in self.db.list_collection_names():
            return
        self.db[collection_name].insert_one(data)

    def create_view(self, collection_name, view_name, pipeline):
        if self.client is None:
            return
        if self.db is None:
            return
        if view_name in self.db.list_collection_names():
            print(f"View {view_name} already exists, please go to mongoCompass to check it")
            return
        if collection_name not in self.db.list_collection_names():
            return
        self.db.create_collection(view_name, viewOn=collection_name, pipeline=pipeline)
        print(f"Creating view {view_name} on {collection_name}")

    def create_index(self, collection_name, index):
        if self.client is None:
            return
        if self.db is None:
            return
        if collection_name not in self.db.list_collection_names():
            return
        self.db[collection_name].create_index(index)


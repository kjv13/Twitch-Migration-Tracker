#!/usr/bin/python
import sys
import configparser
from pymongo import MongoClient


class NoSQLConnection:
    """Used to connect to a NoSQL database and send queries into it"""
    config_file = 'db.cfg'
    section_name = 'NonRelational Database Details'

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(self.config_file)

        try:
            self.db_name = config[self.section_name]['db_name']
            self.watching_collection = \
                config[self.section_name]['watching_collection_name']
            self.monitoring_collection = \
                config[self.section_name]['monitoring_collection_name']
            self.migration_collection_name = \
                config[self.section_name]['migrations_collection_name']
            self.hostname = config[self.section_name]['hostname']
            self.user = config[self.section_name]['user']
        except Exception as e:
            print('there was a problem accessing the db.cfg file and reading')
            print(str(e))
            sys.exit()

        self.client = MongoClient(self.hostname, 27017)
        self.db = self.client[self.db_name]

    def query_one(self, criteria=None, projection=None):
        result = self.db[self._collection].find_one(criteria, projection)
        return result

    def query(self, criteria=None, projection=None):
        result = self.db[self._collection].find(criteria, projection)
        return result

    def update(self, criteria, new_values, upsert=True):
        result = self.db[self._collection].update_one(criteria, new_values,
                                                      upsert)
        return result

    def delete(self, criteria):
        result = self.db[self._collection].delete_many(criteria)
        return result

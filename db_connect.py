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
            self._db_name = config.get[self.section_name]['db_name']
            self._collection = config.get[self.section_name]['collection_name']
            self._hostname = config.get[self.section_name]['hostname']
            self._user = config.get[self.section_name]['user']
        except Exception as e:
            print('one of the options in the config file has no value\n{0}: ' +
                  '{1}').format(e.errno, e.strerror)
            sys.exit()

        self.client = MongoClient(self._hostname, 27017)
        self.db = self.client[self._db_name]

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

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
            self.monitoring_collection = \
                config[self.section_name]['monitoring_collection_name']
            self.migration_collection = \
                config[self.section_name]['migrations_collection_name']
            self.viewercount_collection = \
                config[self.section_name]['viewercount_collection_name']
            self.hostname = config[self.section_name]['hostname']
            self.user = config[self.section_name]['user']
        except Exception as e:
            print('there was a problem accessing the db.cfg file and reading')
            print(str(e))
            sys.exit()

        self.client = MongoClient(self.hostname, 27017)
        self.db = self.client[self.db_name]

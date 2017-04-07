#!/usr/bin/python
import sys
import os
import configparser
from pymongo import MongoClient


class NoSQLConnection:
    """Used to connect to a NoSQL database and send queries into it"""

    current_dir = os.path.dirname(__file__)
    config_rel_path = '../config/db.cfg'
    config_abs_path = os.path.join(current_dir, config_rel_path)

    section_name = 'NonRelational Database Details'

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(self.config_abs_path)

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

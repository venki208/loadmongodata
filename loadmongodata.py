import os
import json
import pymongo

from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):

    '''
    Manage command for inserting mongodata into database. it will act like loaddata
    cmd: python manage.py loadmongodata <fixtures.json>
    '''

    def add_arguments(self, parser):
        parser.add_argument('filepath', nargs='+', type=str)

    def handle(self, *args, **options):
        mongo_db = None
        mongo_username = None
        mongo_password = None
        mongo_host = None
        ob_count = 0
        jsonfile = open(settings.BASE_DIR + '/' + options['filepath'][0], 'r')
        parsed = json.loads(jsonfile.read())
        connection = pymongo.MongoClient(
            "mongodb://"+mongo_host,
            username=mongo_username,
            password=mongo_password,
            authSource=mongo_db
        )
        db = connection.get_database(mongo_db)
        db_table = db.get_collection(parsed['table_name'])
        db_table.insert_many(parsed["data"])
        self.stdout.write('Successfully inserted "%d"' % db_table.count())


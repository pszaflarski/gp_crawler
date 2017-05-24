import csv
import json

from general_tools import *
from sync_cache import *

import psycopg2
import boto3

if __name__ == '__main__':
    start_url = 'http://www.fuschia.ie/'
    creds = load_creds("credentials.json")

    reader = csv.reader(open("cache\\cachemap.csv"))

    rows = [x for x in reader if x[0] == start_url]

    if len(rows) == 0: sync_from_postgres(start_url, creds)

    for row in rows:
        source = open("cache\\" + row[2], 'r', encoding='utf-8', errors='ignore').read()
        print(row[1], source)
        tree = etree_pipeline_fromstring(source)

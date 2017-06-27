import csv
import json

from general_tools import *

import psycopg2
import boto3


def find_scraped_pages(table, start_url, creds):
    conn = psycopg2.connect(**creds)
    cur = conn.cursor()

    cur.execute("""SELECT * FROM {table} where start_url = %s;""".format(table=table), (start_url,))

    return cur.fetchall()


def get_source_from_s3(sourcefile_name, bucket, cred_dict):
    aws_access_key_id = cred_dict['aws_access_key_id']
    aws_secret_access_key = cred_dict['aws_secret_access_key']

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    ob = s3.Object(bucket, sourcefile_name)
    return ob.get()["Body"].read().decode('utf-8')


def save_source_file(filename, source):
    f = open("cache\\" + filename, 'w', encoding='utf-8', errors='ignore')
    f.write(source)
    f.close()


def save_cachemap(start_url, url, filename):
    writer = csv.writer(open('cache\\cachemap.csv', 'a', encoding='utf-8', errors='ignore'), lineterminator='\n')

    row = [start_url, url, filename, datetime.datetime.utcnow()]

    writer.writerow(row)


def sync_from_postgres(start_url, creds, table, bucket):
    s3_creds = creds['s3']
    postgres_creds = creds['postgres']

    l = find_scraped_pages(table, start_url, postgres_creds)
    for result in l:
        sourcefile_name = result[-2]
        url = result[1]

        source = get_source_from_s3(sourcefile_name, bucket, s3_creds)

        save_source_file(sourcefile_name, source)

        save_cachemap(start_url, url, sourcefile_name)

        print(url)


def check_cache(start_url=None):
    reader = csv.reader(open('cache\\cachemap.csv', 'r', encoding='utf-8', errors='ignore'))

    cached_at_list = []

    for row in reader:
        if row[0] == start_url or start_url is None:
            now = datetime.datetime.utcnow()
            cached_at = datetime.datetime.strptime(row[-1], "%Y-%m-%d %H:%M:%S.%f")
            cached_at_list.append(now - cached_at)

    return min(cached_at_list)


if __name__ == '__main__':
    start_url = 'http://fenixcosmetics.com'
    creds = load_creds("credentials.json")

    try:
        t = check_cache(start_url)
        print("time since last cache:", t)
        print("seconds since last sync:", t.total_seconds())
    except (FileNotFoundError, ValueError):
        print("cache missing")

    yn = input("resync " + start_url + "?(y/n)").lower()

    if yn == 'y':
        sync_from_postgres(start_url, creds, table=creds['postgres_path'], bucket=creds['s3_bucket'])
    else:
        print("sync not complete")

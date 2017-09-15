from .db_connector import *
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-params', action='store')
    args = parser.parse_args()

    if args.params is None:
        cdc = CrawlerDataConnector()
        cdc.cache_to_db()
    else:
        params = json.loads(args.params)
        cdc = CrawlerDataConnector(**params)
        cdc.cache_to_db()

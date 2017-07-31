import multiprocessing
import subprocess
import csv
import platform

import pandas
from sqlalchemy import *

from gp_crawler import *


def create_queue(it, db_file='./queue.db', table_name='queue'):
    with open(db_file, 'w'):
        pass

    df = pandas.DataFrame(it)
    engine_string = 'sqlite:///{db_file}'.format(db_file=db_file)
    db = create_engine(engine_string)
    df.to_sql(con=db, index_label='id', name=table_name, if_exists='replace')


def get_job_from_queue(index, db_file='queue.db', table_name='queue'):
    engine_string = 'sqlite:///{db_file}'.format(db_file=db_file)
    db = create_engine(engine_string)
    metadata = MetaData()

    t = Table(table_name, metadata, autoload=True, autoload_with=db)

    d = dict(list(db.execute(t.select().where(t.c.id == index)))[0])

    return d


def multijob(i):
    j = get_job_from_queue(i)
    jid = {x: y for x, y in j.items() if x != 'id'}

    cr = Crawler()
    cr.crawl_site(**jid)

def wrapper(d):
    cr = Crawler()
    cr.crawl_site(**d)



if __name__ == '__main__':
    url_list = [{'start_url': 'http://chef5minutemeals.com/'}, {'start_url': 'http://slapyamama.com/'},
                {'start_url': 'https://soredgear.com/'}]
    create_queue(url_list)
    index_list = list(range(len(url_list)))

    pool = multiprocessing.Pool(processes=2)

    print(pool.map(wrapper, url_list))





    #

import multiprocessing
import subprocess
import csv
import platform

import pandas
from sqlalchemy import *

from gp_crawler import *

def wrapper(d):
    cr = Crawler()
    cr.crawl_site(**d)



if __name__ == '__main__':
    url_list = [{'start_url': 'http://chef5minutemeals.com/'}, {'start_url': 'http://slapyamama.com/'},
                {'start_url': 'https://soredgear.com/'}]

    pool = multiprocessing.Pool(processes=2)

    print(pool.map(wrapper, url_list))





    #
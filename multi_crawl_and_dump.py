import multiprocessing
import subprocess
import csv


def start_crawl(arg):
    cmd = 'python crawl_and_dump.py -index ' + str(arg)
    print(cmd)
    return subprocess.call(cmd, shell=False)


def start_multiprocess(l,num_workers =4, queue_csv = "queue.csv"):

    out_row = [[x] for x in l]
    with open(queue_csv, 'w', encoding='utf-8', errors='ignore') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerows(out_row)

    args = list(range(len(l)))

    pool = multiprocessing.Pool(processes=num_workers)

    print(pool.map(start_crawl, args))


if __name__ == '__main__':

    start_url_list = ['http://www.fuschia.ie/', 'http://www.fuschia.ie/', 'http://www.fuschia.ie/',
                      'http://www.fuschia.ie/', 'http://www.fuschia.ie/']

    start_multiprocess(start_url_list)


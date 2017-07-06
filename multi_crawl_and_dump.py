import multiprocessing
import subprocess
import csv
import platform


def start_crawl(arg):
    if platform.system() == 'Windows':
        cmd = 'python crawl_and_dump.py -index ' + str(arg)
    elif platform.system() == 'Linux':
        cmd = 'sudo python3 crawl_and_dump.py -index ' + str(arg)

    print(cmd)
    return subprocess.call(cmd, shell=True)


def start_multiprocess(l, num_workers=4, queue_csv="queue.csv"):
    out_row = [[x] for x in l]
    with open(queue_csv, 'w', encoding='utf-8', errors='ignore') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerows(out_row)

    args = list(range(len(l)))

    pool = multiprocessing.Pool(processes=num_workers)

    print(pool.map(start_crawl, args))


if __name__ == '__main__':
    start_url_list = ['http://chef5minutemeals.com', 'http://slapyamama.com']
    start_multiprocess(start_url_list, num_workers = 2)

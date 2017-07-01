import psutil
import subprocess
import time


def start_crawl():
    return subprocess.call('bash gpcrawler.sh', shell=True)


def stop_crawl():
    return subprocess.call('bash gpcrawler_kill.sh', shell=True)

if __name__ == '__main__':

    max_percent = 75
    min_free = 500000000
    wait_step = 3

    print("starting crawl")
    start_crawl()
    while True:

        free = psutil.virtual_memory().free
        percent = psutil.virtual_memory().percent

        print("free memory:",free,"-",100 - percent,"%")

        if free < min_free or percent > max_percent:
            print("not enough free memory, stopping process and restarting...")
            stop_crawl()
            time.sleep(1)
            start_crawl()

        time.sleep(wait_step)

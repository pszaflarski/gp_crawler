import psutil
import subprocess
import time


def reset_crawl():
    return subprocess.call('bash reset.sh', shell=True)


if __name__ == '__main__':

    max_percent = 75
    min_free = 500000000
    wait_step = 3

    while True:

        free = psutil.virtual_memory().free
        percent = psutil.virtual_memory().percent

        print("free memory:",free,"-",100 - percent,"%")

        if free < min_free or percent > max_percent:
            print("not enough free memory, stopping process and restarting...")
            reset_crawl()

        time.sleep(wait_step)


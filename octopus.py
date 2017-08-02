from common import *

from gp_crawler import *


class Octopus:
    def __init__(self, file_path=None, in_memory=False, s3_bucket=None, s3_cred_dict=None):
        self.driver = None
        self.base_url_format = "{scheme}://{netloc}"
        self.base_url_path_format = "{scheme}://{netloc}{path}"

        if file_path:
            self.file_path = file_path
        else:
            self.file_path = FILE_PATH

        self.in_memory = in_memory

        if s3_bucket is None:
            self.s3_bucket = S3_BUCKET
            self.s3_cred_dict = S3_CREDS
        else:
            self.s3_bucket = s3_bucket
            self.s3_cred_dict = s3_cred_dict

        self._progress_data_template = {
            'start_url': None,
            'to_visit': set(),
            'to_visit_low_priority': set(),
            'visited': set(),
            'last_activity': datetime.datetime.utcnow(),
            'state': 'in progress'
        }

        self.crawler = Crawler(
            file_path=self.file_path,
            in_memory=self.in_memory,
            s3_bucket=self.s3_bucket,
            s3_cred_dict=self.s3_cred_dict
        )

    def async_crawl(self, url_list):
        num_workers = 2
        self.crawler.async_crawl_sites(url_list, num_workers=num_workers)

    def vaccuum_files(self):
        if platform.system() == 'Windows':
            cmd = 'python db_connector.py'
        elif platform.system() == 'Linux':
            cmd = 'sudo python3 db_connector.py'

        subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    # clear cache


    o = Octopus()

    o.vaccuum_files()

    url_list = [
        'noballs.co.uk/',
        'movimentoapparel.com/',
        'veganrobs.com/',
        'getyuve.com/'
        'effifoods.com/',

        'vsstuff.com/' ,
        'mitzaccessories.com/',
        'nohikids.com/'
    ]

    # v = multiprocessing.Process(target=vaccuum_daemon)
    # v.daemon = True
    w = multiprocessing.Process(target=o.async_crawl, args=[url_list])
    # jobs = [v, w]

    # v.start()
    w.start()

    # w.join()
    # v.terminate()

    # clear cache again
    # vaccuum_files()


    free = psutil.virtual_memory().free
    percent = psutil.virtual_memory().percent

    while True:
        if w.is_alive():
            o.vaccuum_files()
        else:
            break
        time.sleep(5)

    o.vaccuum_files()

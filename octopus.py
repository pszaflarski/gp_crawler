from common import *

from gp_crawler import *
from db_connector import *


class Octopus:
    def __init__(self, db_connection=None, file_path=None, in_memory=False, s3_bucket=None, s3_cred_dict=None,
                 num_workers=3):
        self.driver = None
        self.base_url_format = "{scheme}://{netloc}"
        self.base_url_path_format = "{scheme}://{netloc}{path}"

        if db_connection:
            self.db_connection = db_connection
        else:
            self.db_connection = CONNECTION_STRING

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

    def async_crawl(self, url_list, num_workers):
        self.crawler.async_crawl_sites(url_list, num_workers=num_workers)

    def get_resume_file_from_s3(self, start_url):
        hashbase = sha256(fix_url(start_url).encode('utf-8')).hexdigest()
        filename = hashbase + '_progress.json'

        try:
            data = get_from_s3(filename, bucket=self.s3_bucket, cred_dict=self.s3_cred_dict)
        except:
            return
        with open(join(self.file_path, filename), 'w', encoding='utf-8', errors='ignore') as f:
            f.write(data)
            f.close()

    def get_resume_files_from_s3(self, start_urls, workers=3):
        pool = multiprocessing.Pool(processes=workers)
        pool.map(self.get_resume_file_from_s3, start_urls)

    def clear_cache(self):
        files = [join(self.file_path, x) for x in listdir(self.file_path)
                 if
                 isfile(join(self.file_path, x)) and (
                     len(x) == 64 or
                     'json' in x or
                     'pkl' in x)]

        do_remove = {x:remove_file(x) for x in files}
        return do_remove

    def vaccuum_files(self, d=None):
        if d is None:
            d = json.dumps({"db_connection": self.db_connection,
                            "file_path": self.file_path,
                            "s3_bucket": self.s3_bucket,
                            "s3_cred_dict": self.s3_cred_dict
                            })
        else:
            d = json.dumps(d)

        d = json.dumps({'': d})[4:-1]

        cmd = None
        if platform.system() == 'Windows':
            cmd = 'python db_connector_multiprocess_wrapper.py'
        elif platform.system() == 'Linux':
            cmd = 'sudo python3 db_connector_multiprocess_wrapper.py'

        if not cmd:
            return

        cmd += " -params " + d

        subprocess.call(cmd, shell=True)


if __name__ == '__main__':

    vaccuum_interval = 5
    max_percent = 90
    min_free = 500000000
    completed = False

    o = Octopus()
    o.vaccuum_files()
    o.clear_cache()


    url_list = set([
        'noballs.co.uk/',
        'movimentoapparel.com/',
        'veganrobs.com/',
        'getyuve.com/',
        'effifoods.com/',

        'vsstuff.com/',
        'mitzaccessories.com/',
        'nohikids.com/',

        'milochie.com/',
        'mitzaccessories.com/',
        'nohikids.com/',
        'ecocentricmom.com/',
        'getyuve.com/',
        'zaazee.co.uk/',
        'vsstuff.com/',
        'movimentoapparel.com/',
        'veganrobs.com/',
        'wodgearclothing.com/',
        'kiragrace.com/']
    )

    while True:

        # clear cache
        o.vaccuum_files()

        o.get_resume_files_from_s3(url_list)
        w = multiprocessing.Process(target=o.async_crawl, args=[url_list, 3])
        w.start()

        while True:
            if w.is_alive():
                vaccuum_timer = time.time()
                o.vaccuum_files()
            else:
                completed = True
                break

            free = psutil.virtual_memory().free
            percent = psutil.virtual_memory().percent

            if free < min_free or percent > max_percent:
                print("MEMORY TRIGGER! - restart")
                w.terminate()
                break

            elapsed = time.time() - vaccuum_timer
            time.sleep(max([vaccuum_interval - elapsed, 0]))

        if completed:
            break
        else:
            continue

    o.vaccuum_files()
    o.clear_cache()
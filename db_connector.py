from common import *

class CrawlerDataConnector:
    def __init__(self, db_connection='sqlite:///crawler.db', file_path='./cache/', s3_bucket=None):

        self.db_connection = db_connection
        self.file_path = file_path
        self.s3_bucket = s3_bucket

        self.db = create_engine(db_connection)
        self.db.echo = False

        self.metadata = MetaData(self.db)
        self.dialect = self.db_connection.split(':')[0]

        if self.dialect == 'sqlite':

            self.page_data = Table('page_data', self.metadata,
                                   Column('start_url', TEXT, index=True),
                                   Column('url', TEXT, index=True),
                                   Column('internal', TEXT),
                                   Column('external', TEXT),
                                   Column('non_http', TEXT),
                                   Column('page_source', TEXT, index=True),
                                   Column('exception', TEXT),
                                   Column('scraped_at', DATETIME, index = True),
                                   )

            self.progress_data = Table('progress_data', self.metadata,
                                       Column('start_url', TEXT, index=True),
                                       Column('progress_file', TEXT),
                                       Column('last_activity', DATETIME, index = True),
                                       Column('state', TEXT, index=True),
                                       )
        elif 'postgresql' in self.dialect:

            self.page_data = Table('page_data', self.metadata,
                                   Column('start_url', TEXT, index=True),
                                   Column('url', TEXT, index=True),
                                   Column('internal', JSONB),
                                   Column('external', JSONB),
                                   Column('page_source', TEXT, index=True),
                                   Column('exception', TEXT),
                                   Column('scraped_at', DATETIME, index = True),
                                   )

            self.progress_data = Table('progress_data', self.metadata,
                                       Column('start_url', TEXT, primary_key=True),
                                       Column('progress_file', TEXT),
                                       Column('last_activity', DATETIME, index = True),
                                       Column('state', TEXT, index=True),
                                       )

    def _create_page_data_table(self):
        self.page_data.create()

    def cache_to_db(self):

        files = [x for x in listdir(self.file_path) if isfile(join(self.file_path, x))]

        page_data_files = [join(self.file_path, x) for x in files if len(x) == 68 and '.pkl' in x]
        progress_data_files = [join(self.file_path, x) for x in files if len(x) == 77 and '.pkl' in x]

        def _remove_file(file):
            try:
                os.remove(file)
                return None
            except Exception as e:
                return e

        def _make_nice(x, to_db = True):
            if isinstance(x, set):
                x = list(x)
            elif isinstance(x, datetime.datetime):
                if to_db:
                    return x
                else:
                    return str(x)
            elif isinstance(x, str):
                return x
            if to_db:
                return json.dumps(x)
            else:
                return x

        page_data_out = []
        for file in page_data_files:
            data = {x: _make_nice(y) for x, y in pickle.load(open(file, 'rb')).items()}

            hashstring = str(file.split('/')[-1]).split('.')[0]

            source_html = data.get('page_source')
            data['page_source'] = hashstring

            self._save_file(hashstring, source_html)

            try:
                u = update(self.page_data)
                u = u.values({x:y for x,y in data.items() if x != 'page_source'})
                u = u.where(self.page_data.c.page_source==data['page_source'])
                r = self.db.execute(u)
                rc = r.rowcount
            except:
                rc =0

            if rc == 0:
                page_data_out.append(data)

        while True:
            errors = 0
            try:
                self.db.execute(self.page_data.insert(), page_data_out)
                break
            except:
                errors += 1
                if errors >= 3: raise Exception
                self._create_page_data_table()

        progress_data_out = []
        for file in progress_data_files:
            data = {x: y for x, y in pickle.load(open(file, 'rb')).items()}

            hashstring = str(file.split('/')[-1]).split('.')[0] + '.json'
            json_out = {x:_make_nice(y, to_db=False) for x, y in data.items()}

            self._save_file(hashstring, json.dumps(json_out))

            data.update({'progress_file': hashstring})

            try:
                u = update(self.progress_data)
                u = u.values({x: y for x, y in data.items() if x != 'progress_file'})
                u = u.where(self.page_data.c.page_source == data['progress_file'])
                r = self.db.execute(u)
                rc = r.rowcount
            except:
                rc = 0

            if rc == 0:
                progress_data_out.append(data)

        while True:
            errors = 0
            try:
                self.db.execute(self.progress_data.insert(), progress_data_out)
                break
            except:
                errors += 1
                if errors >= 3: raise Exception
                self._create_progress_data_table()

        return {
            'progress_data_files':progress_data_files,
            'page_data_files':page_data_files
        }


    def _save_file(self, file_name, file_contents):
        with open(join(self.file_path, file_name), 'w', encoding='utf-8', errors='ignore') as savefile:
            savefile.write(file_contents)

    def insert_page_data(self, page_data):
        i = self.page_data.insert()

        row = {
            'internal': page_data['internal'],
            'non_http': page_data['non_http'],
            'external': page_data['external'],
            'page_source': 'FFFFFFFFFFFFF',  # ,pd[''],
            'url': page_data['url'],
            'start_url': page_data['start_url'],
            'exception': None,
            'scraped_at': datetime.datetime.utcnow()

        }
        print(row)
        i.execute(row)

    def _create_progress_data_table(self):
        self.progress_data.create()

    def insert_progress_data(self, progress_data):
        i = self.page_data.insert()

        row = {
            'start_url': progress_data['start_url'],
            'to_visit': progress_data['to_visit'],
            'to_visit_low_priority': progress_data['to_visit_low_priority'],
            'visited': progress_data['visited'],
            'last_activity': progress_data['last_activity'],
            'state': progress_data['state'],
        }
        print(row)
        i.execute(row)


if __name__ == '__main__':
    cdc = CrawlerDataConnector()

    cdc.cache_to_db()

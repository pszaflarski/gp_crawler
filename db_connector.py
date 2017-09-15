from common import *


class CrawlerDataConnector:
    def __init__(self, db_connection=None, file_path=None, s3_bucket=None, s3_cred_dict=None):

        if db_connection:
            self.db_connection = db_connection
        else:
            self.db_connection = SECRET.CONNECTION_STRING

        if file_path:
            self.file_path = file_path
        else:
            self.file_path = SECRET.FILE_PATH

        if s3_bucket is None:
            self.s3_bucket = SECRET.S3_BUCKET
            self.s3_cred_dict = SECRET.S3_CREDS
        else:
            self.s3_bucket = s3_bucket
            self.s3_cred_dict = s3_cred_dict

        self.dialect = self.db_connection.split(':')[0]

        if self.dialect == 'sqlite':
            self.db = create_engine(self.db_connection)
            self.db.echo = False
            self.metadata = MetaData(self.db)

            self.page_data = Table(SECRET.PAGE_DATA_TABLE_NAME, self.metadata,
                                   Column('start_url', TEXT, index=True),
                                   Column('url', TEXT, index=True),
                                   Column('internal', TEXT),
                                   Column('external', TEXT),
                                   Column('non_http', TEXT),
                                   Column('page_source', TEXT, index=True),
                                   Column('exception', TEXT),
                                   Column('scraped_at', DATETIME, index=True)
                                   )

            self.progress_data = Table(SECRET.PROGRESS_DATA_TABLE_NAME, self.metadata,
                                       Column('start_url', TEXT, index=True),
                                       Column('progress_file', TEXT),
                                       Column('last_activity', DATETIME, index=True),
                                       Column('state', TEXT, index=True)
                                       )
        elif 'postgresql' in self.dialect:
            from sqlalchemy.dialects import postgresql

            self.db = create_engine(self.db_connection, isolation_level='AUTOCOMMIT')
            self.db.echo = False
            self.metadata = MetaData(self.db, schema=SECRET.SCHEMA)

            self.page_data = Table(SECRET.PAGE_DATA_TABLE_NAME, self.metadata,
                                   Column('start_url', postgresql.TEXT, index=True),
                                   Column('url', postgresql.TEXT, index=True),
                                   Column('internal', postgresql.JSONB),
                                   Column('external', postgresql.JSONB),
                                   Column('page_source', postgresql.TEXT, index=True),
                                   Column('exception', postgresql.TEXT),
                                   Column('scraped_at', postgresql.TIMESTAMP, index=True)
                                   )

            self.progress_data = Table(SECRET.PROGRESS_DATA_TABLE_NAME, self.metadata,
                                       Column('start_url', postgresql.TEXT, primary_key=True),
                                       Column('progress_file', postgresql.TEXT),
                                       Column('last_activity', postgresql.TIMESTAMP, index=True),
                                       Column('state', postgresql.TEXT, index=True)
                                       )

    def _create_page_data_table(self):
        self.page_data.create()

    def cache_to_db(self, delete_after_sync=True):

        files = [x for x in listdir(self.file_path) if isfile(join(self.file_path, x))]

        page_data_files = [join(self.file_path, x) for x in files if len(x) == 68 and '.pkl' in x]
        progress_data_files = [join(self.file_path, x) for x in files if len(x) == 77 and '.pkl' in x]

        def _make_nice(x, to_db=True):
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
        cols = {str(x).split('.')[-1] for x in self.page_data.columns}
        for file in page_data_files:
            data = {x: _make_nice(y) for x, y in pickle.load(open(file, 'rb')).items()}

            hashstring = str(file.split('/')[-1]).split('.')[0]

            source_html = data.get('page_source')
            data['page_source'] = hashstring

            self._save_file(hashstring, source_html)

            try:
                u = update(self.page_data)
                u = u.values({x: y for x, y in data.items() if x != 'page_source' and x in cols})
                u = u.where(self.page_data.c.page_source == data['page_source'])
                r = self.db.execute(u)
                rc = r.rowcount
            except:
                rc = 0

            if rc == 0:
                page_data_out.append(data)

        while True:
            errors = 0
            try:
                if page_data_out:
                    self.db.execute(self.page_data.insert(), page_data_out)
                break
            except:
                errors += 1
                if errors >= 3: raise Exception
                self._create_page_data_table()

        data = {}
        progress_data_out = []
        cols = {str(x).split('.')[-1] for x in self.progress_data.columns}

        for file in progress_data_files:
            try:
                data = {x: y for x, y in pickle.load(open(file, 'rb')).items()}
            except EOFError:
                continue

            hashstring = str(file.split('/')[-1]).split('.')[0] + '.json'
            json_out = {x: _make_nice(y, to_db=False) for x, y in data.items()}

            self._save_file(hashstring, json.dumps(json_out))

            data.update({'progress_file': hashstring})

            try:
                u = update(self.progress_data)
                u = u.values({x: y for x, y in data.items() if x != 'start_url' and x in cols})
                u = u.where(self.progress_data.c.start_url == data['start_url'])
                r = self.db.execute(u)
                rc = r.rowcount
            except:
                rc = 0

            if rc == 0:
                progress_data_out.append(data)

        while True:
            errors = 0
            try:
                if progress_data_out:
                    self.db.execute(self.progress_data.insert(), progress_data_out)
                break
            except:
                errors += 1
                if errors >= 3: raise Exception
                self._create_progress_data_table()

        if delete_after_sync:
            return {
                'progress_data_files': [remove_file(x) for x in progress_data_files],
                'page_data_files': [remove_file(x) for x in page_data_files]
            }
        else:
            return {}

    def _save_file(self, file_name, file_contents):

        if self.s3_bucket is None:
            with open(join(self.file_path, file_name), 'w', encoding='utf-8', errors='ignore') as savefile:
                savefile.write(file_contents)
        else:
            file_to_s3(file_name, file_contents, self.s3_bucket, self.s3_cred_dict)

    def _create_progress_data_table(self):
        self.progress_data.create()


if __name__ == '__main__':
    cdc = CrawlerDataConnector()

    cdc.cache_to_db()

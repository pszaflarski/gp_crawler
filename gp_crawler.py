from common import *


class Crawler:
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

    def crawl_one(self, page, start_url=None):
        """
        :param page: page to scrape
        :param start_url: if this is provided, it will classify all links not part of start_url as external
        :return: a dictionary with: {
            'internal': internal links
            'non_http': links that do not follow the http or https scheme
            'external': external links
            'page_source': the source for the page
            'url': the url that the page ended up at after it finished loading
            'exception': None if everything is OK, otherwise returns the exception
            }
        """

        # initialize values for error catching
        internal = set()
        non_http = set()
        external = set()
        source = ''
        current_url = page

        try:
            if start_url is None:
                # this will guess at what the start_url should be
                p = urlparse(page)
                if p.netloc == '':
                    p = urlparse('http://' + page)

                start_url = "{scheme}://{netloc}".format(scheme=p.scheme, netloc=p.netloc)

            self.driver = urlget(self.driver, page)

            source = page_source(self.driver)

            tree = etree_pipeline_fromstring(source)

            current_url = self.driver.current_url
            links = self._get_links(tree)

            external = {x for x in links if not self._is_internal(start_url, x)}
            internal = {x for x in links if x not in external}

            non_http = {x for x in internal if urlparse(x).scheme not in {'http', 'https', ''}}
            internal = {x for x in internal if x not in non_http}

            out_dict = {
                'internal': internal,
                'non_http': non_http,
                'external': external,
                'page_source': source,
                'url': current_url,
                'exception': None
            }
        except Exception as exc:
            out_dict = {
                'internal': internal,
                'non_http': non_http,
                'external': external,
                'page_source': source,
                'url': current_url,
                'exception': str(exc)
            }

        return out_dict

    def crawl_site(self, start_url, resume=True, resume_from=None, prioritize=True, max_site_size=20000, silent=False):
        """
        :param start_url: a valid address to a website
        :param resume: if True, it will attempt to resume from previously saved progress
        :param resume_from: pass resume data to the crawler so that you don't have to restart from the beginning
            this will override resume=False
        :param prioritize: if True, will prioritize urls with a path that that does not exist in visited and will
            deprioritize any url that has a path that matches any one that has already been crawled for example
            if 'google.com' has already been crawled, then 'google.com/?q=help' will be deprioritized
        :param max_site_size: limit for number of pages to crawl in a site, default is 20000, if it's -1, the crawler will
            not stop crawling until there are no pages left to crawl
        :param silent: if True, crawler will not print progress to screen as it crawls
        :return: Nothing
        """

        _start_url = fix_url(start_url)

        if resume_from is not None:
            progress_data = resume_from
        elif resume:
            progress_data = self._load_progress(_start_url)
        else:
            progress_data = {
                'start_url': _start_url,
                'to_visit': {_start_url},
                'to_visit_low_priority': set(),
                'visited': set(),
                'last_activity': datetime.datetime.utcnow(),
                'state': 'just started'
            }

        progress_data = self._repair_progress_data(progress_data)

        while True:

            if len(progress_data['visited']) >= max_site_size and len(progress_data['visited']) > 0:
                break

            try:
                url = progress_data['to_visit'].pop()
            except KeyError:
                if prioritize:
                    break
                try:
                    url = progress_data['to_visit_low_priority'].pop()
                except KeyError:
                    break

            # save progress here so that if you have an error with this page, you don't keep trying
            self._save_progress(progress_data)

            progress_data = self._organize_pd(url, progress_data)
            r = self.crawl_one(url)
            if r.get('exception') is not None:
                page_output = {
                    'internal': r.get('internal'),
                    'non_http': r.get('non_http'),
                    'external': r.get('external'),
                    'page_source': r.get('page_source'),
                    'url': url,
                    'start_url': _start_url,
                    'exception': r.get('exception'),
                    'scraped_at': datetime.datetime.utcnow()
                }
                self._save_page_data(page_output)
                continue

            # handle redirected urls
            redir_url = r['url']
            progress_data = self._organize_pd(redir_url, progress_data)

            # convert all internal links to absolute
            internal = [urljoin(url, x) for x in r['internal']]

            add_to_visit_staging = [x for x in internal if x not in progress_data['visited']
                                    and x not in progress_data['to_visit'] and x not in progress_data[
                                        'to_visit_low_priority']]

            # all links that whose path has already been visited and all links whose path has already been labelled
            # low priority will be added to low priority
            add_to_visit_lp = [x for x in add_to_visit_staging if
                               urlparse(x).netloc in progress_data['visited'] or
                               urlparse(x).netloc in progress_data['to_visit_low_priority']]

            # all other links need to be visited
            add_to_visit = [x for x in add_to_visit_staging if x not in add_to_visit_lp]
            del add_to_visit_staging

            # update the lists where you still have to visit
            progress_data['to_visit'].update(add_to_visit)
            progress_data['to_visit_low_priority'].update(add_to_visit_lp)
            progress_data['state'] = 'in progress'
            progress_data['last_activity'] = datetime.datetime.utcnow()

            page_output = {
                'internal': internal,
                'non_http': r.get('non_http'),
                'external': r.get('external'),
                'page_source': r.get('page_source'),
                'url': url,
                'start_url': _start_url,
                'exception': None,
                'scraped_at': datetime.datetime.utcnow()
            }

            self._save_page_data(page_output)

            if not silent:
                p = urlparse(url)
                base_url_path = self.base_url_path_format.format(scheme=p.scheme, netloc=p.netloc, path=p.path)
                printout = {x: len(y) for x, y in progress_data.items() if 'visit' in x}
                printout.update(
                    {'site_size': printout['visited'] + printout['to_visit'] + printout['to_visit_low_priority']})
                print(_start_url, {True: base_url_path, False: url}[prioritize], printout)

        progress_data['state'] = 'complete'
        success = self._save_progress(progress_data)

        close_webdriver(self.driver)

        if not silent:
            print(_start_url, 'All Done! saved -', success)

    def async_crawl_sites(self,
                          start_urls,
                          num_workers=2,
                          resume=True,
                          resume_from=None,
                          prioritize=True,
                          max_site_size=20000,
                          silent=False):

        def merge_dict(a, b):
            c = dict(a)
            c.update(b)
            return c

        args = {
            'resume': resume,
            'resume_from': resume_from,
            'prioritize': prioritize,
            'max_site_size': max_site_size,
            'silent': silent
        }

        d = [merge_dict({'start_url': x}, args) for x in start_urls]

        pool = multiprocessing.Pool(processes=num_workers)
        pool.map(self._crawl_site_packed, d)

    def _crawl_site_packed(self, d):
        self.crawl_site(**d)

    def _get_links(self, tree):
        e = tree.xpath("//*/@href")

        links_on_page = set([x for x in e])

        e = tree.xpath("//*/@src")

        links_on_page.update(set([x for x in e]))

        return links_on_page

    def _is_internal(self, base_url, url):

        try:
            p = urlparse(url)

            netloc_equal_condition = urlparse(base_url).netloc.replace('www.', '') == p.netloc.replace('www.', '')
            no_scheme_condition = p.scheme == '' and '.' not in p.netloc

            is_internal = netloc_equal_condition or no_scheme_condition
        except:
            is_internal = False

        return is_internal

    def _load_progress(self, start_url):

        def load_from_pickle(hashbase):
            try:
                filename = hashbase + '_progress.pkl'
                path_filename = os.path.join(self.file_path, filename)
                with open(path_filename, 'rb') as picklefile:
                    resume_data = pickle.load(picklefile)
                if resume_data['state'] == 'just started' or resume_data['visited'] == set():
                    resume_data['to_visit'] = {start_url}
            except FileNotFoundError:
                resume_data = {}

            return resume_data

        def load_from_json(hashbase):
            try:
                filename = hashbase + '_progress.json'
                path_filename = os.path.join(self.file_path, filename)

                with open(path_filename, 'r') as jsonfile:
                    file_source = jsonfile.read()
                resume_data = json.loads(file_source)
                if resume_data['state'] == 'just started' or resume_data['visited'] == set():
                    resume_data['to_visit'] = {start_url}
            except (FileNotFoundError, Exception):
                resume_data = {}

            return resume_data

        hashbase = sha256(start_url.encode('utf-8')).hexdigest()

        resume_data = load_from_pickle(hashbase)
        if resume_data == {}: resume_data = load_from_json(hashbase)
        if resume_data == {}:
            resume_data = {
                'start_url': start_url,
                'to_visit': {start_url},
                'to_visit_low_priority': set(),
                'visited': set(),
                'last_activity': datetime.datetime.utcnow(),
                'state': 'just started'
            }

        resume_data = self._repair_progress_data(resume_data)

        return resume_data

    def _repair_progress_data(self, pd):
        broken_keys = {x: y for x, y in pd.items() if x not in self._progress_data_template}
        return_dict = dict(pd)
        return_dict.update(broken_keys)

        return_dict['to_visit'] = set(return_dict['to_visit'])
        return_dict['to_visit_low_priority'] = set(return_dict['to_visit_low_priority'])
        return_dict['visited'] = set(return_dict['visited'])

        del pd
        return return_dict

    def _organize_pd(self, url, pd, reorg=True):

        pd = self._repair_progress_data(pd)

        start_url = pd.get('start_url')
        last_activity = pd.get('last_activity')
        state = pd.get('state')

        pd['visited'].add(url)

        p = urlparse(url)
        base_url_path = self.base_url_path_format.format(scheme=p.scheme, netloc=p.netloc, path=p.path)

        # add the base path to the low priority list if it hasn't already been visited
        if base_url_path != url and base_url_path not in pd['visited']:
            pd['to_visit_low_priority'].add(base_url_path)

        to_visit_lp_out = pd['to_visit_low_priority']

        # this will reorganize all 'to_visit' sites based on new information
        # if this is too process intensive, set reorg to False
        if reorg:
            to_visit_out = set()
            for to_visit_url in pd['to_visit']:
                p = urlparse(to_visit_url)
                if base_url_path == self.base_url_path_format.format(scheme=p.scheme, netloc=p.netloc, path=p.path):
                    to_visit_lp_out.add(to_visit_url)
                else:
                    to_visit_out.add(to_visit_url)
        else:
            to_visit_out = pd['to_visit']

        # this might be "dangerous" since we're swapping out things from a dictionary
        pd = {
            'start_url': start_url,
            'to_visit': to_visit_out,
            'to_visit_low_priority': to_visit_lp_out,
            'visited': pd['visited'],
            'last_activity': last_activity,
            'state': state
        }

        return pd

    def _save_page_data(self, page_data):
        success = True

        try:
            url = page_data.get('url')
            filename = os.path.join(self.file_path, sha256(url.encode('utf-8')).hexdigest() + '.pkl')
            with open(filename, 'w') as picklefile:
                pass
            picklefile = open(filename, 'wb')
            pickle.dump(page_data, picklefile)
            picklefile.close()
        except:
            success = False

        return success

    def _save_progress(self, resume_data):

        success = True
        try:
            start_url = resume_data.get('start_url')
            filename = os.path.join(self.file_path, sha256(start_url.encode('utf-8')).hexdigest() + '_progress.pkl')
            with open(filename, 'w') as picklefile:
                pass
            with open(filename, 'wb') as picklefile:
                pickle.dump(resume_data, picklefile)
        except:
            success = False

        return success


if __name__ == '__main__':
    c = Crawler()

    url_list = [
        'noballs.co.uk/',
        'movimentoapparel.com/',
        'veganrobs.com/',
        'getyuve.com/',
        'effifoods.com/'
    ]

    # c.async_crawl_sites(url_list)
    # c.crawl_site(url_list[4], resume=False)

    d = c.crawl_one('https://veganrobs.com/')
    d = c.crawl_one('https://veganrobs.com/products/cauliflower-puffs.oembed#shop')
    print(d.pop('page_source'))
    print(d)

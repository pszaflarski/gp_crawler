from selenium_wrapper import *
from db_connector import *

from urllib.parse import urljoin
from urllib.parse import urlparse
import pickle
from hashlib import sha256
from os import path


def get_links(tree, current_url):
    e = tree.xpath("//*/@href")

    links_on_page = set(rel_to_abs_l(current_url, [x for x in e]))

    e = tree.xpath("//*/@src")

    links_on_page.update(set(rel_to_abs_l(current_url, [x for x in e])))

    return links_on_page


def rel_to_abs_l(base_url, l):
    abs_links = []
    for link in l:
        if is_relative(link):
            abs_links.append(urljoin(base_url, link))
        else:
            abs_links.append(link)

    return abs_links


def is_relative(url):
    return ':' not in url


def is_internal(base_url, url):
    try:
        p = urlparse(url)

        netloc_equal_condition = urlparse(base_url).netloc == p.netloc
        no_scheme_condition = p.scheme == '' and '.' not in p.netloc

        is_internal = netloc_equal_condition or no_scheme_condition
    except:
        is_internal = False

    return is_internal


class Crawler:
    def __init__(self, db_connection='sqlite:///crawler.db', file_path='./cache/'):
        self.driver = None
        self.base_url_format = "{scheme}://{netloc}"
        self.base_url_path_format = "{scheme}://{netloc}{path}"

        self.db_connection = db_connection
        self.file_path = file_path

        self.dbc = CrawlerDataConnector(db_connection, file_path)

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
                start_url = "{scheme}://{netloc}".format(scheme=p.scheme, netloc=p.netloc)

            self.driver = urlget(self.driver, page)

            source = page_source(self.driver)

            tree = etree_pipeline_fromstring(source)

            current_url = self.driver.current_url
            links = get_links(tree, current_url)

            external = {x for x in links if not is_internal(start_url, x)}
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

    def _organize_pd(self, url, pd, reorg=True):

        start_url = pd.get('start_url')

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
            'visited': pd['visited']
        }

        return pd

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
        if resume_from is not None:
            pd = resume_from
        elif resume:
            pd = self._load_progress(start_url)
        else:
            pd = {
                'start_url': start_url,
                'to_visit': {start_url},
                'to_visit_low_priority': set(),
                'visited': set()
            }

        while True:
            try:
                url = pd['to_visit'].pop()
            except KeyError:
                if prioritize:
                    break
                try:
                    url = pd['to_visit_low_priority'].pop()
                except KeyError:
                    break

            # save progress here so that if you have an error with this page, you don't keep trying
            self._save_progress(pd)

            pd = self._organize_pd(url, pd)
            r = self.crawl_one(url)
            if r.get('exception') is not None:
                page_output = {
                    'internal': r.get('internal'),
                    'non_http': r.get('non_http'),
                    'external': r.get('external'),
                    'page_source': r.get('page_source'),
                    'url': url,
                    'start_url': start_url,
                    'exception': r.get('exception')
                }
                self._save_page_data(page_output)
                continue

            # handle redirected urls
            redir_url = r['url']
            pd = self._organize_pd(redir_url, pd)

            # convert all internal links to absolute
            internal = [urljoin(url, x) for x in r['internal']]

            add_to_visit_staging = [x for x in internal if x not in pd['visited']
                                    and x not in pd['to_visit'] and x not in pd['to_visit_low_priority']]

            # all links that whose path has already been visited and all links whose path has already been labelled
            # low priority will be added to low priority
            add_to_visit_lp = [x for x in add_to_visit_staging if
                               urlparse(x).netloc in pd['visited'] or
                               urlparse(x).netloc in pd['to_visit_low_priority']]

            # all other links need to be visited
            add_to_visit = [x for x in add_to_visit_staging if x not in add_to_visit_lp]
            del add_to_visit_staging

            # update the lists where you still have to visit
            pd['to_visit'].update(add_to_visit)
            pd['to_visit_low_priority'].update(add_to_visit_lp)

            page_output = {
                'internal': internal,
                'non_http': r.get('non_http'),
                'external': r.get('external'),
                'page_source': r.get('page_source'),
                'url': url,
                'start_url': start_url,
                'exception': None
            }

            self._save_page_data(page_output)

            if not silent:
                p = urlparse(url)
                base_url_path = self.base_url_path_format.format(scheme=p.scheme, netloc=p.netloc, path=p.path)
                print(start_url, {True: base_url_path, False: url}[prioritize],
                      {x: len(y) for x, y in pd.items() if x != 'start_url'})

        if not silent:
            print('All Done!')

    def _save_page_data(self, page_data):
        success = True

        try:
            url = page_data.get('url')
            filename = path.join(self.file_path, sha256(url.encode('utf-8')).hexdigest() + '.pkl')
            with open(filename, 'w') as picklefile:
                pass
            with open(filename, 'wb') as picklefile:
                pickle.dump(page_data, picklefile)
        except:
            success = False

        return success

    def _save_progress(self, resume_data):
        success = True
        try:
            start_url = resume_data.get('start_url')
            filename = path.join(self.file_path, sha256(start_url.encode('utf-8')).hexdigest() + '_progress.pkl')
            with open(filename, 'w') as picklefile:
                pass
            with open(filename, 'wb') as picklefile:
                pickle.dump(resume_data, picklefile)
        except:
            success = False

        return success

    def _load_progress(self, start_url):

        try:
            filename = path.join(self.file_path, sha256(start_url.encode('utf-8')).hexdigest() + '_progress.pkl')
            with open(filename, 'rb') as picklefile:
                resume_data = pickle.load(picklefile)
        except:
            resume_data = {
                'start_url': start_url,
                'to_visit': {start_url},
                'to_visit_low_priority': set(),
                'visited': set()
            }

        return resume_data


if __name__ == '__main__':
    c = Crawler()

    # d = c._load_progress('https://soredgear.com/')
    #
    # print(d)

    c.crawl_site('https://soredgear.com/')

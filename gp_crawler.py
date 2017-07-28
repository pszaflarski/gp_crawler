"""
The plan is to make this module and object a container for all the variables in this project.
Once finished, a user should be able to just use this module to do all their crawling and scraping
without needing to call any of the other modules in this project

"""

from selenium_wrapper import *

from urllib.parse import urljoin
from urllib.parse import urlparse


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
        no_scheme_condition = p.scheme == ''

        is_internal = netloc_equal_condition or no_scheme_condition
    except:
        is_internal = False

    return is_internal


class Crawler:
    def __init__(self):
        self.driver = None

    def crawl_one(self, page, start_url=None):
        """
        :param page: page to scrape
        :param start_url: if this is provided, it will classify all links not part of start_url as external
        :return: a dictionary with:
            'internal': internal links
            'non_http': links that do not follow the http or https scheme
            'external': external links
            'page_source': the source for the page
            'url': the url that the page ended up at after it finished loading
            'exception': None if everything is OK, otherwise returns the exception
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

            self.driver = get(self.driver, page)

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

    def crawl_site(self, start_url, resume=True, prioritize=True, max_site_size=20000):
        """
        :param start_url: a valid address to a website
        :param resume: if True, it will attempt to resume from previously saved progress
        :param prioritize: if True, will prioritize urls with a path that that does not exist in visited and will
            deprioritize any url that has a path that matches any one that has already been crawled for example
            if 'google.com' has already been crawled, then 'google.com/?q=help' will be deprioritized
        :param max_site_size: limit for number of pages to crawl in a site, default is 20000, if it's -1, the crawler will
            not stop crawling until there are no pages left to crawl
        :return: Nothing
        """

        base_url_format = "{scheme}://{netloc}"
        base_url_path_format = "{scheme}://{netloc}{path}"

        if resume:
            pd = self._load_progress(start_url)
        else:
            pd = {
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
            resume_output = {x: list(y) for x, y in pd.items()}
            self._save_progress(resume_output)

            p = urlparse(url)
            base_url_path = base_url_path_format.format(scheme=p.scheme, netloc=p.netloc, path=p.path)

            pd['visited'].add(url)

            # add the base path to the low priority list if it hasn't already been visited
            if base_url_path != url and base_url_path not in pd['visited']:
                pd['to_visit_low_priority'].add(base_url_path)

            r = self.crawl_one(url)
            if r.get('exception') is None:
                page_output = {
                    'internal': r.get('internal'),
                    'non_http': r.get('non_http'),
                    'external': r.get('external'),
                    'page_source': r.get('page_source'),
                    'url': url,
                    'start_url': start_url
                }



            # handle redirected urls
            redir_url = r['url']
            pd['visited'].add(redir_url)
            pr = urlparse(redir_url)
            redir_url_base_path = base_url_path_format.format(scheme=pr.scheme, netloc=pr.netloc, path=pr.path)

            # add the base path for the redirected link to the low priority list if it hasn't already been visited
            if redir_url_base_path != redir_url and redir_url_base_path not in pd['visited']:
                pd['to_visit_low_priority'].add(redir_url_base_path)

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
                'start_url': start_url
            }

    def _save_progress(self, resume_data):
        pass

    def _load_progress(self, start_url):
        """
        :param start_url: the website that you are crawling
        :return: a dictionary with {
            'to_visit': set of pages that you still have to visit,
            'to_visit_low_priority': set of pages that you still have to visit, but are low priority,
            'visited': set of pages that have already been visited
        }
        """
        return {
            'to_visit': {start_url},
            'to_visit_low_priority': set(),
            'visited': set()
        }


if __name__ == '__main__':
    c = Crawler()

    d = c.crawl_site('https://soredgear.com/')

    e = 'mailto://pjacob@hubba.com'
    u = 'https://soredgear.com/'

    j = urljoin(u, e)

    print(j)

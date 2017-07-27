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
        no_scheme_condition = p.scheme is None or p.scheme == ''

        is_internal = netloc_equal_condition or no_scheme_condition
    except:
        is_internal = False

    return is_internal


class Crawler:
    def __init__(self):
        # common info
        s3_creds = {}
        s3_bucket = ""
        postgres_creds = {}
        postgres_path = ""
        postgres_resume_path = ""

        # crawl variables
        num_crawler_workers = 4
        max_crawl_per_site = 10000

        # memory watcher variables
        max_percent = 75
        min_free = 500000000
        wait_step = 3
        session_name = "work"

        # sync/scrape variables
        num_sync_workders = 10
        num_scrape_workers = 10
        cache_folder = 'cache'
        cache_file = cache_folder + "\\cachemap.csv"
        scrape_strategy = {}

        # webdriver
        self.driver = None

    def crawl_one(self, page, start_url = None):
        """ <-- this is so much better, use this!!

        :param page:
        :param start_url:
        :return:
        """
        # takes: a valid address to a webpage
        # returns: a dictionary with:
        #   'internal': internal links
        #   'external': external links
        #   'page_source': the source for the page
        #   'url': the url that the page ended up at after it finished loading

        if start_url is None:
            # this will guess at what the start_url should be
            p = urlparse(page)
            start_url = "{scheme}://{netloc}".format(scheme = p.scheme, netloc = p.netloc)

        self.driver = get(self.driver, page)

        source = page_source(self.driver)

        tree = etree_pipeline_fromstring(source)

        current_url = self.driver.current_url
        links = get_links(tree, current_url)

        external = set([x for x in links if not is_internal(start_url, x)])
        internal = set([urljoin(start_url,x) for x in links if x not in external])

        out_dict = {
            'internal': internal,
            'external': external,
            'page_source': source,
            'url': current_url
        }

        return  out_dict

    def crawl_site(self, site, resume = True, prioritize = True, max_site_size = 20000):
        # takes: a valid address to a website
        # returns: Nothing
        # requies: a database and file system to store source files and page data
        # OPTIONAL PARAMETERS:
        # resume: if True, uses the database to find out where the crawler left off, and continues from there
        # prioritize: if True, will prioritize urls with a path that that does not exist in visited and will
        #   deprioritize any url that has a path that matches any one that has already been crawled for example
        #   if 'google.com' has already been crawled, then 'google.com/?q=help' will be deprioritized
        # max_site_size: limit for number of pages to crawl in a site, default is 20000, if it's -1, the crawler will
        #   not stop crawling until there are no pages left to crawl

        base_url_format = "{scheme}://{netloc}"
        base_url_path_format = "{scheme}://{netloc}/{path}"

        progress_dict = {
            'to_visit': {site},
            'to_visit_low_priority': set(),
            'visited': set()
        }

        url = progress_dict['to_visit'].pop()

        p = urlparse(url)
        base_url_path = base_url_path_format.format(scheme = p.scheme, netloc = p.netloc, path = p.path)



        r = self.crawl_one(url)



        print(r['internal'])

        print(progress_dict)















if __name__ == '__main__':

    c = Crawler()

    d = c.crawl_site('https://soredgear.com/')


    e = 'mailto://pjacob@hubba.com'
    u = 'https://soredgear.com/'

    j = urljoin(u,e)

    print(j)



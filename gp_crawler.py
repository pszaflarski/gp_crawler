"""
The plan is to make this module and object a container for all the variables in this project.
Once finished, a user should be able to just use this module to do all their crawling and scraping
without needing to call any of the other modules in this project

"""

from selenium_wrapper import *


from urllib.parse import urljoin

def get_links(tree, current_url):
    e = tree.xpath("//a/@href")

    links_on_page = set(rel_to_abs_l(current_url, [x for x in e]))

    e = tree.xpath("//*[contains(@src,'.htm')]/@src")

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
        internal = {i:x for i,x in enumerate(base_url.split('/'))}.get(2,base_url)
        test_domain = {i:x for i,x in enumerate(url.split('/'))}.get(2,url)
        is_internal = (internal in test_domain) or (test_domain in internal)
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
        # takes: a valid address to a webpage
        # returns: a dictionary with:
        #   'internal': internal links
        #   'external': external links
        #   'page_source': the source for the page
        #   'url': the url that the page ended up at after it finished loading

        if start_url is None:
            # this will guess at what the start url should be
            start_url = page.split('/')[2]

        print(start_url)

        self.driver = get(self.driver, page)

        source = page_source(self.driver)

        tree = etree_pipeline_fromstring(source)

        current_url = self.driver.current_url
        links = get_links(tree, current_url)

        external = set([x for x in links if not is_internal(start_url, x)])
        internal = set([x for x in links if x not in external])

        out_dict = {
            'internal': internal,
            'external': external,
            'page_source': source,
            'url': current_url
        }

        return  out_dict






    def crawl_site(self, site):
        # takes: a valid address to a website
        # returns: Nothing
        # requies: a database and file system to store source files and page data
        pass


if __name__ == '__main__':
    c = Crawler()

    d = c.crawl_one('http://www.google.ca')

    print(d)

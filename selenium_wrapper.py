"""
The plan is to make this module and object a container for all the variables in this project.
Once finished, a user should be able to just use this module to do all their crawling and scraping
without needing to call any of the other modules in this project

"""

import selenium_wrapper


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

    def crawl_one(self, page):
        # takes: a valid address to a webpage
        # returns: a dictionary with:
        #   'internal': internal links
        #   'external': external links
        #   'page_source': the source for the page
        #   'url': the url that the page ended up at after it finished loading
        pass

    def crawl_site(self, site):
        # takes: a valid address to a website
        # returns: Nothing
        # requies: a database and file system to store source files and page data
        pass


if __name__ == '__main__':
    pass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import selenium.common.exceptions as selenium_exceptions
from selenium.webdriver.chrome.options import Options

import os
import time
import csv
import json
import datetime

from lxml import html
from lxml import etree
from html import escape

import pickle


def driver_page_source_plus(driver):
    try:
        a = driver.find_elements_by_xpath('/*')
        return '\n'.join([x.get_attribute('outerHTML') for x in a])
    except:
        return driver.page_source


def to_number(s):
    try:
        sl = s.lower().replace(',', '').replace(' ', '')
        if 'k' in sl:
            mult = 1000
        elif 'm' in sl:
            mult = 1000000
        else:
            mult = 1

        r = int(float(sl.replace('m', '').replace('k', '')) * mult)
        return r
    except:
        return None


def init_webdriver():
    chromedriver = "chromedriver.exe"
    os.environ["webdriver.chrome.driver"] = chromedriver

    chrome_options = Options()
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("test-type")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--js-flags=--expose-gc")
    chrome_options.add_argument("--enable-precise-memory-info")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("test-type=browser")
    chrome_options.add_argument("disable-infobars")

    driver = webdriver.Chrome(chromedriver, chrome_options=chrome_options)

    return driver


def load_creds(filename):
    return json.load(open(filename, 'r', encoding='utf-8', errors='ignore'))


def etree_pipeline(driver):
    source = driver_page_source_plus(driver)
    tree = etree_pipeline_fromstring(source)

    return tree


def etree_pipeline_fromstring(source):
    parser = etree.HTMLParser(encoding='utf-8')
    u = source.encode()
    tree = etree.fromstring(u, parser=parser)
    return tree


def login_to_facebook(driver, email, password):
    driver.get("https://www.facebook.com/")
    a = driver.find_element_by_id("email").click()
    ActionChains(driver).send_keys(email).perform()
    driver.find_element_by_id("pass").click()
    ActionChains(driver).send_keys(password).perform()
    ActionChains(driver).send_keys(Keys.ENTER).perform()
    time.sleep(1)


def login_to_instagram(driver):
    # requires that you are logged into facebook

    driver.get("https://www.instagram.com/accounts/login/")

    a = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'Log in as')]")))
    a.click()

    time.sleep(2)


def clean_handle(handle):
    clean = handle.split('/')[-1].replace('@', '').lower()
    return clean


def write_dict_to_csv(filename, fieldnames, d=None, mode='a'):
    if mode == 'w':
        f = open(filename, 'w', encoding="utf-8", errors="ignore")
        writer = csv.DictWriter(f,
                                lineterminator="\n",
                                fieldnames=fieldnames)
        writer.writeheader()
        f.close()
    if d is not None:
        f = open(filename, 'a', encoding="utf-8", errors="ignore")
        writer = csv.DictWriter(f,
                                lineterminator="\n",
                                fieldnames=fieldnames)
        writer.writerow(d)
        f.close()


if __name__ == '__main__':
    pass

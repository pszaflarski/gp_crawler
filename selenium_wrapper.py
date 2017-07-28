import os
import time
import csv
import json
import datetime
import platform
import subprocess

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import selenium.common.exceptions as selenium_exceptions
from selenium.webdriver.chrome.options import Options

from lxml import html
from lxml import etree
from html import escape

import boto3

from openpyxl import Workbook


def get(driver, url, tries=1):
    if driver is None: driver = init_webdriver()

    for i in range(tries):
        try:
            driver.get(url)
            break
        except:
            driver.quit()
            del driver
            driver = init_webdriver()
            continue

    return driver


def page_source(driver):
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


def init_webdriver(headless=True):
    if platform.system() == 'Windows':
        chromedriver = "chromedriver.exe"
    elif platform.system() == 'Linux':
        subprocess.call('sudo chmod +x ./chromedriver', shell=True)
        chromedriver = "./chromedriver"

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
    if headless:
        chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(chromedriver, chrome_options=chrome_options)

    return driver


def get_from_s3(sourcefile_name, bucket, cred_dict):
    aws_access_key_id = cred_dict['aws_access_key_id']
    aws_secret_access_key = cred_dict['aws_secret_access_key']

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    ob = s3.Object(bucket, sourcefile_name)
    return ob.get()["Body"].read().decode('utf-8')


def file_to_s3(filename, file_data, bucket, cred_dict):
    aws_access_key_id = cred_dict['aws_access_key_id']
    aws_secret_access_key = cred_dict['aws_secret_access_key']

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    ob = s3.Object(bucket, filename)
    return ob.put(Body=file_data)


def delete_s3_file(filename, bucket, cred_dict):
    aws_access_key_id = cred_dict['aws_access_key_id']
    aws_secret_access_key = cred_dict['aws_secret_access_key']

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    ob = s3.Object(bucket, filename)
    return ob.delete()


def load_creds(filename):
    return json.load(open(filename, 'r', encoding='utf-8', errors='ignore'))


def etree_pipeline(driver):
    source = page_source(driver)
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


def csv_to_xl(csv_file_source, xlsx_filename, encoding='utf-8'):
    wb = Workbook(write_only=True)
    ws = wb.create_sheet()

    with open(csv_file_source, 'r', encoding=encoding, errors='ignore') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            ws.append(row)

    wb.save(xlsx_filename)


if __name__ == '__main__':
    pass

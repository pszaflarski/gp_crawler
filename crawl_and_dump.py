from general_tools import *

from urllib.parse import urljoin

from hashlib import sha256
import argparse

import psycopg2
import boto3


def is_relative(url):
    return ':' not in url


def is_internal(base_url, url):
    try:
        internal = base_url.split('/')[2]
        test_domain = url.split('/')[2]
        is_internal = (internal in test_domain) or (test_domain in internal)
    except:
        is_internal = False

    return is_internal


def rel_to_abs_l(base_url, l):
    abs_links = []
    for link in l:
        if is_relative(link):
            abs_links.append(urljoin(base_url, link))
        else:
            abs_links.append(link)

    return abs_links


def get_links(tree, current_url):
    e = tree.xpath("//a/@href")

    links_on_page = set(rel_to_abs_l(current_url, [x for x in e]))

    e = tree.xpath("//*[contains(@src,'.htm')]/@src")

    links_on_page.update(set(rel_to_abs_l(current_url, [x for x in e])))

    return links_on_page


def store_postgres(table, data, creds):
    conn = psycopg2.connect(**creds)
    cur = conn.cursor()

    cur.execute("""INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s);""".format(table), data)

    conn.commit()

    conn.close()


def check_resume_data(start_url, creds):
    s3_creds = creds['s3']
    s3_bucket = creds['s3_bucket']

    filename = sha256(start_url.encode('utf-8')).hexdigest() + '_resume.json'

    try:
        ob = json.loads(get_from_s3(filename, s3_bucket, s3_creds))
    except Exception as e:
        ob = {'visited': set(), 'to_visit': {start_url}}

    return (set(ob['visited']), set(ob['to_visit']))


def update_resume_data(data, creds):
    postgres_creds = creds['postgres']
    table = creds['postgres_resume_path']
    s3_creds = creds['s3']
    s3_bucket = creds['s3_bucket']

    start_url = data[0]

    hashfile = sha256(start_url.encode('utf-8')).hexdigest()
    filename = hashfile + '_resume.json'

    row = data[:3] + [hashfile]
    format_data = row + row[1:]

    conn = psycopg2.connect(**postgres_creds)
    cur = conn.cursor()

    s = cur.mogrify("""
    insert into {} values (%s,%s,%s,%s)
    on conflict (start_url) do
    update set (completed,received_at,hashfile) = (%s,%s,%s);
    """.format(table), format_data)

    cur.execute(s)
    conn.commit()
    conn.close()

    if data[-1] is None and data[-2] is None:
        delete_s3_file(filename, s3_bucket, s3_creds)
    else:
        file_data = json.dumps({'visited': data[-2], 'to_visit': data[-1]})
        file_to_s3(filename, file_data, s3_bucket, s3_creds)


def store_data(start_url, url, internal, external, source, scraped_at, creds):
    s3_creds = creds['s3']
    postgres_creds = creds['postgres']
    table = creds['postgres_path']
    bucket = creds['s3_bucket']

    hashfile = sha256(url.encode('utf-8')).hexdigest()

    file_to_s3(hashfile, source.encode('utf-8'), bucket, s3_creds)

    l = [start_url, url, list(internal), list(external), hashfile, scraped_at]

    store_postgres(table, l, postgres_creds)


def main(start_url, max_visited=10000):
    creds = load_creds("credentials.json")
    r = check_resume_data(start_url, creds)

    visited = r[0]
    to_visit = r[1]
    visited_count = len(visited)

    driver = init_webdriver()

    while True:
        # update the resume table with resume information
        resume_list = [start_url, False, datetime.datetime.utcnow(), list(visited), list(to_visit)]
        update_resume_data(resume_list, creds)

        try:
            next_url = to_visit.pop()
        except KeyError:
            break

        visited.add(next_url)
        visited_count += 1

        try:
            driver.get(next_url)
        except Exception as e:
            # attempt to store a fail as this - let's make this better soon
            store_data(start_url, next_url, [], [], "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                       datetime.datetime.utcnow(), creds)
            continue

        current_url = driver.current_url
        visited.add(current_url)

        tree = etree_pipeline(driver)

        links = get_links(tree, current_url)

        external = set([x for x in links if not is_internal(start_url, x)])
        internal = set([x for x in links if x not in external])
        add_to_visit = [x for x in internal if x not in visited and x not in to_visit]

        page_source = driver_page_source_plus(driver)

        to_visit.update(add_to_visit)

        store_data(start_url, next_url, internal, external, page_source, datetime.datetime.utcnow(), creds)

        if len(to_visit) == 0 or (len(visited) >= max_visited and max_visited != -1):
            break

        print(start_url, "- to visit:", len(to_visit), ", visited:", visited_count)

    # update the resume table with completed information
    if len(to_visit) == 0:
        resume_list = [start_url, True, datetime.datetime.utcnow(), None, None]
    else:
        resume_list = [start_url, True, datetime.datetime.utcnow(), list(visited), list(to_visit)]

    update_resume_data(resume_list, creds)

    driver.quit()


def main_multiprocess(index, queue_csv="queue.csv"):
    reader = csv.reader(open(queue_csv, 'r', encoding='utf-8', errors='ignore'))
    i = int(index)
    start_url = [x[0] for x in reader][i]
    main(start_url)


if __name__ == '__main__':
    # creds = load_creds("credentials.json")
    # start_url = 'http://naturesbeautymix.com'
    #
    # r = check_resume_data(start_url, creds)
    #
    # print(r)

    parser = argparse.ArgumentParser()
    parser.add_argument('-index', action='store')
    args = parser.parse_args()
    i = args.index

    if i is None:
        start_url = 'https://www.lollaland.com/'
        main(start_url)
    else:
        main_multiprocess(i)

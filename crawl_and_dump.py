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


def check_resume_table(table, start_url, creds):
    conn = psycopg2.connect(**creds)
    cur = conn.cursor()

    s = cur.mogrify("""
        select * from {} where start_url = %s
        """.format(table), [start_url])

    cur.execute(s)
    try:
        results = cur.fetchall()[0][-2:]
        results = [set(results[0]), set(results[1])]
    except IndexError:
        results = [set(), {start_url}]

    return results


def update_resume_table(table, data, creds):
    conn = psycopg2.connect(**creds)
    cur = conn.cursor()

    data = data + data[1:]

    s = cur.mogrify("""
    insert into {} values (%s,%s,%s,%s,%s)
    on conflict (start_url) do
    update set (completed,received_at,visited,to_visit) = (%s,%s,%s,%s);
    """.format(table), data)

    cur.execute(s)
    conn.commit()
    conn.close()


def sourcefile_to_s3(sourcefile_name, source, bucket, cred_dict):
    bin_data = source.encode('utf-8')

    aws_access_key_id = cred_dict['aws_access_key_id']
    aws_secret_access_key = cred_dict['aws_secret_access_key']

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    ob = s3.Object(bucket, sourcefile_name)
    ob.put(Body=bin_data)


def store_data(start_url, url, internal, external, source, scraped_at, creds):
    hashfile = sha256(url.encode('utf-8')).hexdigest()
    # f = open(hashfile, 'w', encoding='utf-8', errors='ignore')
    # f.write(source)
    # f.close()

    s3_creds = creds['s3']
    postgres_creds = creds['postgres']
    table = creds['postgres_path']
    bucket = creds['s3_bucket']

    sourcefile_to_s3(hashfile, source, bucket, s3_creds)

    l = [start_url, url, list(internal), list(external), hashfile, scraped_at]

    store_postgres(table, l, postgres_creds)


def main(start_url):
    creds = load_creds("credentials.json")
    r = check_resume_table(creds["postgres_resume_path"], start_url, creds['postgres'])

    visited = r[0]
    to_visit = r[1]
    visited_count = len(visited)

    driver = init_webdriver()

    while True:
        # update the resume table with resume information
        resume_list = [start_url, False, datetime.datetime.utcnow(), list(visited), list(to_visit)]
        update_resume_table(creds["postgres_resume_path"], resume_list, creds['postgres'])

        next_url = to_visit.pop()
        visited.add(next_url)
        visited_count += 1

        driver.get(next_url)

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

        if len(to_visit) == 0: break

        print(start_url, "- to visit:", len(to_visit), ", visited:", visited_count)

    # update the resume table with completed information
    resume_list = [start_url, True, datetime.datetime.utcnow(), None, None]
    update_resume_table(creds["postgres_resume_path"], resume_list, creds['postgres'])

    driver.quit()


def main_multiprocess(index, queue_csv="queue.csv"):
    reader = csv.reader(open(queue_csv, 'r', encoding='utf-8', errors='ignore'))
    i = int(index)
    start_url = [x[0] for x in reader][i]
    main(start_url)


if __name__ == '__main__':
    # creds = load_creds("credentials.json")
    # start_url = 'http://hubbarulez.com'
    #
    # r = check_resume_table(creds["postgres_resume_path"], start_url, creds['postgres'])
    #
    # print(r)

    parser = argparse.ArgumentParser()
    parser.add_argument('-index', action='store')
    args = parser.parse_args()
    i = args.index

    if i is None:
        start_url = 'http://www.fuschia.ie/'
        main(start_url)
    else:
        main_multiprocess(i)

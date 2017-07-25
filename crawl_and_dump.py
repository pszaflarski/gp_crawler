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
        ob = {'to_visit': {start_url}}

    return ob


def update_resume_data(data, creds):
    postgres_creds = creds['postgres']
    table = creds['postgres_resume_path']
    s3_creds = creds['s3']
    s3_bucket = creds['s3_bucket']

    start_url = data[0]
    visit_data = data[-1]

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

    if visit_data == {}:
        delete_s3_file(filename, s3_bucket, s3_creds)
    else:
        file_data = json.dumps(visit_data)
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


def get_base_url(url, parameters_set):
    if parameters_set is None: parameters_set = set()
    parameters_set = set(parameters_set)

    url_out = url
    for parameter in parameters_set:
        url_out = url_out.split(parameter)[0]

    return url_out


def sort_visited(visited_dict, parameter_set):
    visited = set(visited_dict.get('visited', set()))
    to_visit = set(visited_dict.get('to_visit', set()))
    to_visit_lp = set(visited_dict.get('to_visit_lp', set()))

    to_visit.update(to_visit_lp)

    visited_out = set()
    to_visit_out = set()
    to_visit_lp_out = set()

    for url in visited:
        visited_out.update({url, get_base_url(url, parameter_set)})

    for url in to_visit:
        if get_base_url(url, parameter_set) in visited_out:
            to_visit_lp_out.add(url)
        else:
            to_visit_out.add(url)

    return {
        'visited': visited_out,
        'to_visit': to_visit_out,
        'to_visit_lp': to_visit_lp_out
    }


def main(start_url, max_visited=-1, parameters_set=None, high_priority=True):
    creds = load_creds("credentials.json")
    r = check_resume_data(start_url, creds)

    r = sort_visited(r, parameters_set)

    visited = set(r.get('visited', set()))
    to_visit = set(r.get('to_visit', set()))
    to_visit_lp = set(r.get('to_visit_lp', set()))

    visited_count = len(visited)
    if parameters_set is None: parameters_set = set()

    driver = init_webdriver()

    while True:
        # update the resume table with resume information
        resume_list = [start_url, False, datetime.datetime.utcnow(),
                       {'visited': list(visited), 'to_visit': list(to_visit), 'to_visit_lp': list(to_visit_lp)}]
        update_resume_data(resume_list, creds)

        try:
            next_url = to_visit.pop()
        except KeyError:
            if high_priority:
                break
            try:
                next_url = to_visit_lp.pop()
            except KeyError:
                break

        visited.update({next_url, get_base_url(next_url, parameters_set)})
        visited_count += 1

        try:
            # we technically haven't visited the base url yet
            driver.get(next_url)
            current_url = driver.current_url
            visited.update({current_url, get_base_url(current_url, parameters_set)})

            tree = etree_pipeline(driver)
        except Exception as e:

            # attempt to store a fail as this - let's make this better soon
            store_data(start_url, next_url, [], [], "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
                       datetime.datetime.utcnow(), creds)
            continue

        links = get_links(tree, current_url)

        external = set([x for x in links if not is_internal(start_url, x)])
        internal = set([x for x in links if x not in external])

        add_to_visit_staging = [x for x in internal if x not in visited and x not in to_visit and x not in to_visit_lp]
        add_to_visit_lp = set([x for x in add_to_visit_staging if get_base_url(x, parameters_set) in visited])
        add_to_visit = set([x for x in add_to_visit_staging if x not in add_to_visit_lp])

        del add_to_visit_staging

        page_source = driver_page_source_plus(driver)

        to_visit.update(add_to_visit)
        to_visit_lp.update(add_to_visit_lp)

        store_data(start_url, next_url, internal, external, page_source, datetime.datetime.utcnow(), creds)

        high_priority_condition = len(to_visit) == 0
        max_visited_condition = (len(visited) >= max_visited and max_visited != -1)
        general_condition = len(to_visit) + len(to_visit_lp) == 0

        if high_priority and high_priority_condition or max_visited and max_visited_condition or general_condition:
            break

        print(start_url, "- high priority to visit:", len(to_visit), "- low priority to visit:", len(to_visit_lp), ", visited:", visited_count)
        print(to_visit)

    # update the resume table with completed information
    if len(to_visit) + len(to_visit_lp) == 0:
        resume_list = [start_url, True, datetime.datetime.utcnow(), {}]
    else:
        resume_list = [start_url, True, datetime.datetime.utcnow(),
                       {'visited': list(visited), 'to_visit': list(to_visit), 'to_visit_lp': list(to_visit_lp)}]

    update_resume_data(resume_list, creds)

    driver.quit()


def main_multiprocess(args_tuple):
    # args_tuple should have: index, queue_csv, parameters_set

    index = args_tuple[0]
    max_visited = args_tuple[1]
    queue_csv = args_tuple[2]
    parameters_set = args_tuple[3]

    reader = csv.reader(open(queue_csv, 'r', encoding='utf-8', errors='ignore'))
    i = int(index)
    start_url = [x[0] for x in reader][i]
    main(start_url, max_visited=max_visited, parameters_set=parameters_set)


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

    parameters = ['?','#']

    if i is None:
        start_url = 'https://www.soredgear.com/'
        main(start_url, max_visited=-1, parameters_set = parameters, high_priority=True)
    else:
        main_multiprocess([i, 20000, 'queue.csv', parameters])

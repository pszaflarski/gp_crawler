import csv
import json
import multiprocessing
import re

from general_tools import *
from sync_cache import *


def detect_scrape(tree, xpath):
    if xpath is None or xpath == '':
        return True
    else:
        l = tree.xpath(xpath)
        return len(l) > 0


def element_to_text(e, attribute):
    return " ".join([x for x in e.itertext()])


def element_to_html(e, attribute):
    def stringify(e):
        return etree.tostring(e, encoding='utf-8', method='html').decode('utf-8')

    if attribute == 'outer':
        s = stringify(e)
    elif attribute == 'inner':
        s = ''.join([stringify(x) for x in e.iterdescendants()])
    else:
        s = None

    return s


def element_to_attribute(e, attribute):
    return e.get(attribute)


def scrape(tree, scrape_data):
    output = {x: None for x in scrape_data}

    func_dict = {
        "text": element_to_text,
        "html": element_to_html,
        "attribute": element_to_attribute
    }

    for header in scrape_data:
        stuff = scrape_data[header]
        multi = stuff.get("multiple")
        xpath = stuff.get("xpath")
        fmt = stuff.get("format")
        func = func_dict[fmt.split(":")[0]]
        attrib = fmt.split(":")[-1]

        es = tree.xpath(xpath)

        if multi:
            l = json.dumps([func(x, attrib) for x in es])
        else:
            try:
                l = func(es[0], attrib)
            except IndexError:
                l = None

        output[header] = l

    return output


def scrape_one(args):
    row = args[0]
    url = row[1]
    hashfile = row[2]

    info = args[1]
    scrape_trigger = info['scrape_trigger']
    scrape_data = info['scrape_data']
    output_file = info['output_file']
    fieldnames = info['fieldnames']
    include_regex = info["include_regex"]
    if info["exclude_regex"] is None:
        exclude_regex = "a^"
    else:
        exclude_regex = info["exclude_regex"]

    included = re.fullmatch(include_regex, url)
    excluded = re.fullmatch(exclude_regex, url)

    if not included or excluded: return

    source = open("cache\\" + hashfile, 'r', encoding='utf-8', errors='ignore').read()
    tree = etree_pipeline_fromstring(source)
    triggered = detect_scrape(tree, scrape_trigger)
    if triggered:
        print("product found at:", url, end="==>")

        output = scrape(tree, scrape_data)
        output.update({"url": url})
        write_dict_to_csv(output_file, fieldnames, d=output, mode='a')
        print(output)


if __name__ == '__main__':

    start_url = 'https://tester'

    creds = load_creds("credentials.json")
    reader = csv.reader(open("cache\\cachemap.csv"))
    rows = [x for x in reader if x[0] == start_url]

    xpaths = json.load(open("xpaths.json", 'r', encoding='utf-8', errors='ignore'))

    scrape_trigger = xpaths["trigger"]
    scrape_data = xpaths["scrape_data"]
    include_regex = xpaths["include_regex"]
    exclude_regex = xpaths["exclude_regex"]

    fieldnames = ["url"] + [x for x in scrape_data]
    output_file = "output.csv"

    write_dict_to_csv(output_file, fieldnames, mode='w')

    if len(rows) == 0: sync_from_postgres(start_url, creds, creds['postgres_path'], creds['s3_bucket'])

    args = [(x, {
        'scrape_trigger': scrape_trigger,
        'scrape_data': scrape_data,
        'output_file': output_file,
        'fieldnames': fieldnames,
        'exclude_regex': exclude_regex,
        'include_regex': include_regex
    }) for x in rows]

    pool = multiprocessing.Pool(processes=10)
    pool.map(scrape_one, args)

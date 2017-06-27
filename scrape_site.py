import csv
import json

from general_tools import *
from sync_cache import *


def detect_scrape(tree, xpath):
    if xpath is None:
        return True
    else:
        l = tree.xpath(xpath)
        return len(l) > 0


def element_to_text(e, attribute):
    return " ".join([x for x in e.itertext()])


def element_to_html(e, attribute):
    pass


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


if __name__ == '__main__':

    start_url = 'http://fenixcosmetics.com'

    creds = load_creds("credentials.json")
    reader = csv.reader(open("cache\\cachemap.csv"))
    rows = [x for x in reader if x[0] == start_url]

    xpaths = json.load(open("xpaths.json", 'r', encoding='utf-8', errors='ignore'))

    scrape_trigger = xpaths["trigger"]
    scrape_data = xpaths["scrape_data"]

    fieldnames = ["url"] + [x for x in scrape_data]
    output_file = "output.csv"
    write_dict_to_csv(output_file, fieldnames, mode='w')

    if len(rows) == 0: sync_from_postgres(start_url, creds, creds['postgres_path'], creds['s3_bucket'])

    for row in rows:
        source = open("cache\\" + row[2], 'r', encoding='utf-8', errors='ignore').read()
        tree = etree_pipeline_fromstring(source)
        triggered = detect_scrape(tree, scrape_trigger)
        if triggered:
            print("product found at:", row[1], end="==>")

            output = scrape(tree, scrape_data)
            output.update({"url": row[1]})
            write_dict_to_csv(output_file, fieldnames, d=output, mode='a')
            print(output)

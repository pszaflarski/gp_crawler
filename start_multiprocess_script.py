from multi_crawl_and_dump import *
from general_tools import *

import psycopg2
import pickle


def get_websites(n = 10):
    d = load_creds("credentials.json")['redshift']

    sql = """
        select
          accountid,
          website,
          min(datediff(day,salesloft_stagesetat__c,getdate())) setat,
          min(
              case
                  when salesloftstage__c = '0308 Closed - Won' and datediff(day,salesloft_stagesetat__c,getdate()) <= 14 then 0
                  when salesloftstage__c = '0308 Closed - Won' and datediff(day,salesloft_stagesetat__c,getdate()) > 14 then 1
                  when salesloftstage__c = '0303 Selling - Pitch Booked' then 2
                  when salesloftstage__c = '0304 Selling - Pitched' then 3
                  when salesloftstage__c = '0302 Selling - Qualified' then 4
                  when salesloftstage__c = '0301 Prospecting - Active' then 5
                  when salesloftstage__c = '0310 Closed - Lapsed' then 5
                  when salesloftstage__c = '0309 Closed - Timeline' then 6
                  when salesloftstage__c = '0311 Prospecting - No Response' then 7
                  when salesloftstage__c = '0305 Closed - Wrong Person' then 8
                  when salesloftstage__c = '0306 Closed - Disqualified' then 8
                  when salesloftstage__c = '0307 Closed - Lost' then 8
                  when salesloftstage__c is null or salesloftstage__c = '' then 5
                  else 5
              end
          ) priority
          from
              salesforce.sf_contact sfc
          left join
              salesforce.sf_account sfa
          on
              accountid = sfa.id
          group by
              1,2
          order by
              4
        """

    conn = psycopg2.connect(**d)
    cur = conn.cursor()

    cur.execute(sql)
    l1 = cur.fetchall()

    pickle.dump(l1, open('temp.pkl', 'wb'))
    l1 = pickle.load(open('temp.pkl', 'rb'))

    d = load_creds('credentials.json')['postgres']

    conn = psycopg2.connect(**d)
    cur = conn.cursor()

    sql = """select start_url from crawler.crawlerpages_resume where completed"""

    cur.execute(sql)
    l2 = set([x[0] for x in cur.fetchall() if x[0]])

    websites = ['http://' + x[1] for x in l1 if x[1] is not None]
    websites = [x for x in websites if x not in l2][:n]

    return websites


if __name__ == '__main__':

    while True:
        try:
            websites = get_websites(n = 20)

            if len(websites) == 0:break

            print("sites in queue:",", ".join(websites))

            start_multiprocess(websites, num_workers=4)
        except:
            continue








from sqlalchemy import *
from sqlalchemy.types import DATETIME
import datetime


"""
'internal': internal,
'non_http': r.get('non_http'),
'external': r.get('external'),
'page_source': r.get('page_source'),
'url': url,
'start_url': start_url
'exception': None
"""

class CrawlerDataConnector:
    def __init__(self,db_connection = 'sqlite:///crawler.db', file_path = './cache/'):

        self.db_connection = db_connection
        self.file_path = file_path

        self.db = create_engine(db_connection)
        self.db.echo = True

        self.metadata = MetaData(self.db)

        self.page_data = Table('page_data', self.metadata,
            Column('start_url', String(), index=True),
            Column('url', String(), index=True),
            Column('internal_links', BLOB),
            Column('external_links', BLOB),
            Column('page_hash', String()),
            Column('exception', String()),
            Column('scraped_at', DATETIME),
        )

    def _create_pd_table(self):
        self.page_data.create()

    def save_page_data(self,pd):
        i = self.page_data.insert()

        row = {
        'internal': pd['internal'],
        'non_http': pd['non_http'],
        'external': pd['external'],
        'page_source': 'FFFFFFFFFFFFF', #,pd[''],
        'url': pd['url'],
        'start_url': pd['start_url'],
        'exception': None,
        'scraped_at': datetime.datetime.utcnow()

        }
        print(row)
        i.execute(row)



if __name__ == '__main__':

    d = {'page_source': '5', 'external': {'https://www.facebook.com/soredgear/', 'https://edge.personalizer.io/storefront/2.0.0/js/shopify/storefront.min.js?key=du7k6-ru2sx0zpt6u3r2tj-myzu9', 'https://chimpstatic.com/mcjs-connected/js/users/5f0391bdaac980d2ed3af12de/1bc8acbc94eb8e92c1c09c1c5.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/products/sog_f08_n_2__26820.1370640474.1280.1280_206x258.png?v=1498288008', 'https://connect.facebook.net/signals/config/583236795200519?v=2.7.18', 'https://cdn.shopify.com/s/files/1/1872/3697/files/SOC_250x250.PNG?v=1493543247', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Sored_3544873a-5cf4-4c83-847f-c1fb9b75ea83_450x.PNG?v=1492914011', 'https://cdn.shopify.com/s/files/1/1872/3697/t/2/assets/theme.js?3607578594676088918', 'https://www.pinterest.com/soredgear/', 'https://twitter.com/SoredGear', 'https://cdn.shopify.com/s/files/1/1872/3697/products/Nucleus__17511.1486417808.1280.1280_1024x1024_8f217957-1bc0-4c83-8510-c445a6bbb9e1_480x480.jpg?v=1499721863', 'https://www.youtube.com/channel/UChK7dZw7imuePXFDhkVBv0Q', 'https://cdn.shopify.com/s/javascripts/shopify_stats.js?v=6', 'https://cdn.shopify.com/s/files/1/1872/3697/products/er2_cb__85506.1372369182.1280.1280_206x258.jpg?v=1499722007', 'https://edge.personalizer.io/snippets/images/arrow-right.png', 'https://shopify.velsof.com/js/ExitPopup/users/306_upload.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/products/HCB_002F_2014_recolour__61967.1458675000.1280.1280_206x258.png?v=1498288176', 'https://shopify.velsof.com/js/cookie.js?shop=vault-services.myshopify.com', '//fonts.googleapis.com/css?family=Karla:400,700', 'https://cdn.ywxi.net/js/partner-shopify.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/products/x1_26b1e724a1acc7eccbb0b551c7388682__81736.1377727866.1280.1280_206x258.jpg?v=1498287982', 'https://cdn.shopify.com/s/files/1/1872/3697/products/runner_le_600__90261.1376438670.1280.1280_1024x1024_f1b47c1f-e93c-4e57-9e57-7bd1344fa96e_206x258.jpg?v=1499722974', 'https://instashop.wearezipline.com/app/js/dist/shopify/gallery-1.3.js?gallery&shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Sog_250x250.PNG?v=1493543128', 'https://edge.personalizer.io/storefront/2.0.0/css/shopify/venture-theme.min.css', 'https://edge.personalizer.io/storefront/2.0.0/js/channel/core.min.html?channelID=lsChannel', 'https://productreviews.shopifycdn.com/assets/v4/spr.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/assets/themes_support/ga_urchin_forms-68ca1924c495cfc55dac65f4853e0c9a395387ffedc8fe58e0f2e677f95d7f23.js', 'https://limespot.azureedge.net/storefront/2.0.0/js/shopify/checkout-tracker.min.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/t/2/assets/vendor.js?3607578594676088918', '//fonts.googleapis.com/css?family=Unica+One:400,700', 'https://messenger-commerce.shopifycdn.com/assets/new_message_us?version=1497410697&page_id=970582829711748&color=blue&size=large&position_horizontal=right&position_vertical=bottom&messenger_app_id=1163199097047119&shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Fenix_250x250.PNG?v=1493543159', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Colligan_250x250.PNG?v=1493543193', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Coleman_250x250.PNG?v=1493543280', 'https://cdn.shopify.com/s/files/1/1872/3697/products/Ranger_Hero__00805.1446059501.1280.1280_1024x1024_727881b6-b185-41fc-8ecf-75250a8bd323_206x258.jpg?v=1499722907', 'https://www.google-analytics.com/analytics.js', 'https://cdn.shopify.com/s/files/1/1872/3697/products/FA110_OPEN2_206x258.png?v=1498288082', 'https://cdn.shopify.com/s/assets/shop_events_listener-4c5801cae3452eff0ededa0ac07d432c1240b78b7e11282cceb3c3213951104b.js', 'https://product-customizer-cdn.shopstorm.com/assets/storefront/product-customizer-v2-55496c75da8d30869536d1b2451ad95a3b85f06d64dc8fa31607a9c115460fec.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/products/sog_b63_n_2__44343.1392846086.1280.1280_206x258.png?v=1499408954', 'https://connect.facebook.net/en_US/fbevents.js', 'https://cdn.shopify.com/s/files/1/1872/3697/products/ASRS_CB_Left_Cropped__51683.1390715592.1280.1280_1024x1024_f88952c2-cd82-403a-b87d-882c8787d75b_480x480.jpg?v=1499718802', 'https://cdn.shopify.com/s/assets/storefront/express_buttons-1715ebafe081fee47f2a17923a6be49280024486cfb7b6554588c6a38f9c540f.js', 'https://cdn.shopify.com/s/files/1/1872/3697/t/2/assets/theme.scss.css?3607578594676088918', 'https://www.instagram.com/soredgear/', 'https://sdk.beeketing.com/js/beeketing.js?shop=vault-services.myshopify.com', 'https://www.shopify.com', 'https://edge.personalizer.io/snippets/images/rec-box-product-loading.gif', 'https://shopify.velsof.com/js/ExitPopup/ouibounce.js?shop=vault-services.myshopify.com', 'https://cdn.shopify.com/s/files/1/1872/3697/files/Gerber_250x250.PNG?v=1493543339', 'https://cdn.shopify.com/s/files/1/1872/3697/t/2/assets/shopstorm-apps.scss.css?3607578594676088918', 'https://edge.personalizer.io/snippets/images/arrow-left.png', 'https://cdn.shopify.com/s/javascripts/tricorder/trekkie.storefront.min.js?v=2017.03.29.1', 'https://cdn.shopify.com/s/files/1/1872/3697/files/midland_250x250.PNG?v=1493543217', 'https://connect.facebook.net/signals/config/1541175242579873?v=2.7.18', 'https://cdn.shopify.com/s/files/1/1872/3697/collections/stomp_opened__39564.1404591666.1280.1280_grande_153481a2-e05e-4e83-86ac-b39a741a7c1f_480x480.jpg?v=1498535217', 'https://cdn.shopify.com/s/files/1/1872/3697/files/falcon_32x32.jpg?v=1497245216', 'https://edge.personalizer.io/storefront/2.0.0/css/recommendations.min.css'}, 'non_http': set(), 'url': 'https://soredgear.com/', 'exception': None, 'internal': ['https://soredgear.com/products/sog-survival-hawk?lssrc=popular&lshst=home', 'https://soredgear.com/', 'https://soredgear.com/blogs/sored-gear-kit-contents', 'https://soredgear.com/apps/help-center', 'https://soredgear.com/products/sored-gear-get-home-bag-sog-special-edition?lssrc=popular&lshst=home', 'https://soredgear.com/pages/contact-us', 'https://soredgear.com/products/sored-m-17-medic-kit?lssrc=popular&lshst=home', 'https://soredgear.com/products/sored-tactical-trauma-kit-3?lssrc=popular&lshst=home', 'https://soredgear.com/products/katadyn-pocket-water-microfilter-20-year-warranty?lssrc=popular&lshst=home', 'https://soredgear.com/products/sored-gear-2-5-liter-hydration-bladder?lssrc=popular&lshst=home', 'https://soredgear.com/18723697/digital_wallets/dialog', 'https://soredgear.com/products/fenix-ld22-2015-edition-300-lumen-handheld-tactical-flashlight?lssrc=popular&lshst=home', 'https://soredgear.com/products/midland-er300-emergency-multi-power-radio?lssrc=popular&lshst=home', 'https://soredgear.com/pages/about-us', 'https://soredgear.com/products/sored-t-7-medic-kit?lssrc=popular&lshst=home', 'https://soredgear.com/account/login', 'https://soredgear.com/products/sored-gear-range-bag-w-compact-trauma-kit?lssrc=popular&lshst=home', 'https://soredgear.com/products/sored-gear-ready-and-go-cr-critical-response?lssrc=popular&lshst=home', 'https://soredgear.com/collections/food-and-water', 'https://soredgear.com/collections/electronics', 'https://soredgear.com/products/lifestraw-personal-water-filter?lssrc=popular&lshst=home', 'https://soredgear.com/collections/packs-and-pouches', 'https://soredgear.com/products/sored-compact-active-shooter-response-system-casrs?lssrc=popular&lshst=home', 'https://soredgear.com/products/sog-entrenching-tool?lssrc=popular&lshst=home', 'https://soredgear.com/collections/tools-and-blades', 'https://soredgear.com/search', 'https://soredgear.com/collections/tactical-kits', 'https://soredgear.com/collections/medical-kits', 'https://soredgear.com/collections/flashlights', 'https://soredgear.com/cart', 'https://soredgear.com/products/sog-b63-power-lock-eod-2-0-multi-tool?lssrc=popular&lshst=home', 'https://soredgear.com/products/sog-seal-pup-fixed-blade-knife?lssrc=popular&lshst=home', 'https://soredgear.com/pages/see-our-kit-comparison-chart', 'https://soredgear.com/products/sog-s62-power-lock-multi-tool?lssrc=popular&lshst=home', 'https://soredgear.com/collections', 'https://soredgear.com/products/sored-gear-emergency-roll-away-for-two-sg2?lssrc=popular&lshst=home', 'https://soredgear.com/products/katadyn-hiker-water-micro-filter?lssrc=popular&lshst=home', 'https://soredgear.com/pages/become-a-sored-gear-dealer', 'https://soredgear.com/collections/emergency-kits', 'https://soredgear.com/blogs/library', 'https://soredgear.com/products/sored-gear-ready-and-go-pack?lssrc=popular&lshst=home', 'https://soredgear.com/#MainContent'], 'start_url': 'https://soredgear.com/'}

    cdc = CrawlerDataConnector()

    # cdc._create_pd_table()

    cdc.save_page_data(d)

    q = cdc.page_data.select()
    c = q.execute()

    for result in c:
        print(result)











# db = create_engine('sqlite:///tutorial.db')
#
# db.echo = False  # Try changing this to True and see what happens
#
# metadata = MetaData(db)
#
# page_data = Table('page_data', metadata,
#     Column('start_url', String(), index=True),
#     Column('url', String(), index=True),
#     Column('internal_links', BLOB),
#     Column('external_links', BLOB),
#     Column('page_hash', String()),
#     Column('exception', String()),
#     Column('scraped_at', DATETIME),
# )
#
#
# print(db.table_names())
#
# # print(users.indexes)
# #
# #
# try:
#     page_data.create()
# except:
#     pass
# #
# # i = users.insert()
# # i.execute(name='Mary', age=30, password='secret')
# # i.execute({'name': 'John', 'age': 42},
#           {'name': 5, 'age': 57},
#           {'name': 'Carl', 'age': 33})
#
# s = users.select()
# rs = s.execute()
#
# row = rs.fetchone()
# print('Id:', row[0])
# # print 'Name:', row['name']
# # print 'Age:', row.age
# # print 'Password:', row[users.c.password]
#
# for row in rs:
#     print(row.name, 'is', row.age, 'years old')
#

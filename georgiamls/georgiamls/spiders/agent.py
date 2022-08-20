from urllib.parse import urlencode

import scrapy
from scrapy import Request


def get_url(url):
    payload = {'api_key': 'YOR API KEY', 'url': url, 'country_code': 'us'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


class AgentSpider(scrapy.Spider):
    name = 'agent'
    allowed_domains = ['www.georgiamls.com']
    start_urls = ['https://www.georgiamls.com']
    custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 1,
                       'FEED_URI': r'output.xlsx',
                       'FEED_FORMAT': 'xlsx',
                       'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
                       }

    def start_requests(self):
        url = 'https://www.georgiamls.com/real-estate-agents/directory/'
        yield scrapy.Request(get_url(url), callback=self.parse)

    def parse(self, response):
        alphabet_urls = response.xpath("//ul[@id='alphabet']/li/a/@href").extract()
        for url in alphabet_urls:
            if not url.startswith(self.start_urls[0]):
                url = self.start_urls[0] + url
            yield Request(get_url(url), callback=self.parse_details, meta={'orignal_url': url}, dont_filter=True)

    def parse_details(self, response):
        agent_listing = response.xpath("//div[@class='table-responsiveX']/table/tbody/tr")
        for tr in agent_listing:
            yield {'agent_name': tr.xpath("./td[1]/a/text()").get(),
                   'phone_no': tr.xpath("./td[2]/a/text()").get(),
                   'office_phone_no': tr.xpath("./td[3]/a/text()").get(),
                   'real_estate_office': tr.xpath("./td[4]/a/text()").get(),
                   'detail_url': self.start_urls[0] + tr.xpath("./td[1]/a/@href").get(),
                   'listing_url': response.meta['orignal_url']
                   }

        next_page_url = response.xpath("//a[@rel='next']/@href").get()
        if next_page_url:
            if not next_page_url.startswith(self.start_urls[0]):
                next_page_url = self.start_urls[0] + next_page_url
            yield Request(get_url(next_page_url), callback=self.parse_details, meta={'orignal_url': next_page_url},
                          dont_filter=True)

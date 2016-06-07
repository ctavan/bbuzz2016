# Run like this:
# scrapy runspider bbuzz_spider.py -s LOG_LEVEL=INFO -o s3://bbuzz2016/all-years.jsonlines

import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse

class BbuzzSpider(CrawlSpider):
    name = 'bbuzz'

    def __init__(self, year=None, *args, **kwargs):
        super(BbuzzSpider, self).__init__(*args, **kwargs)

        if year:
            prefixes = ['%s.' % year if year != '2016' else 'www.']
        else:
            prefixes = [('%s.' % year) for year in range(2010, 2016)] + ['www.']

        self.allowed_domains = [('%sberlinbuzzwords.de' % prefix) for prefix in prefixes]
        self.start_urls = [('http://%sberlinbuzzwords.de/' % prefix) for prefix in prefixes]

    rules = (
        Rule(LinkExtractor(allow=(), deny=('/file/')),
             callback='parse_item',
             follow=True),
    )

    # Hack to fix the lack of proper content-type from 2010 and 2011 websites
    def _response_downloaded(self, response):
        if not isinstance(response, HtmlResponse):
            new_headers = dict(response.headers)
            new_headers.update({'Content-Type': ['text/html; charset=utf-8']})
            response = HtmlResponse(
                response.url,
                status=response.status,
                headers=new_headers,
                body=response.body,
                flags=response.flags,
                request=response.request,
            )
        return super(BbuzzSpider, self)._response_downloaded(response)

    def parse_item(self, response):
        link_year = extractYear(str(response.url))
        content_year = ' '.join(response.css('.date-display-single::text').extract())
        special_years = set([2012, 2013])
        if link_year not in special_years and (not content_year or content_year.find(str(link_year)) == -1):
            #print 'Not parsing %s due to year mismatch: %s vs %s' % (response.url, link_year, content_year)
            pass
        elif link_year in special_years and not len(response.css('.field-field-session-slot::text').extract()):
            #print 'Not parsing %s due to year mismatch: %s vs %s' % (response.url, link_year, content_year)
            pass
        else:
            yield {
                'title': ' '.join(response.css('h1.title::text').extract() + response.css('h1#page-title::text').extract()),
                'content': ' '.join(response.css('#main p::text').extract() + response.css('article p::text').extract()),
                'speakers': stripArray(response.css('.field-field-speaker a::text').extract() + response.css('.field-field-indiv-speakers a::text').extract() + response.css('.field-field-speakers a::text').extract() + response.css('.field-name-field-session-speaker a::text').extract()),
                'link': response.url,
            }

def stripArray(x):
    return [element.lower().strip() for element in x]

def extractYear(link):
    year_match = re.search('(\d+)\.berlinbuzzwords', link)
    return int(year_match.group(1)) if year_match else 2016


# 2010
# h1
# #main p
# URL: /content/
# .field-field-speaker a

# 2011
# h1
# #main p
# URL: /content/
# .field-field-speaker a

# 2012
# URL: /sessions/
# h1
# #main p
# .field-field-indiv-speakers a

# 2013
# URL: /sessions/
# h1
# #main p
# .field-field-speakers a

# 2014
# URL: /session/
# h1#page-title
# article p
# .field-name-field-session-speaker a

# 2015
# URL: /session/
# h1#page-title
# article p
# .field-name-field-session-speaker a

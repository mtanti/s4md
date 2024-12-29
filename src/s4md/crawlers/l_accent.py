'''
Download PDFs from L-aċċent Maltese langauge magazine.
'''

import email
from typing import Iterator
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


class LAccentCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='L-Aċċent',
            is_ordered_by_new=True,
        )

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        page_url = 'https://ec.europa.eu/translation/maltese/magazine/mt_magazine_en.htm'
        domain = 'https://ec.europa.eu/'
        visited_links = set()
        soup = get_page(page_url, delay)
        for (link, link_info) in get_links(domain, soup):
            if link_info.is_document:
                if link_info.clean_url not in visited_links:
                    visited_links.add(link_info.clean_url)
                    doc_info = DocumentInfo(page_url, link_info.orig_url, link_info.clean_url, link_info.fname, link_info.upload_date)
                    yield (True, doc_info)

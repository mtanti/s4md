'''
Download PDFs from the European Agency for Special Needs and Inclusive Education Maltese publications.
'''

import email
from typing import Iterator
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


class EuropeanAgencyCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='European Agency',
            is_ordered_by_new=False,
        )

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        page_url = 'https://www.european-agency.org/Malti/publications/'
        domain = 'https://www.european-agency.org/'
        visited_links = set()
        soup = get_page(page_url, delay)
        for (link, link_info) in get_links(domain, soup):
            if link_info.is_document:
                if link_info.clean_url not in visited_links:
                    visited_links.add(link_info.clean_url)
                    doc_info = DocumentInfo(page_url, link_info.orig_url, link_info.clean_url, link_info.fname, link_info.upload_date)
                    yield (True, doc_info)

'''
Download PDFs from Bird Life Malta's magazine Il-Ħuttafa.
'''

from typing import Iterator
import email
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


class IlHuttafaCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='Il-Ħuttafa',
            is_ordered_by_new=True,
        )

    def scrape(
        self,
        delay: float = 0.0,
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        page_url = 'https://birdlifemalta.org/information/publications/il-huttafa/'
        domain = 'https://birdlifemalta.org/'
        visited_links = set()
        soup = get_page(page_url, delay)
        for (link, link_info) in get_links(domain, soup):
            if link_info.is_document:
                if link_info.clean_url not in visited_links:
                    visited_links.add(link_info.clean_url)
                    doc_info = DocumentInfo(page_url, link_info.orig_url, link_info.clean_url, link_info.fname, link_info.upload_date)
                    yield (True, doc_info)

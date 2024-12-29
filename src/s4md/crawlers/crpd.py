'''
Download PDFs from the Commission for the Rights of Persons with Disability website.
'''

from typing import Iterator
import email
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


FNAME_BLACK_LIST_PREFIX = [
    'CRPD-Article-17-MT-'
]


class CRPDCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='CRPD',
            is_ordered_by_new=False,
        )

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        init_url = 'https://www.crpd.org.mt/mt/'
        domain = 'https://www.crpd.org.mt/'
        soup = get_page(init_url, delay)
        page_urls = []
        for (link, link_info) in get_links(domain, soup.find('ul', id='menu-main-menu-mt')):
            if link_info.is_webpage:
                page_urls.append(link_info.clean_url)

        visited_links = set()
        for page_url in page_urls:
            soup2 = get_page(page_url, delay)
            for (link2, link_info2) in get_links(domain, soup2):
                if link_info2.is_document:
                    if link_info2.clean_url not in visited_links:
                        visited_links.add(link_info2.clean_url)
                        doc_info = DocumentInfo(page_url, link_info2.orig_url, link_info2.clean_url, link_info2.fname, link_info2.upload_date)
                        accepted = link_info2.is_maltese_fname and not any(link_info2.fname.startswith(prefix) for prefix in FNAME_BLACK_LIST_PREFIX)
                        yield (accepted, doc_info)

'''
Download PDFs from the Goverment of Malta group of websites.
'''

from typing import Iterator
import re
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


HEADING_RE = re.compile('h[0-9]')
FNAME_BLACK_LIST = {
    'MT-Gas-SOS-Emergency-Plan.pdf',
}

class GovMtCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='Government of Malta',
            is_ordered_by_new=False,
        )

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        visited_links = set()
        for subdomain in [
            'opm',
            'fondiewropej',
            'kultura',
            'affarijietbarranin',
            'familja',
            'agrikoltura',
            'akkomodazzjoni',
            'ekonomija',
            'inkluzjoni',
            'ghawdex',
            'sigurta',
            'turizmu',
            'sostenibilita',
            'finanzi',
            'edukazzjoni',
            'artijiet',
            'gustizzja',
            'sahha',
            'infrastruttura',
            'djalogusocjali',
            'gvernlokali',
            'zghazagh',
            'riformi',
            'sajd',
            'anzjanitaattiva',
            'xoghlijietpubblici',
            'indafapubblika',
        ]:
            page_url = f'https://{subdomain}.gov.mt/rizorsi/'
            domain = f'https://{subdomain}.gov.mt/'
            soup = get_page(page_url, delay)
            for (link, link_info) in get_links(domain, soup):
                if link_info.is_document:
                    if link_info.clean_url not in visited_links:
                        visited_links.add(link_info.clean_url)
                        doc_info = DocumentInfo(page_url, link_info.orig_url, link_info.clean_url, link_info.fname, None)
                        accepted = link_info.is_maltese_fname and link_info.fname not in FNAME_BLACK_LIST
                        yield (accepted, doc_info)
            for subsection_title in ['Pubblikazzjonijiet', 'Baġit', 'Artikli']:
                subsection_title_elem = soup.find(HEADING_RE, string=subsection_title)
                if subsection_title_elem is not None:
                    for (link, link_info) in get_links(domain, subsection_title_elem.parent):
                        if link_info.is_webpage:
                            soup2 = get_page(link_info.clean_url, delay)
                            for (link2, link_info2) in get_links(domain, soup2):
                                if link_info2.is_document:
                                    if link_info2.clean_url not in visited_links:
                                        visited_links.add(link_info2.clean_url)
                                        doc_info = DocumentInfo(link_info.clean_url, link_info2.orig_url, link_info2.clean_url, link_info2.fname, None)
                                        accepted = link_info2.is_maltese_fname and link_info2.fname not in FNAME_BLACK_LIST
                                        yield (accepted, doc_info)

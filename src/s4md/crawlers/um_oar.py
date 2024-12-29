'''
Download PDFs from the University of Malta's Open Access Repository online library.
'''

from typing import Iterator, Optional
import datetime
from s4md.crawler import Crawler, DocumentInfo, get_page, get_links


FNAME_BLACK_LIST = {
    'OAR%40UM_help.pdf',
}

class UMOARCrawler(Crawler):
    '''
    '''

    def __init__(
        self,
    ) -> None:
        '''
        '''
        super().__init__(
            name='UM OAR',
            is_ordered_by_new=True,
        )

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        next_url: Optional[str] = 'https://www.um.edu.mt/library/oar/browse?type=iso&value=mt&sort_by=3&order=DESC&rpp=100&etal=0'
        domain = 'https://www.um.edu.mt/'
        visited_links = set()
        rights_set = set()
        while True:
            assert next_url is not None
            soup = get_page(next_url, delay)
            next_url = None
            for (link, link_info) in get_links(domain, soup):
                if link_info.clean_url.startswith('https://www.um.edu.mt/library/oar/handle/'):
                    soup2 = get_page(f'{link_info.clean_url}?mode=full', delay)
                    rights = soup2.find('td', string='dc.rights').next_sibling.string
                    rights_set.add(rights)
                    for (link2, link_info2) in get_links(domain, soup2):
                        if link_info2.clean_url.startswith('https://www.um.edu.mt/library/oar/bitstream/') and link_info2.is_document:
                            date_str = soup2.find('td', string='dc.date.accessioned').next_sibling.string
                            date = datetime.datetime.fromisoformat(date_str[:-1])
                            if link_info2.clean_url not in visited_links:
                                visited_links.add(link_info2.clean_url)
                                doc_info = DocumentInfo(link_info.clean_url, link_info2.orig_url, link_info2.clean_url, link_info2.fname, date)
                                accepted = link_info2.fname not in FNAME_BLACK_LIST
                                yield (accepted, doc_info)
                elif link.string == 'next\xa0>':
                    next_url = link_info.clean_url

            if next_url is None:
                print(rights_set)
                break


for (x, y) in UMOARCrawler().scrape(1.0):
    print(y.parent_url, '>', y.clean_doc_url, x)
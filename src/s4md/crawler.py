'''
'''

import re
import time
import datetime
import urllib
import os
import email
import cloudscraper
import validators
from typing import Optional, Iterator
from bs4 import BeautifulSoup
from dataclasses import dataclass


MALTESE_FNAME_FILTER_RE = re.compile('(\\b|_|[0-9])(mt|mlt|maltese[- ]version|rapport(i?)|diskors|familja)(\\b|_)', re.IGNORECASE)
DOCUMENT_EXTS = ['.pdf', '.docx', '.doc']
WEBPAGE_EXTS = ['.htm', '.html', '.aspx', '.php']


@dataclass
class DocumentInfo:
    '''
    '''
    parent_url: str
    orig_doc_url: str
    clean_doc_url: str
    orig_fname: str
    upload_date: Optional[datetime.datetime]


@dataclass
class URLInfo:
    '''
    '''
    orig_url: str
    clean_url: str
    fname: str
    is_document: bool
    is_webpage: bool
    is_maltese_fname: bool
    upload_date: Optional[datetime.datetime]


class Crawler:
    '''
    '''

    def __init__(
        self,
        name: str,
        is_ordered_by_new: bool,
    ) -> None:
        '''
        '''
        self.name = name
        self.is_ordered_by_new = is_ordered_by_new

    def scrape(
        self,
        delay: float = 0.0
    ) -> Iterator[tuple[bool, DocumentInfo]]:
        '''
        '''
        raise NotImplementedError()


def get_header(
    url: str,
    delay: float = 0.0,
) -> tuple[bool, dict[str, str]]:
    '''
    '''
    time.sleep(delay)
    scraper = cloudscraper.create_scraper()
    response = scraper.head(url)
    headers = response.headers
    ok = response.ok
    return (ok, headers)


def get_page(
    url: str,
    delay: float = 0.0,
) -> BeautifulSoup:
    '''
    '''
    time.sleep(delay)
    scraper = cloudscraper.create_scraper()
    response = scraper.get(url)
    html = response.content
    soup = BeautifulSoup(html, features='html.parser')
    return soup


def download(
    url: str,
    destination_path: str,
    delay: float = 0.0,
) -> None:
    '''
    '''
    time.sleep(delay)
    scraper = cloudscraper.create_scraper()
    response = scraper.get(url, stream=True)
    with open(destination_path, 'wb') as f:
        for block in response.iter_content():
            f.write(block)


def get_link_info(
    domain: str,
    url: str,
    delay: float = 0.0,
) -> URLInfo:
    '''
    '''
    clean_url = url
    is_document = False
    is_webpage = False
    is_maltese_fname = False
    fname = ''
    upload_date: Optional[datetime.datetime] = None

    if clean_url.startswith('/'):
        clean_url = f'{domain}{clean_url}'
    url_parts = urllib.parse.urlparse(clean_url)._asdict()
    url_parts['path'] = url_parts['path'].replace('//', '/') # Remove any wrongly used double slashes.
    clean_url = urllib.parse.ParseResult(**url_parts).geturl()
    fname = os.path.split(url_parts['path'])[1]

    if validators.url(clean_url) and (clean_url.startswith(domain) or clean_url.replace('http://', 'https://').startswith(domain)):
        if '.' not in fname:
            fname = ''
            is_webpage = True
        elif not any(fname.endswith(ext) for ext in DOCUMENT_EXTS):
            if any(fname.endswith(ext) for ext in WEBPAGE_EXTS):
                is_webpage = True
        else:
            (found, header) = get_header(clean_url, delay)
            if 'Location' in header:
                clean_url = header['Location']
                (found, header) = get_header(clean_url, delay)
            if found:
                if header['Content-Type'].startswith('text'):
                    # Document does not exist and leads to a custom 404 page.
                    pass
                else:
                    is_document = True
                    is_maltese_fname = MALTESE_FNAME_FILTER_RE.search(fname) is not None
                    if 'Last-Modified' in header:
                        upload_date = email.utils.parsedate_to_datetime(header['Last-Modified'])

    return URLInfo(url, clean_url, fname, is_document, is_webpage, is_maltese_fname, upload_date)


def get_links(
    domain: str,
    soup: BeautifulSoup,
    delay: float = 0.0,
) -> Iterator[tuple[BeautifulSoup, URLInfo]]:
    '''
    '''
    for link in soup.find_all('a'):
        href = link.get('href')
        if href is not None:
            link_info = get_link_info(domain, href, delay)
            yield (link, link_info)

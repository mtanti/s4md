'''
'''

import sqlite3
import os
import tempfile
import csv
import hashlib
import datetime
from s4md.crawler import Crawler, download
from s4md.crawlers.crpd import CRPDCrawler
from s4md.crawlers.european_agency import EuropeanAgencyCrawler
from s4md.crawlers.gov_mt import GovMtCrawler
from s4md.crawlers.il_huttafa import IlHuttafaCrawler
from s4md.crawlers.l_accent import LAccentCrawler
from s4md.crawlers.um_oar import UMOARCrawler


CRAWLERS: list[Crawler] = [
    EuropeanAgencyCrawler(),
    LAccentCrawler(),
    IlHuttafaCrawler(),
    CRPDCrawler(),
    GovMtCrawler(),
    UMOARCrawler(),
]


def download_repo(
    repo_path: str,
    crawlers: list[Crawler] = CRAWLERS,
    delay: float = 0.0,
) -> None:
    '''
    '''
    os.makedirs(os.path.join(repo_path, 'documents'), exist_ok=True)
    conn = sqlite3.connect(os.path.join(repo_path, 'documents.sqlite3'))
    with conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS `documents` (
                `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                `parent_url` TEXT,
                `orig_doc_url` TEXT,
                `clean_doc_url` TEXT,
                `orig_fname` TEXT,
                `new_fname` TEXT UNIQUE,
                `upload_date` TEXT,
                `download_date` TEXT,
                `sha256_hash` TEXT UNIQUE
            )
            ;
        ''')

    with open(os.path.join(repo_path, 'skipped_documents.csv'), 'w', encoding='utf-8', newline='') as skipped_f:
        writer = csv.writer(skipped_f)
        writer.writerow(['parent_url', 'orig_doc_url'])

    for crawler in crawlers:
        print(crawler.name)
        for (accepted, doc_info) in crawler.scrape(delay):
            if not accepted:
                with open(os.path.join(repo_path, 'skipped_documents.csv'), 'a', encoding='utf-8', newline='') as skipped_f:
                    writer = csv.writer(skipped_f)
                    writer.writerow([doc_info.parent_url, doc_info.orig_doc_url])
            else:
                print('>', doc_info.clean_doc_url)
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_doc_path = os.path.join(tmp_dir, doc_info.orig_fname)
                    download(doc_info.clean_doc_url, tmp_doc_path, delay)
                    with open(tmp_doc_path, 'rb') as f:
                        hash = hashlib.sha256(f.read()).hexdigest()

                    cursor = conn.execute('''
                        SELECT 1
                        FROM `documents`
                        WHERE sha256_hash = :hash
                        ;
                    ''', {
                        'hash': hash,
                    })
                    already_downloaded = cursor.fetchone() is not None

                    if not already_downloaded:
                        upload_date = doc_info.upload_date.isoformat() if doc_info.upload_date is not None else None
                        download_date = datetime.datetime.fromtimestamp(os.path.getmtime(tmp_doc_path))
                        doc_ext = os.path.splitext(doc_info.orig_fname)[1].lower()

                        with conn:
                            cursor = conn.execute('''
                                INSERT INTO `documents` (
                                    `parent_url`,
                                    `orig_doc_url`,
                                    `clean_doc_url`,
                                    `orig_fname`,
                                    `new_fname`,
                                    `upload_date`,
                                    `download_date`,
                                    `sha256_hash`
                                )
                                VALUES (
                                    :parent_url,
                                    :orig_doc_url,
                                    :clean_doc_url,
                                    :orig_fname,
                                    NULL,
                                    :upload_date,
                                    :download_date,
                                    :sha256_hash
                                )
                                ;
                            ''', {
                                'parent_url': doc_info.parent_url,
                                'orig_doc_url': doc_info.orig_doc_url,
                                'clean_doc_url': doc_info.clean_doc_url,
                                'orig_fname': doc_info.orig_fname,
                                'upload_date': upload_date,
                                'download_date': download_date,
                                'sha256_hash': hash,
                            })

                            doc_id = cursor.lastrowid
                            new_fname = f'{doc_id:0>4d}{doc_ext}'
                            conn.execute('''
                                UPDATE `documents` SET
                                    `new_fname` = :new_fname
                                WHERE
                                    id = :doc_id
                                ;
                            ''', {
                                'new_fname': new_fname,
                                'doc_id': doc_id,
                            })

                            os.rename(tmp_doc_path, os.path.join(repo_path, 'documents', new_fname))
        print()

    print('Exporting list to CSV')
    with open(os.path.join(repo_path, 'documents.csv'), 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'id',
            'parent_url',
            'orig_doc_url',
            'clean_doc_url',
            'orig_fname',
            'new_fname',
            'upload_date',
            'download_date',
            'sha256_hash',
        ])
        cursor = conn.execute('''
            SELECT
                `id`,
                `parent_url`,
                `orig_doc_url`,
                `clean_doc_url`,
                `orig_fname`,
                `new_fname`,
                `upload_date`,
                `download_date`,
                `sha256_hash`
            FROM
                `documents`
        ''')
        for row in cursor:
            writer.writerow(row)

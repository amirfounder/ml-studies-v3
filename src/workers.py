import re
import time
from os.path import exists

import requests
import bs4
import spacy
import feedparser

from commons import write, read, log
from decorators import worker, subtask, task
from models import Index, IndexEntry, Report
from enums import WorkerNames, OutputPaths, Status

nlp = spacy.load('en_core_web_sm')


@subtask
def get_cnn_rss_urls():
    path = 'data/cnn_rss_html.html'
    if not exists(path):
        resp = requests.get('https://www.cnn.com/services/rss/')
        resp.raise_for_status()
        write(path, resp.text)

    soup = bs4.BeautifulSoup(read(path), 'html.parser')
    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return list(zip(topics, urls))


@subtask
def get_cnn_money_rss_urls():
    path = 'data/cnn_money_rss_html.html'
    if not exists(path):
        resp = requests.get('https://money.cnn.com/services/rss/')
        resp.raise_for_status()
        write(path, resp.text)

    soup = bs4.BeautifulSoup(read(path), 'html.parser')
    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/(rss/money_|cnnmoneymorningbuzz)')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [
        'cnn_money_' + u
            .removeprefix('http://rss.cnn.com/rss/')
            .removeprefix('money_')
            .removeprefix('cnnmoneymorningbuzz')
            .removesuffix('.rss')
        for u in urls
    ]

    return list(zip(topics, urls))


@worker
def index_rss_entries():
    with Index() as index:
        topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
        topics_entries = []

        for topic, rss_url in topics_urls:
            topics_entries.append((topic, feedparser.parse(rss_url).entries))

        count = 0
        for topic, entries in topics_entries:
            for entry in entries:
                url = entry['link']

                if url not in index and 'cnn.com' in url[:20]:
                    next_file_name = str(len(index) + 1)

                    reports = {}
                    for n, v in [(n.name, n.value) for n in WorkerNames]:
                        reports[v] = Report(output_path=OutputPaths[n].value.format(next_file_name))

                    index[url] = IndexEntry(
                        _index=index,
                        url=url,
                        topic=topic,
                        reports=reports,
                        output_filename=next_file_name
                    )

        log(f'New entries indexed: {count}')


@worker
def scrape_htmls():

    @task
    def scrape_html(entry: IndexEntry):
        resp = requests.get(entry.url)
        resp.raise_for_status()
        write(entry[WorkerNames.SCRAPE_HTMLS].output_path, resp.text)
        time.sleep(1)

    with Index() as index:
        entries_to_scrape = [
            entry for entry in index.values() if
            not entry[WorkerNames.SCRAPE_HTMLS].has_been_attempted
        ]

        log(f'Entries to scrape: {len(entries_to_scrape)}')

        for entry in entries_to_scrape:
            result, exception = scrape_html(entry)


@worker
def extract_texts():

    @task
    def extract_text(entry: IndexEntry):
        soup = bs4.BeautifulSoup(read(entry[WorkerNames.SCRAPE_HTMLS].output_path), 'html.parser')
        write(entry[WorkerNames.EXTRACT_TEXTS].output_path, soup.text)

    with Index() as index:
        entries = [
            entry for entry in index.values() if
            entry[WorkerNames.SCRAPE_HTMLS].status == Status.SUCCESS
            # not entry[WorkerNames.EXTRACT_TEXTS].status == Status.SUCCESS
        ]
        log(f'Entries to extract article text from (v2): {len(entries)}')
        for entry in entries:
            extract_text(entry)


@worker
def process_texts():

    @task
    def process_text(entry: IndexEntry):
        doc = nlp(read(entry[WorkerNames.EXTRACT_TEXTS].output_path))
        tokens = [token for token in doc]
        tokens = [
            token for token in tokens if
            not token.like_email and
            not token.like_url and
            not token.is_stop and
            not token.is_punct
        ]
        write(entry[WorkerNames.PROCESS_TEXTS].output_path, ''.join([t.text for t in tokens]))

    with Index(lambda e: e['extract_texts'].status == Status.SUCCESS and e['process_texts'].status != Status.SUCCESS) as index:
        entries = [
            entry for entry in index.values() if
            entry[WorkerNames.EXTRACT_TEXTS].status == Status.SUCCESS
            # not entry[WorkerNames.PROCESS_TEXTS].status == Status.SUCCESS
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        for entry in entries:
            process_text(entry)

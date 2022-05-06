import re
import time
from os.path import exists
from typing import Generator

import requests
import bs4
import spacy
import feedparser
from spacy.tokens import Token

from commons import write, read
from helpers import worker, log
from models import Index, IndexEntry, Report
from enums import WorkerNames, OutputPaths, Status

nlp = spacy.load('en_core_web_sm')


@worker()
def sync_index():

    with Index() as index:
        index: dict[str, IndexEntry]
        for k, v in index.items():
            for _k, _v in v.reports.items():
                if not _v.output_path:
                    output_path = OutputPaths[WorkerNames(_k).name].value.format(v.output_filename)
                    _v.output_path = output_path

@worker()
def index_rss_entries():

    @index_rss_entries.subtask
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

    @index_rss_entries.subtask
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

    @index_rss_entries.subtask
    def index_entry(_index, _topic, _url):
        next_file_name = str(len(_index) + 1)

        reports = {}
        for n, v in [(n.name, n.value) for n in WorkerNames]:
            reports[v] = Report(output_path=OutputPaths[n].value.format(next_file_name))

        _index[_url] = IndexEntry(
            _index=_index,
            url=_url,
            topic=_topic,
            reports=reports,
            output_filename=next_file_name
        )

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
                    index_entry(index, topic, url)
                    count += 1

        log(f'New entries indexed: {count}')


@worker(name=WorkerNames.SCRAPE_HTMLS)
def scrape_htmls():

    @scrape_htmls.task
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
            scrape_html(entry)


@worker(name=WorkerNames.EXTRACT_TEXTS)
def extract_texts():

    @extract_texts.task
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


@worker(name=WorkerNames.PROCESS_TEXTS)
def process_texts():

    @process_texts.subtask
    def clean_tokens(tokens: list[Token]) -> Generator[Token, None, None]:

        def is_valid(_token: Token):
            return (
                not _token.like_email and
                not _token.like_url and
                not _token.is_stop and
                not _token.is_punct
            )

        for token in tokens:
            if is_valid(token):
                yield token

    @process_texts.task
    def clean_entry(entry: IndexEntry):
        doc = nlp(read(entry[WorkerNames.EXTRACT_TEXTS].output_path))
        tokens = [token for token in doc]
        tokens = clean_tokens(tokens)
        write(entry[WorkerNames.PROCESS_TEXTS].output_path, ''.join([t.text for t in tokens]))

    with Index() as index:
        entries = [
            entry for entry in index.values() if
            entry[WorkerNames.EXTRACT_TEXTS].status == Status.SUCCESS
            # not entry[WorkerNames.PROCESS_TEXTS].status == Status.SUCCESS
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        for entry in entries:
            clean_entry(entry)

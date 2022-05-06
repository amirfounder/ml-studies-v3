import re
import time

import requests  # when JS rendering sites cause trouble -- skip to next news site. we will open up scraper service l8r
import bs4
import spacy

from helpers import *
from models import *
from enums import *

nlp = spacy.load('en_core_web_sm')


@worker()
def sync_index():

    @sync_index.task
    def sync_preprocessed_texts(_i, _entry):
        if not entry.preprocessed_text_path:
            entry.preprocessed_text_path = 'data/cnn_articles_preprocessed_texts/' + str(i + 1) + '.txt'

    with CnnArticleIndexManager() as index:
        for i, entry in enumerate(index.values()):
            sync_preprocessed_texts(i, entry)


@worker()
def index_rss_entries():

    @index_rss_entries.task
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

    @index_rss_entries.task
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

    @index_rss_entries.task
    def index_entry(_index, _topic, _url):
        next_file_name = str(len(_index) + 1)
        _index[_url] = IndexEntry(
            _index=_index,
            url=_url,
            topic=_topic,
            reports={w.value: {} for w in Worker},
            filename=next_file_name
        )

    with CnnArticleIndexManager() as index:
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


@worker(name=Worker.SCRAPE_HTMLS)
def scrape_htmls():

    @scrape_htmls.task
    def scrape_entry(entry):
        resp = requests.get(entry.url)
        resp.raise_for_status()
        write(entry.scraped_html_path, resp.text)
        entry.scrape_was_successful = True
        time.sleep(1)

    with CnnArticleIndexManager() as index:
        entries_to_scrape = [
            entry for entry in index.values() if
            not entry[Worker.SCRAPE_HTMLS].has_been_attempted
            # entry[Worker.SCRAPE_HTMLS].status == Report.FAILED
        ]

        log(f'Entries to scrape: {len(entries_to_scrape)}')

        for entry in entries_to_scrape:
            scrape_entry(entry)


@worker(name=Worker.EXTRACT_TEXTS)
def extract_texts():

    @extract_texts.task
    def extract_text(entry):
        soup = bs4.BeautifulSoup(read(entry.scraped_html_path), 'html.parser')
        text = soup.text
        write(entry.extracted_text_path, text)

    with CnnArticleIndexManager() as index:
        entries = [
            entry for entry in index.values() if
            entry[Worker.SCRAPE_HTMLS].status == Report.SUCCESS and
            not entry[Worker.EXTRACT_TEXTS].has_been_attempted
        ]
        log(f'Entries to extract article text from (v2): {len(entries)}')
        for entry in entries:
            extract_text(entry)


@worker(name=Worker.CLEAN_TEXTS)
def clean_texts():

    @clean_texts.task
    def clean_text(entry):
        article = read(entry.extracted_text_path)

        article = re.sub(r'\n\s*\n', '\n', article)
        article = re.sub(r'(http|https):\/\/\S+', '', article)

        write(entry.preprocessed_text_path, article)

    with CnnArticleIndexManager() as index:
        entries = [
            entry for entry in index.values()
            if entry[Worker.EXTRACT_TEXTS].status == Report.SUCCESS
            and not entry[Worker.CLEAN_TEXTS].has_been_attempted
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        for entry in entries:
            clean_text(entry)


@worker(name=Worker.PROCESS_TEXTS)
def process_texts():

    with CnnArticleIndexManager() as index:
        entries = [
            entry for entry in index.values()
            if entry[Worker.CLEAN_TEXTS].status == Report.SUCCESS
        ]

        for entry in entries:
            pass

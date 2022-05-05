import re
import time

import requests  # when JS rendering sites cause trouble -- skip to next news site. we will open up scraper service l8r
import bs4
import schedule
import spacy

from helpers import *
from models import *

nlp = spacy.load('en_core_web_sm')


@worker(name='index_rss_urls')
def index_latest_rss_entries():

    @index_latest_rss_entries.task
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

    @index_latest_rss_entries.task
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

    @index_latest_rss_entries.task
    def index_entry(_index, _topic, _url):
        if _url not in _index and 'cnn.com' in _url[:20]:
            next_file_name = str(len(_index) + 1)

            scraped_html_path = 'data/cnn_articles_html/' + next_file_name + '.html'
            extracted_text_v1_path = 'data/cnn_articles_extracted_texts_v1/' + next_file_name + '.json'
            extracted_text_v2_path = 'data/cnn_articles_extracted_texts_v2/' + next_file_name + '.txt'

            _index[_url] = IndexEntry(
                url=_url,
                topic=_topic,
                reports={
                    'scrape_urls_v1': None,
                    'extract_text_v2': None,
                    'preprocess_extracted_text_v1': None
                },
                scraped_html_path=scraped_html_path,
                extracted_text_v1_path=extracted_text_v1_path,
                extracted_text_v2_path=extracted_text_v2_path
            )
            return 1
        return 0

    with CnnArticleIndex() as index:
        topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
        topics_entries = []

        for topic, rss_url in topics_urls:
            topics_entries.append((topic, feedparser.parse(rss_url).entries))

        count = 0
        for topic, entries in topics_entries:
            for entry in entries:
                count += index_entry(index, topic, entry['link'])

        log(f'New entries indexed: {count}')


@worker(name='scrape_urls_v1')
def scrape_latest_urls_from_index():

    @scrape_latest_urls_from_index.task
    def task(entry):
        resp = requests.get(entry.url)
        resp.raise_for_status()
        write(entry.scraped_html_path, resp.text)

        log(f'Successfully scraped URL: {entry.url}. Saved to: {entry.scraped_html_path}', level='success')
        entry.scrape_was_successful = True

        time.sleep(1)

    with CnnArticleIndex() as index:
        entries_to_scrape = [
            entry for entry in index.values() if
            not entry.reports[scrape_latest_urls_from_index.name].has_been_attempted
        ]

        log(f'Entries to scrape: {len(entries_to_scrape)}')

        for entry in entries_to_scrape:
            task(entry)


@worker(name='extract_text_v2')
def extract_text_from_article_v2():

    @extract_text_from_article_v2.task
    def task(entry):
        soup = bs4.BeautifulSoup(read(entry.scraped_html_path), 'html.parser')
        text = soup.text
        write(entry.extracted_text_v2_path, text)
        log(f'Successfully extracted text from article: {entry.extracted_text_v2_path}')

    with CnnArticleIndex() as index:
        entries = [
            entry for entry in index.values() if
            not entry.reports[extract_text_from_article_v2.name].has_been_attempted
        ]
        log(f'Entries to extract article text from (v2): {len(entries)}')
        for entry in entries:
            task(entry=entry)


@worker(name='preprocess_extracted_text_v1')
def preprocess_extracted_text_v1():

    @preprocess_extracted_text_v1.task
    def task(entry):
        article = ArticleText.load(try_load_json(read(entry.extracted_text_v2_path)))
        article.paragraphs_text = '' \
            .join(article.paragraphs or []) \
            .removeprefix('\n') \
            .replace('\n\n', '\n') \
            .replace('  ', ' ') \
            .removesuffix('\n')

        lines = []
        for line in article.paragraphs_text.split('\n'):
            if not line.endswith('.'):
                lines.append(line + '. ')

        article.paragraphs_text = ''.join(lines)
        write(entry.extracted_text_v1_path, json.dumps(dict(article)))

    with CnnArticleIndex() as index:
        entries = [
            entry for entry in index.values() if
            entry.reports[extract_text_from_article_v2.name].status == Report.SUCCESS and
            not entry.reports[preprocess_extracted_text_v1.name].has_been_attempted
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        for entry in entries:
            task(entry=entry)


@worker()
def pipeline():
    index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article_v2()
    preprocess_extracted_text_v1()


if __name__ == '__main__':
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

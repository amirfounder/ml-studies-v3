import json
import re
import time
from datetime import datetime, timezone
from os import listdir
from os.path import exists

import feedparser
import requests  # when SPA sites cause trouble -- skip to next news site. we will open up scraper service
import bs4
import schedule


CNN_RSS_PAGE_URL = 'https://www.cnn.com/services/rss/'
CNN_RSS_PAGE_LOCAL_PATH = 'data/cnn_rss_html.html'

CNN_MONEY_RSS_PAGE_URL = 'https://money.cnn.com/services/rss/'
CNN_MONEY_RSS_PAGE_LOCAL_PATH = 'data/cnn_money_rss_html.html'

INDEX_PATH = 'data/index.json'
INDEX_V1_PATH = 'data/index_v1.json'
LOGS_PATH = 'data/logs.log'


def read(path, mode='r', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        return f.read()


def write(path, contents, mode='w', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        f.write(contents)


def try_load_json(o):
    try:
        return json.loads(o)
    except Exception:
        return {}


def log(message, level='INFO'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write(LOGS_PATH, message, mode='a')


def worker(func):
    def wrapper(*args, **kwargs):
        log(f'Started worker: {func.__name__}')
        result = func(*args, **kwargs)
        log(f'Finished worker: {func.__name__}')
        return result
    return wrapper


class IndexEntryModel:
    def __init__(self, **kwargs):
        self.url = kwargs.get('url')
        self.has_been_scraped = kwargs.get('has_been_scraped', False)
        self.has_been_processed_with_text_extraction = kwargs.get('has_been_processed', False)
        self.scraped_html_path = kwargs.get('html_path')
        self.scrape_was_successful = kwargs.get('scrape_was_successful')
        self.scrape_error = kwargs.get('scrape_error')
        self.timestamp = kwargs.get('timestamp') or datetime.now(timezone.utc)

    @classmethod
    def from_dict(cls, obj: dict):
        return cls(**obj)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    def __iter__(self):
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                v = v.isoformat()
            yield k, v


def read_cnn_index() -> dict[str, IndexEntryModel]:
    return {k: IndexEntryModel.from_dict(v) for k, v in try_load_json(read(INDEX_PATH)).items()} \
        if exists(INDEX_PATH) else {}


def save_cnn_index(index: dict[str, IndexEntryModel]) -> None:
    write(INDEX_PATH, json.dumps({k: dict(v) for k, v in index.items()}))


@worker
def get_cnn_rss_urls():
    if not exists(CNN_RSS_PAGE_LOCAL_PATH):
        resp = requests.get(CNN_RSS_PAGE_URL)
        resp.raise_for_status()
        write(CNN_RSS_PAGE_LOCAL_PATH, resp.text)

    html = read(CNN_RSS_PAGE_LOCAL_PATH)
    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return list(zip(topics, urls))


@worker
def get_cnn_money_rss_urls():
    if not exists(CNN_MONEY_RSS_PAGE_LOCAL_PATH):
        resp = requests.get(CNN_MONEY_RSS_PAGE_URL)
        resp.raise_for_status()
        write(CNN_MONEY_RSS_PAGE_LOCAL_PATH, resp.text)

    html = read(CNN_MONEY_RSS_PAGE_LOCAL_PATH)
    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/(rss/money_|cnnmoneymorningbuzz)')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = ['cnn_money_' + u.removeprefix('http://rss.cnn.com/rss/').removeprefix('money_')
        .removeprefix('cnnmoneymorningbuzz').removesuffix('.rss') for u in urls]

    return list(zip(topics, urls))


@worker
def index_latest_rss_entries():
    report = {}

    topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
    topics_entries = [(topic, feedparser.parse(url).entries) for topic, url in topics_urls]
    index = read_cnn_index()

    for topic, entries in topics_entries:
        report[topic] = 0
        for entry in entries:
            url: str = entry.get('link')
            if url not in index and 'cnn.com' in url[:20]:
                index[url] = IndexEntryModel(url=url)
                report[topic] += 1

    save_cnn_index(index)

    log('New entries indexed:\n')
    for k, v in report.items():
        log(f'topic: {k} | new entries: {v}')


@worker
def scrape_latest_urls_from_index():
    index = read_cnn_index()
    entries_to_scrape = [entry for entry in index.values() if not entry.has_been_scraped]

    log(f'Entries to scrape: {len(entries_to_scrape)}')

    for entry in entries_to_scrape:
        try:
            resp = requests.get(entry.url)
            resp.raise_for_status()
            html_path = 'data/cnn_articles/' + str(len(listdir('data/cnn_articles')) + 1) + '.html'
            write(html_path, resp.text)

            log(f'Successfully scraped URL: {entry.url}', level='success')
            index[entry.url] = entry.update(
                has_been_scraped=True,
                scrape_was_successful=True,
                scraped_html_path=html_path
            )

        except Exception as e:
            log(f'Exception while scraping URL: {entry.url} - {str(e)}', level='error')
            index[entry.url] = entry.update(
                has_been_scraped=True,
                scrape_was_successful=False,
                scrape_error=str(e)
            )

        save_cnn_index(index)
        time.sleep(2)


@worker
def extract_text_from_article():
    index = read_cnn_index()
    entries_to_process = [
        entry for entry in index.values() if
        not entry.has_been_processed_with_text_extraction and
        entry.scrape_was_successful
    ]

    log(f'Entries to extract article text from: {len(entries_to_process)}')
    for entry in entries_to_process:
        try:
            html_path = entry.scraped_html_path
            html = read(html_path)
            soup = bs4.BeautifulSoup(html)

            articles = soup.find_all('div', {'class': 'Article__primary'})
            if len(articles) != 3:
                msg = f'Unexpected amount of divs with "Article__primary" class. Expected: 3. Got: {len(articles)}'
                raise Exception(msg)

            # TODO - Validation?
            article = articles[1]
            paragraphs = article.find_all('div', {'class': 'Paragraph__component'})
            print(len(paragraphs))

        except Exception as e:
            log(f'Exception occurred : {str(e)}')


@worker
def workflow():
    index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article()


if __name__ == '__main__':
    workflow()
    schedule.every(30).minutes.do(workflow)

    while True:
        schedule.run_pending()
        time.sleep(60)

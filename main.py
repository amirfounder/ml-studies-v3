import json
import re
import time
from datetime import datetime
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
LOGS_PATH = 'data/logs.log'


def next_cnn_article_path():
    pass


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


def index_latest_entries_from_rss():
    now = datetime.now()
    topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
    topics_entries = [(topic, feedparser.parse(url).entries) for topic, url in topics_urls]

    index = try_load_json(read(INDEX_PATH)) if exists(INDEX_PATH) else {}
    report = {}

    for topic, entries in topics_entries:
        report[topic] = 0
        for entry in entries:
            url: str = entry.get('link')
            if url not in index and 'cnn.com' in url[:20]:
                index[url] = {
                    'url': url,
                    'has_been_scraped': False,
                    'html_path': None,
                    'scrape_was_successful': None,
                    'scrape_exception': None,
                    'year': now.year,
                    'month': now.month,
                    'day': now.day,
                    'hour': now.hour,
                    'minute': now.minute,
                    'second': now.second
                }
                report[topic] += 1

    write(INDEX_PATH, json.dumps(index))
    print('New entries indexed:\n')
    for k, v in report.items():
        print(f'topic: {k} | new entries: {v}')


def scrape_newest_entries():
    index = try_load_json(read(INDEX_PATH)) if exists(INDEX_PATH) else {}
    entries_to_scrape = [entry for entry in index.values() if not entry['has_been_scraped']]

    log(f'Entries to scrape: {len(entries_to_scrape)}')

    for entry in entries_to_scrape:
        try:
            resp = requests.get(entry['url'])
            resp.raise_for_status()
            html_path = 'data/cnn_articles/' + str(len(listdir('data/cnn_articles')) + 1) + '.html'
            write(html_path, resp.text)

            log(f'Successfully scraped URL: {entry["url"]}', level='success')
            index[entry['url']].update({
                'has_been_scraped': True,
                'html_path': html_path,
                'scrape_was_successful': True
            })

        except Exception as e:
            log(f'Exception while scraping URL: {entry["url"]} - {str(e)}', level='error')
            index[entry['url']].update({
                'has_been_scraped': True,
                'scrape_was_successful': False,
                'exception': str(e)
            })

        write(INDEX_PATH, json.dumps(index))
        time.sleep(2)


def extract_text_from_article():
    pass


def workflow():


    index_latest_entries_from_rss()
    scrape_newest_entries()


if __name__ == '__main__':
    workflow()
    schedule.every(30).minutes.do(workflow)

    while True:
        schedule.run_pending()
        time.sleep(60)

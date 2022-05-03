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
    write('data/logs.log', message, mode='a')


def get_cnn_rss_urls():
    url = 'https://www.cnn.com/services/rss/'
    path = 'data/cnn_rss_html.html'

    if not exists(path):
        html = requests.get(url).text
        write(path, html)
    else:
        html = read(path)

    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return list(zip(topics, urls))


def get_cnn_money_rss_urls():
    path = 'data/cnn_money_rss_html.html'
    url = 'https://money.cnn.com/services/rss/'

    if not exists(path):
        html = requests.get(url).text
        write(path, html)
    else:
        html = read(path)

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

    path = 'data/index.json'

    index = try_load_json(read(path)) if exists(path) else {}
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

    write(path, json.dumps(index))
    print('New entries indexed:\n')
    for k, v in report.items():
        print(f'topic: {k} | new entries: {v}')


def scrape_newest_entries():
    path = 'data/index.json'

    index = try_load_json(read(path)) if exists(path) else {}
    entries_to_scrape = [entry for entry in index.values() if not entry['has_been_scraped']]

    log(f'Entries to scrape: {len(entries_to_scrape)}')

    for entry in entries_to_scrape:
        try:
            resp = requests.get(entry['url'])
            resp.raise_for_status()
            html_path = 'data/cnn_articles/' + str(len(listdir('data/cnn_articles')) + 1) + '.html'
            write(html_path, resp.text)

            index[entry['url']].update({
                'has_been_scraped': True,
                'html_path': html_path,
                'scrape_was_successful': True
            })

            write(path, json.dumps(index))
            log(f'Successfully scraped URL: {entry["url"]}', level='success')

        except Exception as e:
            log(f'Exception while scraping URL: {entry["url"]} - {str(e)}', level='error')
            index[entry['url']].update({
                'has_been_scraped': True,
                'scrape_was_successful': False,
                'exception': str(e)
            })

        time.sleep(2)


def workflow():


    index_latest_entries_from_rss()
    scrape_newest_entries()


if __name__ == '__main__':
    workflow()
    schedule.every(30).minutes.do(workflow)

    while True:
        schedule.run_pending()
        time.sleep(60)

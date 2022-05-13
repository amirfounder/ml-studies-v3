from datetime import datetime, timedelta
import re
from os.path import exists

import bs4
import feedparser
import requests
from spacy.tokens import Token

from ..commons import write, read
from ..decorators import subtask
from ..enums import Paths


@subtask(silent_success=True, silent_start=True)
def scrape_rss_entries(rss_url) -> list[dict]:
    return feedparser.parse(rss_url).entries


@subtask(silent_success=True, silent_start=True)
def get_cnn_rss_urls():
    path = Paths.CNN_RSS_HTML_OUTPUT.format()
    if not exists(path):
        resp = requests.get('https://www.cnn.com/services/rss/')
        resp.raise_for_status()
        write(path, resp.text)

    soup = bs4.BeautifulSoup(read(path), 'html.parser')
    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return list(zip(topics, urls))


@subtask(silent_success=True, silent_start=True)
def get_cnn_money_rss_urls():
    path = Paths.CNN_MONEY_RSS_HTML_OUTPUT.format()
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

import re
from os.path import exists

import requests
import bs4


def get_cnn_rss_urls():
    url = 'https://www.cnn.com/services/rss/'
    path = 'data/cnn_rss_html.html'

    if not exists(path):
        html = requests.get(url).text
        open(path, 'w').write(html)
    else:
        html = open(path, 'r').read()

    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


def get_cnn_money_rss_urls():
    path = 'data/cnn_money_rss_html.html'
    url = 'https://money.cnn.com/services/rss/'

    if not exists(path):
        html = requests.get(url).text
        open(path, 'w').write(html)
    else:
        html = open(path, 'r').read()

    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/(rss/money_|cnnmoneymorningbuzz)')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = ['cnn_money_' + u.removeprefix('http://rss.cnn.com/rss/money_')
        .removeprefix('http://rss.cnn.com/cnnmoneymorningbuzz').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


rss_feeds = {}

rss_feeds.update(get_cnn_rss_urls())
rss_feeds.update(get_cnn_money_rss_urls())

print(rss_feeds)

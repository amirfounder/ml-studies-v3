import re
from os.path import exists

import requests
import bs4

CNN_RSS = 'https://www.cnn.com/services/rss/'
CNN_MONEY_RSS = 'https://money.cnn.com/services/rss/'


def get_cnn_rss_urls():
    path = 'data/cnn_rss_html.html'

    if not exists(path):
        html = requests.get(CNN_MONEY_RSS).text
        with open(path, 'w') as f:
            f.write(html)
    else:
        with open(path, 'r') as f:
            html = f.read()

    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


def get_cnn_money_rss_urls():
    path = 'data/cnn_money_rss_html.html'

    if not exists(path):
        html = requests.get(CNN_MONEY_RSS).text
        with open(path, 'w') as f:
            f.write(html)
    else:
        with open(path, 'r') as f:
            html = f.read()

    soup = bs4.BeautifulSoup(html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/(rss/money_|cnnmoneymorningbuzz)')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = ['cnn_money_' + u.removeprefix('http://rss.cnn.com/rss/money_').removeprefix('cnnmoneymorningbuzz')
        .removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


rss_feeds = {}

rss_feeds.update(get_cnn_rss_urls())
rss_feeds.update(get_cnn_money_rss_urls())

print(rss_feeds)

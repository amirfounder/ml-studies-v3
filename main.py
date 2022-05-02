import re
from os.path import exists

import requests
import bs4


def read(path, mode='r'):
    with open(path, mode) as f:
        return f.read()


def write(path, contents, mode='w'):
    with open(path, mode) as f:
        f.write(contents)


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

    return dict(zip(topics, urls))


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
    topics = ['cnn_money_' + u.removeprefix('http://rss.cnn.com/rss/money_')
        .removeprefix('http://rss.cnn.com/cnnmoneymorningbuzz').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


def main():
    rss_feeds = {}
    rss_feeds.update(get_cnn_rss_urls())
    rss_feeds.update(get_cnn_money_rss_urls())

    print(rss_feeds)


if __name__ == '__main__':
    main()

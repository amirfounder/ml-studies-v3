import re
import requests
import bs4

CNN_RSS = 'https://www.cnn.com/services/rss/'
CNN_MONEY_RSS = 'https://money.cnn.com/services/rss/'


def get_cnn_rss_urls():
    cnn_rss_html = requests.get(CNN_RSS).text

    with open('data/cnn_rss_html.html', 'w') as f:
        f.write(cnn_rss_html)

    soup = bs4.BeautifulSoup(cnn_rss_html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/rss/')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))


def get_cnn_money_rss_urls():
    get_cnn_rss_urls()

    cnn_money_rss_html = requests.get(CNN_MONEY_RSS).text

    with open('data/cnn_money_rss_html.html', 'w') as f:
        f.write(cnn_money_rss_html)

    soup = bs4.BeautifulSoup(cnn_money_rss_html, 'html.parser')

    tags = soup.find_all('a', {'href': re.compile(r'^http://rss.cnn.com/(rss/money_|cnnmoneymorningbuzz)')})
    urls = list(set([t.attrs['href'] for t in tags]))
    topics = [u.removeprefix('http://rss.cnn.com/rss/money_').removeprefix('cnnmoneymorningbuzz').removesuffix('.rss') for u in urls]

    return dict(zip(topics, urls))

get_cnn_money_rss_urls()

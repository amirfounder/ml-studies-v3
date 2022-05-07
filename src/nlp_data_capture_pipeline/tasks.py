import pickle
from os.path import exists
import re
import time

import bs4
import requests

from ..commons import write, read, nlp
from ..decorators import subtask, task
from ..enums import OutputPaths
from ..models import IndexEntry


@subtask
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


@subtask
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


@task
def extract_text(entry: IndexEntry):
    input_path = OutputPaths.SCRAPE_HTMLS.value.format(entry.output_filename)
    output_path = OutputPaths.EXTRACT_TEXTS.value.format(entry.output_filename)

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')
    write(output_path, soup.text)


@task
def scrape_html(entry: IndexEntry):
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(OutputPaths.SCRAPE_HTMLS.format(entry.output_filename), resp.text)
    time.sleep(1)


@task
def process_text(entry: IndexEntry):
    doc = nlp(read(entry.reports[scrape_html.__name__].output_path))
    with open(entry.reports[process_text.__name__].output_path, 'wb') as f:
        pickle.dump(doc, f)




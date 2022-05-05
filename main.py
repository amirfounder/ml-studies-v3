import re
import time
from os import listdir
from threading import Thread

import requests  # when JS rendering sites cause trouble -- skip to next news site. we will open up scraper service l8r
import bs4
import schedule
import spacy

from helpers import *
from models import *

CNN_RSS_PAGE_URL = 'https://www.cnn.com/services/rss/'
CNN_RSS_PAGE_LOCAL_PATH = 'data/cnn_rss_html.html'

CNN_MONEY_RSS_PAGE_URL = 'https://money.cnn.com/services/rss/'
CNN_MONEY_RSS_PAGE_LOCAL_PATH = 'data/cnn_money_rss_html.html'

nlp = spacy.load('en_core_web_sm')


@worker()
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


@worker()
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


@worker(name='index_rss_urls')
def index_latest_rss_entries():
    with CnnArticleIndex() as index:
        report = {}
        topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
        threads = []
        topics_entries = [None] * len(topics_urls)
        for i, (topic, url) in enumerate(topics_urls):
            thread = Thread(target=get_entries_from_rss_url, args=(topics_entries, i, topic, url))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        for topic, entries in topics_entries:
            report[topic] = 0
            for entry in entries:
                url: str = entry.get('link')
                if url not in index and 'cnn.com' in url[:20]:

                    next_file_name = str(len(listdir('data/cnn_articles_html')) + 1)
                    scraped_html_path = 'data/cnn_articles_html/' + next_file_name + '.html'
                    extracted_text_v1_path = 'data/cnn_articles_extracted_texts_v1/' + next_file_name + '.json'
                    extracted_text_v2_path = 'data/cnn_articles_extracted_texts_v2/' + next_file_name + '.txt'

                    index[url] = IndexEntry(
                        url=url,
                        datetime_indexed=datetime.now(timezone.utc),
                        scraped_html_path=scraped_html_path,
                        extracted_text_v1_path=extracted_text_v1_path,
                        extracted_text_v2_path=extracted_text_v2_path
                    )
                    report[topic] += 1

        log('New entries indexed:\n')
        for k, v in report.items():
            log(f'topic: {k} | new entries: {v}')


@worker(name='scrape_urls_v1')
def scrape_latest_urls_from_index():

    @scrape_latest_urls_from_index.task
    def task(entry):
        resp = requests.get(entry.url)
        resp.raise_for_status()
        write(entry.scraped_html_path, resp.text)

        log(f'Successfully scraped URL: {entry.url}', level='success')
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


@worker(name='extract_text_v1')
def extract_text_from_article_v1():

    @extract_text_from_article_v1.task
    def task(entry, report: Report):
        soup = bs4.BeautifulSoup(read(entry.scraped_html_path), 'html.parser')
        cnn_selector_map = {
            'category_a': {
                'outer': [
                    ('div', {'class': 'Article__content'}),
                    ('div', {'class': 'BasicArticle__body'})
                ],
                'inner': ('div', {'class': 'Paragraph__component'})
            },
            'category_b': {
                'outer': [
                    ('div', {'class': 'pg-rail-tall__body'}),
                    ('div', {'class': 'pg-special-article__body'})
                ],
                'inner': ('div', {'class': 'zn-body__paragraph'})
            },
            'category_c': {
                'outer': [
                    ('div', {'class': 'SpecialArticle__body'})
                ],
                'inner': ('div', {'class': 'SpecialArticle__paragraph'})
            },
            'category_d': {
                'outer': [
                    ('div', {'class': 'article__content'})
                ],
                'inner': ('p', {'class': 'paragraph'})
            },
            'category_e': {
                'outer': [
                    ('div', {'id': 'storycontent'}),
                    ('div', {'class': 'content-container'})
                ],
                'inner': ('p',)
            }
        }

        flag = False

        for strategy, selectors_map in cnn_selector_map.items():
            article = None

            for i, outer_selector in enumerate(selectors_map.get('outer')):
                if len(articles := soup.find_all(*outer_selector)) == 1:
                    article = articles[0]
                    strategy += str(i + 1)

            if not article:
                continue

            article_text = ArticleText(paragraphs=[p.text for p in article.find_all(*selectors_map.get('inner'))])
            write(entry.extracted_text_v1_path, json.dumps(dict(article_text)))
            report.additional_data['strategy_used'] = 'cnn_' + strategy

            log(f'Successfully extracted paragraphs - {entry.extracted_text_v1_path}', level='success')

            flag = True
            break

        if not flag:
            log(f'No strategy in place to parse this HTML document - {entry.scraped_html_path}', level='info')
            report.status = report.FAILED
            report.additional_data['strategy_used'] = 'No strategy found'

    with CnnArticleIndex() as cnn_article_index:
        entries = [
            entry for entry in cnn_article_index.values() if
            entry.reports[extract_text_from_article_v1.name].status == Report.FAILED and  # comment this to resync
            entry.reports[scrape_latest_urls_from_index.name].status == Report.SUCCESS and
            'cnn.com/audio' not in entry.url and
            'cnn.com/videos' not in entry.url and
            'cnn.com/specials' not in entry.url and
            'cnn.com/video' not in entry.url and
            'cnn.com/gallery' not in entry.url and
            'cnn.com/interactive' not in entry.url and
            'cnn.com/infographic' not in entry.url and
            'cnn.com/calculator' not in entry.url and
            'cnn.com/election' not in entry.url and
            'live-news' not in entry.url
        ]

        log(f'Entries to extract article text from (v1): {len(entries)}')

        for entry in entries:
            task(entry)


@worker(name='extract_text_v2')
def extract_text_from_article_v2():

    @extract_text_from_article_v2.task
    def task(entry):
        soup = bs4.BeautifulSoup(read(entry.scraped_html_path), 'html.parser')
        text = soup.text
        write(entry.extracted_text_v2_path, text)

    with CnnArticleIndex() as index:
        entries = [entry for entry in index.values()]
        log(f'Entries to extract article text from (v2): {len(entries)}')
        [task(entry) for entry in entries]


@worker(name='preprocess_extracted_text_v1')
def preprocess_extracted_text_v1():

    @preprocess_extracted_text_v1.task
    def task(entry):
        article = ArticleText.from_dict(try_load_json(read(entry.extracted_text_v1_path)))
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
            entry.reports[extract_text_from_article_v1.name].status == Report.SUCCESS
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        for entry in entries:
            task(entry)


@worker()
def pipeline():
    # index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article_v1()
    preprocess_extracted_text_v1()


if __name__ == '__main__':
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

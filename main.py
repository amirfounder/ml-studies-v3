from datetime import timezone
import re
import time
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

                    index[url] = IndexEntryModel(
                        url=url,
                        datetime_indexed=datetime.now(timezone.utc),
                        scraped_html_path=scraped_html_path,
                        extracted_text_v1_path=extracted_text_v1_path
                    )
                    report[topic] += 1

        log('New entries indexed:\n')
        for k, v in report.items():
            log(f'topic: {k} | new entries: {v}')


@worker
def scrape_latest_urls_from_index():

    def task(entry):
        entry.has_scraping_been_attempted = True
        entry.datetime_scraped = datetime.now(timezone.utc)

        try:
            resp = requests.get(entry.url)
            resp.raise_for_status()
            write(entry.scraped_html_path, resp.text)

            log(f'Successfully scraped URL: {entry.url}', level='success')
            entry.scrape_was_successful = True

        except Exception as e:
            log(f'Exception while scraping URL: {entry.url} - {str(e)}', level='error')
            entry.scrape_was_successful = False
            entry.scrape_error = str(e)

        entry.has_scraping_been_attempted = True
        entry.datetime_scraped = datetime.now(timezone.utc)
        time.sleep(1)

    with CnnArticleIndex() as index:
        entries_to_scrape = [entry for entry in index.values() if not entry.has_scraping_been_attempted]
        log(f'Entries to scrape: {len(entries_to_scrape)}')
        # run_concurrently([(task, entry, None) for entry in entries_to_scrape])
        [task(entry) for entry in entries_to_scrape]


@worker
def extract_text_from_article_v1():

    def task(entry):
        entry.has_text_extraction_v1_been_attempted = True
        entry.datetime_v1_text_extracted = datetime.now(timezone.utc)

        try:
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

            # Every entry will loop over the selector strategy map to find the right strategy for text extraction.
            # If it can't find the right outer strategy, it is logged with level CRITICAL
            for strategy, selectors_map in cnn_selector_map.items():
                article = None

                outer_selectors = selectors_map.get('outer')
                inner_selector = selectors_map.get('inner')

                for i, outer_selector in enumerate(outer_selectors):
                    if len(articles := soup.find_all(*outer_selector)) == 1:
                        article = articles[0]
                        strategy += str(i + 1)

                if not article:
                    continue

                paragraphs = article.find_all(*inner_selector)
                article_text = ArticleText(paragraphs=[p.text for p in paragraphs])

                write(entry.extracted_text_v1_path, json.dumps(dict(article_text)))

                entry.text_extraction_v1_was_successful = True
                entry.text_extraction_v1_strategy_used = 'cnn_' + strategy
                entry.text_extraction_v1_error = None

                log(f'Successfully extracted paragraphs - {entry.extracted_text_v1_path}', level='success')
                break

            if not entry.text_extraction_v1_was_successful:
                log(f'No strategy in place to parse this HTML document - {entry.scraped_html_path}', level='info')
                entry.text_extraction_v1_was_successful = False
                entry.text_extraction_v1_error = 'No strategy in place to parse this HTML document'
                entry.text_extraction_v1_strategy_used = None

        except Exception as e:
            log(f'Exception occurred : {str(e)}', level='error')
            entry.text_extraction_v1_was_successful = False
            entry.text_extraction_v1_error = str(e)
            entry.text_extraction_v1_strategy_used = None

    with CnnArticleIndex() as cnn_article_index:
        entries = [
            entry for entry in cnn_article_index.values() if
            not entry.text_extraction_v1_was_successful and  # comment this to resync
            entry.scrape_was_successful and
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
        # run_concurrently([(task, entry, None) for entry in entries])
        [task(entry) for entry in entries]


@worker
def extract_text_from_article_v2():
    
    def task(entry):
        entry.has_text_extraction_v2_been_attempted = True
        entry.datetime_v2_text_extracted = datetime.now(timezone.utc)

        try:
            soup = bs4.BeautifulSoup(read(entry.scraped_html_path), 'html.parser')
            text = soup.text
            write(entry.extracted_text_v2_path, text)

            entry.text_extraction_v2_was_successful = True
            entry.text_extraction_v2_error = None

        except Exception as e:
            log(f'Exception occurred running "extract_text_from_article_v2" : {str(e)}')
            entry.text_extraction_v2_error = str(e)
            entry.text_extraction_v2_was_successful = False

    with CnnArticleIndex() as index:
        entries = [entry for entry in index.values()]
        log(f'Entries to extract article text from (v2): {len(entries)}')
        [task(entry) for entry in entries]


@worker
def preprocess_extracted_text():

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

        doc = nlp(article.paragraphs_text)
        pass

    with CnnArticleIndex() as index:
        entries = [
            entry for entry in index.values() if
            entry.text_extraction_v1_was_successful
        ]
        log(f'Entries to preprocess extracted text from : {len(entries)}')
        # run_concurrently([(task, entry, None) for entry in entries])
        [task(entry) for entry in entries]


@worker
def pipeline():
    index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article_v1()
    preprocess_extracted_text()


if __name__ == '__main__':
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

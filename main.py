from datetime import timezone
import re
import time

import feedparser
import requests  # when SPA sites cause trouble -- skip to next news site. we will open up scraper service
import bs4
import schedule

from helpers import *
from models import *

CNN_RSS_PAGE_URL = 'https://www.cnn.com/services/rss/'
CNN_RSS_PAGE_LOCAL_PATH = 'data/cnn_rss_html.html'

CNN_MONEY_RSS_PAGE_URL = 'https://money.cnn.com/services/rss/'
CNN_MONEY_RSS_PAGE_LOCAL_PATH = 'data/cnn_money_rss_html.html'


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
    report = {}

    topics_urls = [*get_cnn_rss_urls(), *get_cnn_money_rss_urls()]
    topics_entries = [(topic, feedparser.parse(url).entries) for topic, url in topics_urls]
    index = read_cnn_article_index()

    for topic, entries in topics_entries:
        report[topic] = 0
        for entry in entries:
            url: str = entry.get('link')
            if url not in index and 'cnn.com' in url[:20]:
                index[url] = IndexEntryModel(url=url, datetime_indexed=datetime.now(timezone.utc))
                report[topic] += 1

    save_cnn_article_index(index)

    log('New entries indexed:\n')
    for k, v in report.items():
        log(f'topic: {k} | new entries: {v}')


@worker
def scrape_latest_urls_from_index():
    index = read_cnn_article_index()
    entries_to_scrape = [entry for entry in index.values() if not entry.has_scraping_been_attempted]

    log(f'Entries to scrape: {len(entries_to_scrape)}')

    for entry in entries_to_scrape:

        entry.has_scraping_been_attempted = True
        entry.datetime_scraped = datetime.now(timezone.utc)

        try:
            resp = requests.get(entry.url)
            resp.raise_for_status()
            html_path = 'data/cnn_articles_html/' + str(len(listdir('data/cnn_articles_html')) + 1) + '.html'
            write(html_path, resp.text)

            log(f'Successfully scraped URL: {entry.url}', level='success')
            entry.scrape_was_successful = True
            entry.scraped_html_path = html_path

        except Exception as e:
            log(f'Exception while scraping URL: {entry.url} - {str(e)}', level='error')
            entry.scrape_was_successful = False
            entry.scrape_error = str(e)

        entry.has_scraping_been_attempted = True
        entry.datetime_scraped = datetime.now(timezone.utc)

    save_cnn_article_index(index)
    time.sleep(2)


@worker
def extract_text_from_article():
    cnn_article_index = read_cnn_article_index()

    entries_to_process = [
        entry for entry in cnn_article_index.values() if
        not entry.text_extraction_was_successful and  # comment this to resync
        entry.scrape_was_successful and
        'cnn.com/audio' not in entry.url and
        'cnn.com/videos' not in entry.url and
        'cnn.com/specials' not in entry.url and
        'cnn.com/video' not in entry.url and
        'cnn.com/gallery' not in entry.url and
        'cnn.com/interactive' not in entry.url and
        'cnn.com/infographic' not in entry.url and
        'cnn.com/calculator' not in entry.url and
        'live-news' not in entry.url
    ]

    log(f'Entries to extract article text from: {len(entries_to_process)}')
    for entry in entries_to_process:

        entry.has_text_extraction_been_attempted = True
        entry.datetime_text_extracted = datetime.now(timezone.utc)

        try:
            html_path = entry.scraped_html_path
            html = read(html_path)
            soup = bs4.BeautifulSoup(html, 'html.parser')
            
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
                extracted_article_text = ExtractedArticleText(paragraphs=[p.text for p in paragraphs])

                path = entry.scraped_html_path
                path = path.replace('cnn_articles_html', 'cnn_articles_extracted_texts')
                path = path.replace('.html', '.json')
                write(path, json.dumps(dict(extracted_article_text)))

                entry.text_extraction_was_successful = True
                entry.extracted_text_path = path
                entry.text_extraction_strategy_used = 'cnn_' + strategy
                entry.text_extraction_error = None

                log(f'Successfully extracted paragraphs - {entry.extracted_text_path}', level='success')
                break
            
            if not entry.has_text_extraction_been_attempted:
                log(f'No parser in place to parse this HTML document - {entry.scraped_html_path}', level='critical')
                entry.text_extraction_was_successful = False
                entry.text_extraction_error = 'No parser in place to parse this HTML document'
                entry.extracted_text_path = None
        
        except Exception as e:
            log(f'Exception occurred : {str(e)}', level='error')
            entry.text_extraction_was_successful = False
            entry.text_extraction_error = str(e)

    save_cnn_article_index(cnn_article_index)


@worker
def workflow():
    index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article()


if __name__ == '__main__':
    workflow()
    schedule.every(30).minutes.do(workflow)

    while True:
        schedule.run_pending()
        time.sleep(60)

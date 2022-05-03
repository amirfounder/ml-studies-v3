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
    entries_to_scrape = [entry for entry in index.values() if not entry.has_been_scraped]

    log(f'Entries to scrape: {len(entries_to_scrape)}')

    for entry in entries_to_scrape:
        try:
            resp = requests.get(entry.url)
            resp.raise_for_status()
            html_path = 'data/cnn_articles_html/' + str(len(listdir('data/cnn_articles_html')) + 1) + '.html'
            write(html_path, resp.text)

            log(f'Successfully scraped URL: {entry.url}', level='success')
            index[entry.url] = entry.update(
                scrape_was_successful=True,
                scraped_html_path=html_path,
            )

        except Exception as e:
            log(f'Exception while scraping URL: {entry.url} - {str(e)}', level='error')
            index[entry.url] = entry.update(
                scrape_was_successful=False,
                scrape_error=str(e),
            )

        index[entry.url] = entry.update(
            has_been_scraped=True,
            datetime_scraped=datetime.now(timezone.utc)
        )
        save_cnn_article_index(index)
        time.sleep(2)


@worker
def extract_text_from_article():
    cnn_article_index = read_cnn_article_index()

    entries_to_process = [
        entry for entry in cnn_article_index.values() if
        not entry.text_extraction_was_successful and
        entry.scrape_was_successful
    ]

    log(f'Entries to extract article text from: {len(entries_to_process)}')
    for entry in entries_to_process:
        try:
            html_path = entry.scraped_html_path
            html = read(html_path)
            soup = bs4.BeautifulSoup(html, 'html.parser')

            # Category A article
            if len(articles := soup.find_all('div', {'class': 'Article__content'})) == 1:
                article = articles[0]
                paragraphs = article.find_all('div', {'class': 'Paragraph__component'})
                related_articles = article.find_all('div', {'class': 'RelatedArticle__component'})

                extracted_article_text = ExtractedArticleText(
                    paragraphs=[p.text for p in paragraphs],
                    related_articles=[ra.text for ra in related_articles]
                )

                path = entry.scraped_html_path
                path = path.replace('cnn_articles_html', 'cnn_articles_extracted_texts')
                path = path.replace('.html', '.json')
                write(path, json.dumps(dict(extracted_article_text)))

                entry.text_extraction_was_successful = True
                entry.text_extraction_path = path

                log(f'Successfully extracted paragraphs and related articles - {entry.text_extraction_path}',
                    level='success')

            # Category B Article
            elif len(articles := soup.find_all('div', {'class': 'pg-rail-tall__body'})) == 1:
                article = articles[0]
                paragraphs = article.find_all('div', {'class': 'zn-body__paragraph'})

                extracted_article_text = ExtractedArticleText(paragraphs=[p.text for p in paragraphs])

                path = entry.scraped_html_path
                path = path.replace('cnn_articles_html', 'cnn_articles_extracted_texts')
                path = path.replace('.html', '.json')
                write(path, json.dumps(dict(extracted_article_text)))

                entry.text_extraction_was_successful = True
                entry.text_extraction_path = path

                log(f'Successfully extracted paragraphs - {entry.text_extraction_path}', level='success')

            # Does not fall under any Category (Unparseable!)
            else:
                entry.text_extraction_was_successful = False
                entry.text_extraction_error = 'No parser in place to parse HTML document'
                log(f'No parser in place to parse HTML document - {entry.scraped_html_path}', level='info')

        except Exception as e:
            log(f'Exception occurred : {str(e)}')
            entry.text_extraction_was_successful = False
            entry.text_extraction_error = str(e)

        entry.has_text_been_extracted = True
        entry.datetime_text_extracted = datetime.now(timezone.utc)

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

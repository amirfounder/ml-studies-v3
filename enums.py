from enum import Enum


class WorkerNames(Enum):
    SCRAPE_HTMLS = 'scrape_htmls'
    EXTRACT_TEXTS = 'extract_texts'
    PROCESS_TEXTS = 'process_texts'


class OutputPaths(Enum):
    SCRAPE_HTMLS = 'data/cnn_articles_html/{}.html'
    EXTRACT_TEXTS = 'data/cnn_articles_extracted_texts/{}.json'
    PROCESS_TEXTS = 'data/cnn_articles_processed_texts/{}.csv'


class Status(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'

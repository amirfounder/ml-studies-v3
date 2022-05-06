from enum import Enum


class Worker(Enum):
    SCRAPE_HTMLS = 'scrape_htmls'
    EXTRACT_TEXTS = 'extract_texts'
    PREPROCESS_TEXTS = 'preprocess_texts'
    PROCESS_TEXTS = 'process_texts'

from helpers import worker
from workers import (
    index_rss_entries,
    extract_texts,
    scrape_htmls,
    clean_texts,
)


@worker()
def pipeline():
    index_rss_entries()
    scrape_htmls()
    extract_texts()
    clean_texts()

from ..decorators import pipeline
from .workers import (
    index_rss_entries,
    extract_texts,
    scrape_htmls,
    process_texts,
)


@pipeline
def nlp_data_capture_pipeline():
    index_rss_entries()
    scrape_htmls()
    extract_texts()
    process_texts()

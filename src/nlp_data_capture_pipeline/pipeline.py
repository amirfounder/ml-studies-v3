from ..decorators import pipeline
from .workers import (
    index_rss_entries,
    extract_texts,
    scrape_htmls,
    process_texts,
)


@pipeline
def nlp_data_capture_pipeline():
    index_rss_entries()  # Indexes Latest RSS Entries
    scrape_htmls()  # Scrapes the HTML files of the URLs just scraped
    extract_texts()  # Extracts text from HTML
    process_texts()  # Processes each text



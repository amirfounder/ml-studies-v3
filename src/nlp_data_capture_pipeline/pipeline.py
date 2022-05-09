from ..decorators import pipeline
from .workers import (
    index_newest_articles,
    scrape_articles,
    extract_texts,
    process_texts,
    create_wordclouds,
)


@pipeline
def nlp_data_capture_pipeline():
    index_newest_articles()
    scrape_articles()
    extract_texts()
    process_texts()
    create_wordclouds()

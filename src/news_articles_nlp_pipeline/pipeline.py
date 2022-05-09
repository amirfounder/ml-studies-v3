from ..decorators import pipeline
from .workers import (
    index_newest_articles,
    scrape_articles,
    extract_texts,
    process_texts,
    create_wordclouds,
    create_sentiment_analyses,
    create_emotional_analyses,
    create_n_gram_analyses,
    create_summaries,
)


@pipeline
def news_articles_nlp_pipeline():
    index_newest_articles()
    scrape_articles()
    extract_texts()
    process_texts()
    create_wordclouds()
    create_sentiment_analyses()
    create_emotional_analyses()
    create_n_gram_analyses()
    create_summaries()

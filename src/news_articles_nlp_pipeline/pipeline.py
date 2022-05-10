from ..decorators import pipeline
from .workers import (
    index_newest_articles,
    scrape_articles,
    extract_texts,
    process_texts,
    create_wordclouds,
    create_sentiment_analyses,
    create_n_gram_analyses,
    create_summaries,
)
from ..env import is_env_prod, is_env_dev


@pipeline
def news_articles_nlp_pipeline():
    if is_env_prod():
        index_newest_articles()
        scrape_articles()
        extract_texts()
        process_texts()
        create_wordclouds()
        create_sentiment_analyses()
        create_n_gram_analyses()
        create_summaries()
    elif is_env_dev():
        extract_texts()
        # process_texts()
        create_wordclouds()
        create_sentiment_analyses()
        create_n_gram_analyses()
        create_summaries()

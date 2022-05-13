from ..decorators import pipeline
from .workers import (
    index_newest_articles,
    scrape_articles,
    extract_texts,
    analyze_texts,
    create_sentiment_analyses,
    create_summaries,
)
from ..env import is_env_prod


@pipeline
def news_articles_nlp_pipeline():
    if is_env_prod():
        index_newest_articles()
        scrape_articles()

    extract_texts()
    analyze_texts()
    create_sentiment_analyses()
    create_summaries()

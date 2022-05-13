from enum import Enum

from .env import working_env


class BaseEnum(Enum):
    def format(self, **kwargs):
        kwargs.update({'env': working_env() or 'no-env'})
        return self.value.format(**kwargs)

    def __str__(self):
        return self.format()


class Pipelines(BaseEnum):
    NEWS_ARTICLES_NLP = 'news-articles-nlp'


class ReportTypes(BaseEnum):
    SCRAPE_ARTICLE = 'scrape_articles'
    EXTRACT_TEXT = 'extract_texts'
    ANALYZE_TEXT = 'analyze_texts'
    CREATE_SENTIMENT_ANALYSIS = 'create_sentiment_analysis'
    CREATE_SUMMARY = 'create_summary'


class Paths(BaseEnum):
    LOGGING = 'data/{env}/news-articles-nlp/logs.log'
    ARTICLES_INDEX = 'data/{env}/news-articles-nlp/index.json'
    SENTENCES_INDEX = 'data/{env}/news-articles-nlp/sentence-index.json'

    SCRAPE_HTMLS_OUTPUT = 'data/{env}/news-articles-nlp/articles//{source}/html/{filename}.html'
    EXTRACT_TEXTS_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/extracted/{filename}.txt'
    ANALYZE_TEXTS_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/analyzed/{filename}.json'
    SENTIMENT_ANALYSES_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/sentiment-analyses/{filename}.json'
    SUMMARIES_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/summaries/{filename}.txt'

    CNN_MONEY_RSS_HTML_OUTPUT = 'data/{env}/news-articles-nlp/static/cnn-money-rss-page.html'
    CNN_RSS_HTML_OUTPUT = 'data/{env}/news-articles-nlp/static/cnn-rss-page.html'


class Status(BaseEnum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'

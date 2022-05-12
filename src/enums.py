from enum import Enum

from .env import env


class ReportTypes(Enum):
    SCRAPE_ARTICLE = 'scrape_articles'
    EXTRACT_TEXT = 'extract_texts'
    ANALYZE_TEXT = 'analyze_texts'
    CREATE_SENTIMENT_ANALYSIS = 'create_sentiment_analysis'
    CREATE_SUMMARY = 'create_summary'


class Paths(Enum):
    LOGGING = 'data/{env}/{pipeline}/logs.log'
    ARTICLES_INDEX = 'data/{env}/news-articles-nlp/index.json'
    SENTENCES_INDEX = 'data/{env}/news-articles-nlp/sentence-index.json'

    SCRAPE_HTMLS_OUTPUT = 'data/{env}/news-articles-nlp/articles//{source}/html/{filename}.html'
    EXTRACT_TEXTS_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/extracted/{filename}.txt'
    ANALYZE_TEXTS_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/analyzed/{filename}.json'
    SENTIMENT_ANALYSES_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/sentiment-analyses/{filename}.json'
    SUMMARIES_OUTPUT = 'data/{env}/news-articles-nlp/articles/{source}/summaries/{filename}.txt'

    CNN_MONEY_RSS_HTML_OUTPUT = 'data/{env}/news-articles-nlp/static/cnn-money-rss-page.html'
    CNN_RSS_HTML_OUTPUT = 'data/{env}/news-articles-nlp/static/cnn-rss-page.html'

    def format(self, **kwargs):
        return self.value.format(
            env=env() or 'no-env',
            pipeline='news-articles-nlp',
            **kwargs
        )

    def __str__(self):
        return self.format()


class Status(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'


path1 = 'data/dev/news-articles-nlp/articles/cnn/html/1.html'
path2 = 'data/dev/news-articles-nlp/articles/fox/extracted/1.txt'
path3 = 'data/dev/news-articles-nlp/articles/huffington-post/processed/1.pickle'
path4 = 'data/dev/news-articles-nlp/articles/huffington-post/wordcloud/1.csv'
path5 = 'data/dev/news-articles-nlp/articles/huffington-post/wordcloud/1.png'
template = 'data/{env}/{pipeline}/articles/{source}/{worker}/{filename}.{extension}'

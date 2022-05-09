from enum import Enum

from .env import env


def data_directory_name():
    return 'data/' + env('')


class ReportTypes(Enum):
    SCRAPE_ARTICLE = 'scrape_articles'
    EXTRACT_TEXT = 'extract_texts'
    PROCESS_TEXT = 'process_texts'
    CREATE_WORDCLOUD = 'create_wordclouds'
    CREATE_SENTIMENT_ANALYSIS = 'create_sentiment_analysis'
    CREATE_EMOTIONAL_ANALYSIS = 'create_emotional_analysis'
    CREATE_N_GRAM_ANALYSIS = 'create_n_gram_analysis'
    CREATE_SUMMARY = 'create_summary'


class Paths(Enum):
    LOGGING = '{d}/logs.log'
    SCRAPE_HTMLS_OUTPUT = '{d}/cnn_articles_html/{filename}.html'
    EXTRACT_TEXTS_OUTPUT = '{d}/cnn_articles_extracted_texts/{filename}.txt'
    PROCESS_TEXTS_OUTPUT = '{d}/cnn_articles_processed_texts/{filename}.pickle'
    CNN_ARTICLE_INDEX = '{d}/index.json'
    CREATE_WORDCLOUD_OUTPUT = '{d}/cnn_articles_create_wordcloud_output/{filename}.csv'
    CREATE_WORDCLOUD_IMG_OUTPUT = ''
    CREATE_SENTIMENT_ANALYSIS_OUTPUT = ''
    CREATE_EMOTIONAL_ANALYSIS_OUTPUT = ''
    CREATE_N_GRAM_ANALYSIS_OUTPUT = ''
    CREATE_SUMMARY_OUTPUT = ''

    def format(self, **kwargs):
        kwargs['d'] = data_directory_name()
        return self.value.format(**kwargs)

    def __str__(self):
        return self.format()


class Status(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'

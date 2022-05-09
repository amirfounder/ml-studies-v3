from enum import Enum

from .env import is_env_prod, is_env_dev, env


def data_directory_name():
    return 'data/' + env('')


class ReportTypes(Enum):
    SCRAPE_ARTICLE = 'scrape_articles'
    EXTRACT_TEXT = 'extract_texts'
    PROCESS_TEXT = 'process_texts'
    CREATE_WORDCLOUD = 'create_wordclouds'


class Paths(Enum):
    LOGGING = '{d}/logs.log'
    SCRAPE_HTMLS_OUTPUT = '{d}/cnn_articles_html/{filename}.html'
    EXTRACT_TEXTS_OUTPUT = '{d}/cnn_articles_extracted_texts/{filename}.txt'
    PROCESS_TEXTS_OUTPUT = '{d}/cnn_articles_processed_texts/{filename}.pickle'
    CNN_ARTICLE_INDEX = '{d}/index.json'

    def format(self, **kwargs):
        kwargs['d'] = data_directory_name()
        return self.value.format(**kwargs)

    def __str__(self):
        return self.format()


class Status(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'

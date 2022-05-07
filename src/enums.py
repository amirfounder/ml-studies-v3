from enum import Enum

from .env import is_env_prod, is_env_dev


def data_directory_name():
    return 'data/' + ('dev' if is_env_dev() else 'prod' if is_env_prod() else '')


class ReportTypes(Enum):
    SCRAPE_ARTICLES = 'scrape_htmls'
    EXTRACT_TEXTS = 'extract_texts'
    PROCESS_TEXTS = 'process_texts'


class Paths(Enum):
    LOGGING = '{d}/logs.log'
    SCRAPE_HTMLS_OUTPUT = '{d}/cnn_articles_html/{filename}.html'
    EXTRACT_TEXTS_OUTPUT = '{d}/cnn_articles_extracted_texts/{filename}.json'
    PROCESS_TEXTS_OUTPUT = '{d}/cnn_articles_processed_texts/{filename}.pickle'

    def format(self, **kwargs):
        kwargs['d'] = data_directory_name()
        return self.value.format(**kwargs)

    def __str__(self):
        return self.format()


class Status(Enum):
    FAILURE = 'FAILURE'
    SUCCESS = 'SUCCESS'

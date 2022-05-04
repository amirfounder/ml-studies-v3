import json
import threading
from datetime import datetime
from os import listdir
from os.path import exists

from models import IndexEntryModel

LOGS_PATH = 'data/logs.log'
CNN_ARTICLE_HTML_INDEX_V1_PATH = 'data/index_v1.json'
CNN_ARTICLE_HTML_INDEX_V2_PATH = 'data/index_v2.json'


def read(path, mode='r', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        return f.read()


def write(path, contents, mode='w', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        f.write(contents)


def try_load_json(o):
    try:
        return json.loads(o)
    except Exception:
        return {}


def next_file_path(data_dir, file_suffix):
    return 'data/' + data_dir + '/' + str(len(listdir('data/' + data_dir)) + 1) + file_suffix


def log(message, level='INFO'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write(LOGS_PATH, message, mode='a')


def worker(func):
    def wrapper(*args, **kwargs):
        log(f'Started worker: {func.__name__}')

        try:
            start = datetime.now()
            result = func(*args, **kwargs)
            end = datetime.now()
            log(f'Finished worker: {func.__name__}. Time elapsed = {str(end - start)}')
            return result

        except Exception as e:
            log(f'Unhandled worker exception: {str(e)} | {func.__name__}', level='error')

    return wrapper


def read_cnn_article_index() -> dict[str, IndexEntryModel]:
    index = {}
    if exists(CNN_ARTICLE_HTML_INDEX_V2_PATH):
        for k, v in try_load_json(read(CNN_ARTICLE_HTML_INDEX_V2_PATH)).items():
            index[k] = IndexEntryModel.from_dict(v)
    return index


def save_cnn_article_index(index: dict[str, IndexEntryModel]) -> None:
    write(CNN_ARTICLE_HTML_INDEX_V2_PATH, json.dumps({k: dict(v) for k, v in index.items()}))


class CnnArticleIndex:
    def __init__(self):
        self.index = {}

    def __enter__(self):
        log('Reading index from file ...')
        self.index = read_cnn_article_index()
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            return

        log('Saving index to file ...')
        save_cnn_article_index(self.index)


def active_thread_count():
    return [t for t in threading.enumerate() if t.name.startswith('ml-studies-thread')]


if __name__ == '__main__':
    pass

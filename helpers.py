import inspect
import json
import threading
from time import sleep
from typing import Callable

import feedparser
from datetime import datetime
from os import listdir
from os.path import exists

from models import IndexEntryModel, Report

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


class Worker:
    def __init__(self, name: str = None):
        self.name = name

    def __call__(self, func):
        if not self.name:
            self.name = func.__name__

        def _worker(*args, **kwargs):
            log(f'Started worker: {self.name}')

            try:
                start = datetime.now()
                result = func(*args, **kwargs)
                end = datetime.now()
                log(f'Finished worker: {self.name}. Time elapsed = {str(end - start)}')
                return result

            except Exception as e:
                log(f'Exception occurred running worker: {str(e)} | Worker: {self.name}', level='error')

        return _worker

    def task(self, func):
        def wrapper(entry: IndexEntryModel):
            report = Report()
            report.open()

            entry.reports[self.name] = report
            signature = inspect.signature(func)

            try:
                result = func(entry, report=report) if 'report' in signature.parameters else func(entry)
                report.log_as_success()
                return result

            except Exception as e:
                log(f'Exception occurred running task: {str(e)} | Worker: {self.name}', level='error')
                report.log_as_failed(str(e))

        return wrapper


worker = Worker


class CnnArticleIndex:
    def __init__(self):
        self.index: dict[str, IndexEntryModel] = {}

    def __enter__(self):
        log('Reading index from file ...')
        self._read_cnn_article_index()
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            return

        log('Saving index to file ...')
        self._save_cnn_article_index()

    def _read_cnn_article_index(self) -> None:
        if exists(CNN_ARTICLE_HTML_INDEX_V2_PATH):
            for k, v in try_load_json(read(CNN_ARTICLE_HTML_INDEX_V2_PATH)).items():
                self.index[k] = IndexEntryModel.from_dict(v)

    def _save_cnn_article_index(self) -> None:
        write(CNN_ARTICLE_HTML_INDEX_V2_PATH, json.dumps({k: dict(v) for k, v in self.index.items()}))


def active_thread_count():
    return len([t for t in threading.enumerate() if t.name.startswith('ml-studies-thread')])


def run_concurrently(t_args_list: list[tuple[Callable, tuple, dict]], max_concurrent_threads=100):
    ts = []

    for i, t_args in enumerate(t_args_list):
        t_kwargs = {
            'target': t_args[0],
            'daemon': True,
            'name': 'ml-studies-thread-' + str(i)
        }
        if t_args[1]:
            t_kwargs['args'] = t_args[1] if isinstance(t_args[1], tuple) else (t_args[1],)
        if t_args[2]:
            t_kwargs['kwargs'] = t_args[2]

        t = threading.Thread(**t_kwargs)
        t.start()

        while active_thread_count() > max_concurrent_threads:
            sleep(1)

    for t in ts:
        t.join()


def get_entries_from_rss_url(idx, i, topic, url):
    idx[i] = (topic, feedparser.parse(url).entries)


if __name__ == '__main__':
    pass

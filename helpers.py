import inspect
import json
import threading
from time import sleep
from typing import Callable, Optional
from shutil import copyfile

import feedparser
from datetime import datetime
from os.path import exists

from models import IndexEntry, Report

LOGS_PATH = 'data/logs.log'
CNN_ARTICLE_INDEX_PATH = 'data/index_v3.json'


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


def log(message, level='INFO'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write(LOGS_PATH, message, mode='a')


def worker(name: str = None):
    def inner(func):
        return Worker(func, name)
    return inner


class Worker:
    def __init__(self, func, name: Optional[str]):
        self.name = name

        def _worker(*args, **kwargs):
            log(f'Started worker: {self.name}')

            start = datetime.now()
            result = None

            try:
                result = func(*args, **kwargs)

            except Exception as e:
                log(f'Exception occurred running worker: {str(e)} | Worker: {self.name}', level='error')

            finally:
                end = datetime.now()
                log(f'Finished worker: {self.name}. Time elapsed = {str(end - start)}')
                return result

        self._worker = _worker

    def __call__(self, *args, **kwargs):
        return self._worker(*args, **kwargs)

    def task(self, func):
        def inner(*args, **kwargs):
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            report = Report()
            report.open()

            try:
                result = func(*args, **kwargs)
                report.log_as_success()
                return result

            except Exception as e:
                log(f'Exception occurred running task: {str(e)} | Worker: {self.name} | Task : {func.__name__}'
                    , level='error')
                report.log_as_failed(str(e))

            finally:
                if entry and isinstance(entry, IndexEntry):
                    entry.reports[self.name] = report

        return inner


class CnnArticleIndex:
    def __init__(self):
        self.index: dict[str, IndexEntry | dict] = {}

    def __enter__(self):
        log('Reading index from file ...')
        if exists(CNN_ARTICLE_INDEX_PATH):
            for k, v in try_load_json(read(CNN_ARTICLE_INDEX_PATH)).items():
                self.index[k] = IndexEntry.load(v)
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            log(f'Exception occurred closing the CNNArticleIndex: {str(exc_type)} {str(exc_val)} - {str(exc_tb)}',
                level='error')
            return

        log('Saving index to file ...')
        for k, v in self.index.items():
            self.index[k] = dict(v)

        write(CNN_ARTICLE_INDEX_PATH, json.dumps(self.index))
        return ['lol']


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

        while len([t for t in threading.enumerate() if t.name.startswith('ml-studies-thread')]) >\
                max_concurrent_threads:
            sleep(1)

    for t in ts:
        t.join()


def get_entries_from_rss_url(idx, i, topic, url):
    idx[i] = (topic, feedparser.parse(url).entries)


if __name__ == '__main__':
    pass


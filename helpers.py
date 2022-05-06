import json
import threading
from time import sleep
from typing import Callable, Optional

import feedparser
from datetime import datetime
from os.path import exists

from models import IndexEntry, Report
from enums import Worker as Worker

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


def worker(name: str | Worker = None):
    def inner(func):
        return _Worker(func, name.value if isinstance(name, Worker) else name)
    return inner


class _Worker:
    def __init__(self, func, name: Optional[str]):
        self.name = name or func.__name__

        def _worker(*args, **kwargs):
            log(f'Started worker: {self.name}')

            result = None
            msg = []
            lvl = None
            start = datetime.now()

            try:
                result = func(*args, **kwargs)
                msg.append('Finished worker')
                lvl = 'success'

            except Exception as e:
                msg.append(f'Exception occurred {type(e).__name__} {str(e)}.')
                lvl = 'error'

            finally:
                end = datetime.now()
                msg.extend([f'Worker: {self.name}', f'Time Elapsed: {str(end - start)}'])
                log('. '.join(msg), level=lvl)
                return result

        self._worker = _worker

    def __call__(self, *args, **kwargs):
        return self._worker(*args, **kwargs)

    def task(self, func):
        def inner(*args, **kwargs):
            # log(f'Started Task: {func.__name__}')
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            report = Report()
            report.open()

            result = None
            msg = []
            lvl = None
            start = datetime.now()

            try:
                result = func(*args, **kwargs)
                report.success()
                msg.append('Finished Task')
                lvl = 'success'

            except Exception as e:
                msg.append(f'Exception occurred: {type(e).__name__} {str(e)}')
                lvl = 'error'
                report.fail(str(e))

            finally:
                end = datetime.now()
                msg.extend([f'Worker: {self.name}', f'Task: {func.__name__}', f'Time Elapsed: {str(end - start)}'])
                log('. '.join(msg), level=lvl)

                if entry and isinstance(entry, IndexEntry) and self.name in [w.value for w in Worker]:
                    entry.reports[self.name] = report

                return result

        return inner


class CnnArticleIndexManager:
    def __init__(self):
        self.index: dict[str, IndexEntry | dict] = {}

    def __enter__(self):
        log('Reading index from file ...')
        if exists(CNN_ARTICLE_INDEX_PATH):
            for k, v in try_load_json(read(CNN_ARTICLE_INDEX_PATH)).items():
                v['_index'] = self.index
                self.index[k] = IndexEntry.load(v)
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            log(f'Exception occurred closing CnnArticleIndex: {exc_type.__name__} {str(exc_val)}',
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


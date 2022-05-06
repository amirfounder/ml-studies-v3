import json
import threading
from contextlib import contextmanager
from time import sleep
from typing import Callable, Optional

import feedparser
from datetime import datetime
from os.path import exists

from models import IndexEntry, Report
from enums import Worker as Worker


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


def timed(func):
    """
    Decorator that returns a tuple of the result (or None), exception (or None), elapsed
    :param func:
    :return:
    """
    def inner(*args, **kwargs):
        result = None
        exception = None
        start = datetime.now()

        try:
            result = func(*args, **kwargs)

        except Exception as e:
            exception = e

        end = datetime.now()
        return result, exception, end - start
    return inner


def log(message, level='INFO'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write('data/logs.log', message, mode='a')


def worker(name: str | Worker = None):
    def inner(func):
        return _Worker(func, name.value if isinstance(name, Worker) else name)
    return inner


class _Worker:
    def __init__(self, func, name: Optional[str]):
        self.worker_func = timed(func)
        self.worker_name = name or func.__name__

    def __call__(self, *args, **kwargs):
        return self.worker(*args, **kwargs)

    def worker(self, *args, **kwargs):
        log(f'Started worker: {self.worker_name}')
        result, exception, elapsed = self.worker_func(*args, **kwargs)

        if exception:
            message = f'Exception occurred: {type(exception).__name__} {str(exception)}'
            level = 'error'
        else:
            message = 'Finished worker'
            level = 'success'

        log('{}. Worker: {}. Time Elapsed: {}'.format(message, self.worker_name, str(elapsed)), level=level)
        return result

    def task(self, func):
        task_name = func.__name__
        func = timed(func)

        def inner(*args, **kwargs):
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            report = Report()
            report.open()
            result, exception, elapsed = func(*args, **kwargs)

            if exception:
                report.fail(str(exception))
                message = f'Exception occurred: {type(exception).__name__} {str(exception)}'
                level = 'error'
            else:
                report.success()
                message = 'Finished task'
                level = 'success'

            template = '{}. Worker: {}. Task: {}. Time Elapsed: {}'
            log(template.format(message, self.worker_name, task_name, str(elapsed)), level=level)

            if entry and isinstance(entry, IndexEntry) and self.worker_name in [w.value for w in Worker]:
                entry.reports[self.worker_name] = report

            return result

        inner.__name__ = task_name
        return inner


@contextmanager
def cnn_article_index():
    index = {}
    path = 'data/index_v3.json'
    exception_raised = False

    try:
        log('Reading index from file')
        if exists(path):
            for k, v in try_load_json(read(path)).items():
                v['_index'] = index
                index[k] = IndexEntry.load(v)
        # TODO: yield a paginator
        yield index

    except Exception as e:
        log(f'Exception occurred with cnn index context manager: {type(e).__name__}: {str(e)}')
        exception_raised = True

    finally:
        if not exception_raised:
            log('Saving index to file.')
            for k, v in index.items():
                index[k] = dict(v)

            write(path, json.dumps(index))


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


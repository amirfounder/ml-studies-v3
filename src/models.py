from __future__ import annotations

import json
from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum
from typing import Callable

from .enums import Status
from .commons import read, try_load_json, write, error, now


def iter_ignore(ignore_values):
    def outer(cls):
        return cls
    return outer


class Model(ABC):
    _iter_ignore_vals = {}

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @classmethod
    def iter_ignore(cls, *args):
        def outer(klass):
            cls._iter_ignore_vals[klass] = [*args]
            return klass
        return outer

    def __iter__(self):
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if self in self._iter_ignore_vals and k in self._iter_ignore_vals[self]:
                continue
            if isinstance(v, datetime):
                v = v.isoformat()
            if isinstance(v, Model):
                v = dict(v)
            if isinstance(v, Enum):
                v = v.value
            if isinstance(v, dict):
                for _k, _v in v.items():
                    v[_k] = dict(_v or {})
            if isinstance(v, list):
                for i, _v in enumerate(v):
                    v[i] = dict(v or {})
            yield k, v

    def set(self, **kwargs):
        for k, v in kwargs:
            setattr(self, k, v)
        return self


class IndexManager(Model):
    def __init__(self, domain: str = 'cnn', **kwargs):
        self.domain = domain
        self.kwargs = kwargs
        self.path = 'data/index_v3.json'

    def __enter__(self):
        self.index = Index(self.path, **self.kwargs)
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            error(f'Exception occurred : {exc_type.__name__}: {str(exc_val)}')
            return

        write(self.index.path, json.dumps(dict(self.index)))


@Model.iter_ignore('path', 'filter_callback')
class Index(Model):
    def __init__(self, path: str, filter_callback: Callable = None):
        self.path = path
        self._filter_callback = filter_callback
        self.entries = {}
        self.entries_count = 0
        self.rss_urls = {}

        index = try_load_json(read(path))

        for k, v in index['entries']:
            if self.filter_callback and not self.filter_callback(v):
                continue
            self.entries[k] = IndexEntry(**v)

        for k, v in index['rss_urls']:
            self.rss_urls[k] = RssUrl(**v)


    def __enter__(self):
        index = try_load_json(read('data/index_v3.json')).items()

        self.entries_count = index['entries_count']

        for k, v in index['entries']:
            if self.filter_callback and not self.filter_callback(v):
                continue
            self.entries[k] = IndexEntry(**v)

        for k, v in index['rss_urls']:
            self.rss_urls[k] = RssUrl(**v)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            error(f'Exception occurred : {exc_type.__name__}: {str(exc_val)}')
            return

        write('data/index_v3.json', json.dumps(dict(self)))


class Report(Model, ABC):
    def __init__(self, **kwargs):
        status = kwargs.get('status')
        self.status = status if isinstance(status, Status) else Status(status) if isinstance(status, str) else None
        self.start = kwargs.get('start')
        self.end = kwargs.get('end')
        self.elapsed = kwargs.get('elapsed')
        self.error = kwargs.get('error')
        self.has_been_attempted = kwargs.get('has_been_attempted', False)
        self.last_attempt_timestamp = kwargs.get('last_attempt_timestamp')
        self.additional_data = kwargs.get('additional_data', {})
        self.output_path = kwargs.get('output_path')

    @classmethod
    def open(cls):
        self = cls()
        self.start = now()
        self.has_been_attempted = True
        self.last_attempt_timestamp = now()
        return self

    def close(self, result, exception):
        self.end = now()
        self.elapsed = self.end - self.start
        if exception:
            self.record_failure(exception)
        else:
            self.record_success(result)
        return self

    def record_failure(self, error: str):
        self.end = now()
        self.elapsed = self.end - self.start
        self.status = Status.FAILURE
        self.error = error
        self.result = None
        return self

    def record_success(self, result):
        self.end = now()
        self.elapsed = self.end - self.start
        self.status = Status.SUCCESS
        self.result = result
        self.error = None
        return self


class RssUrl(Model):
    def __init__(self, **kwargs):
        self.topic = kwargs.get('topic')
        self.url = kwargs.get('url')
        self.site = kwargs.get('domain')


class IndexEntry(Model):
    def __init__(self, **kwargs):
        self.output_filename = kwargs['output_filename']
        self.reports: dict[str, Report] = {k: (v if isinstance(v, Report) else Report(**v)) for k, v in kwargs['reports'].items()}
        self.url = kwargs['url']
        self.topic = kwargs['topic']
        self.output_filename = kwargs['output_filename']

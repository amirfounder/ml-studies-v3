from __future__ import annotations

import json
from abc import abstractmethod, ABC
from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Generator

from .enums import Status, ReportTypes, Paths
from .commons import read, try_load_json, write, error, now


class Model(ABC):

    @abstractmethod
    def __init__(self, **kwargs):
        ...

    def __iter__(self):
        items = [(x, getattr(self, x)) for x in dir(self) if not x.startswith('_') and not callable(getattr(self, x))]
        for k, v in items:
            if isinstance(v, datetime):
                v = v.isoformat()
            if isinstance(v, timedelta):
                v = str(v)
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


@contextmanager
def get_index(domain: str = 'cnn') -> Generator[Index, None, None]:
    path = str(Paths.CNN_ARTICLE_INDEX)
    index = Index(path)

    try:
        yield index

    except Exception as e:
        error('Exception occurred. (Details in further logs)', e)

    finally:
        write(path, json.dumps(dict(index)))


class Index(Model):
    @property
    def entries_count(self):
        return len(self.get_entries())

    def __init__(self, path: str):
        self.entries = {}
        self.rss_urls = {}
        self._entries_have_been_loaded = False
        self._rss_urls_have_been_loaded = False
        self._index = try_load_json(read(path))

    def get_entries(self, filter_fn: Callable = None) -> dict:
        if not self._entries_have_been_loaded:
            for k, v in self._index.get('entries', {}).items():
                self.entries[k] = IndexEntry(**v)
            self._entries_have_been_loaded = True

        to_release = {}

        if filter_fn:
            for k, v in self.entries.items():
                if filter_fn(v):
                    to_release[k] = v
        else:
            to_release = self.entries

        return to_release

    def get_rss_urls(self) -> dict:
        if not self._rss_urls_have_been_loaded:
            for k, v in self._index.get('rss_entries', {}).items():
                self.rss_urls[k] = RssUrl(**v)
            self._rss_urls_have_been_loaded = True

        return self.rss_urls


class Report(Model):
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

    def close(self, result: Any, exception: Exception):
        self.end = now()
        self.elapsed = self.end - self.start
        if exception:
            self.record_failure(str(exception))
        else:
            self.record_success(result)
        return self

    def record_failure(self, _error: str):
        self.end = now()
        self.elapsed = self.end - self.start
        self.status = Status.FAILURE
        self.error = _error
        self.result = None
        return self

    def record_success(self, result: Any):
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
        self.filename = kwargs['filename']
        self.reports = {k: (v if isinstance(v, Report) else Report(**v)) for k, v in kwargs.get('reports', {}).items()}
        for t in ReportTypes:
            if t.value not in self.reports:
                self.reports[t.value] = Report()
        self.url = kwargs['url']
        self.topic = kwargs['topic']
        self.filename = kwargs['filename']

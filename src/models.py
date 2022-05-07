from __future__ import annotations

import json
from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum
from typing import Callable

from .enums import Status
from .commons import read, try_load_json, write, log, now


class Model(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    def __iter__(self):
        for k, v in self.__dict__.items():
            if k.startswith('_'):
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


class Index(Model):
    def __init__(self, filter_callback: Callable = None):
        self.filter_callback = filter_callback
        self.index: dict[str, IndexEntry | dict] = {}

    def __getitem__(self, item):
        return self.index[item]

    def __setitem__(self, key, value):
        self.index[key] = value

    def __enter__(self):
        for k, v in try_load_json(read('data/index_v3.json') or {}).items():
            if self.filter_callback and not self.filter_callback(v):
                continue
            self.index[k] = IndexEntry(**v)
        return self.index

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            log('Exception occurred', level='error')
            return

        for k, v in self.index.items():
            self.index[k] = dict(v)

        write('data/index_v3.json', json.dumps(self.index))


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


class IndexEntry(Model):
    def __init__(self, **kwargs):
        self.output_filename = kwargs['output_filename']
        self.reports: dict[str, Report] = {k: (v if isinstance(v, Report) else Report(**v)) for k, v in kwargs['reports'].items()}
        self.url = kwargs['url']
        self.topic = kwargs['topic']
        self.output_filename = kwargs['output_filename']

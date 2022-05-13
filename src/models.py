from __future__ import annotations

from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

from .enums import Status, ReportTypes
from .commons import read, try_load_json


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
                    try:
                        v[_k] = dict(_v or {})
                    except Exception:
                        v[_k] = _v
            if isinstance(v, list):
                for i, _v in enumerate(v):
                    try:
                        v[i] = dict(v or {})
                    except Exception:
                        v[i] = _v
            yield k, v

    def set(self, **kwargs):
        for k, v in kwargs:
            setattr(self, k, v)
        return self


class Index(Model):
    @property
    def _models_count(self):
        return len(self._get_models())

    def __init__(self, path, key, model_cls):
        self._index = try_load_json(read(path))
        self._key = key
        self._model_cls = model_cls
        self._models = {}
        self._models_have_been_loaded = False

    def __contains__(self, item):
        return item in self._get_models()

    def __setitem__(self, key, value):
        self._models[key] = value

    def __getitem__(self, item):
        return self._models[item]

    def _get_models(self, filter_callback: Callable[[Any], bool] = None):
        if not self._models_have_been_loaded:
            for k, v in self._index.get(self._key, {}).items():
                self._models[k] = self._model_cls(**v)
            self._models_have_been_loaded = True

        models_to_return = {}

        if filter_callback:
            for k, v in self._models.items():
                if filter_callback(v):
                    models_to_return[k] = v

        else:
            models_to_return = self._models

        return models_to_return


class SentenceIndex(Index):
    def __init__(self, path):
        super().__init__(path, 'sentences', SentenceIndexEntry)
        self.sentences = self._models

    @property
    def sentences_count(self):
        return self._models_count

    def get_sentences(self):
        return self._get_models()


class ArticleIndex(Index):
    def __init__(self, path: str):
        super().__init__(path, 'articles', ArticleIndexEntry)
        self.articles = self._models

    @property
    def articles_count(self):
        return self._models_count

    def get_articles(self, filter_callback: Callable[[Any], bool] = None) -> dict:
        return self._get_models(filter_callback)


class SentenceIndexEntry(Model):
    def __init__(self, **kwargs):
        self.occurred_in_articles = kwargs.get('occurred_in_articles', [])
        self.occurrences = kwargs.get('occurrences', 0)
        self.non_lemmatized_sequence = kwargs.get('non_lemmatized_sequence')


class ArticleIndexEntry(Model):
    def __init__(self, **kwargs):
        self.filename = kwargs['filename']
        self.url = kwargs['url']
        self.topic = kwargs['topic']
        self.filename = kwargs['filename']
        self.source = kwargs.get('source')
        self.reports = {t.value: None for t in ReportTypes}

        for k, v in kwargs.get('reports', {}).items():
            if k in self.reports:
                if not isinstance(v, Report):
                    v = Report(**v)
                self.reports[k] = v


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

    @classmethod
    def open(cls, **kwargs):
        self = cls(**kwargs)
        self.has_been_attempted = True
        return self

    def close(self, result: Any, exception: Exception, **kwargs):
        if exception:
            self._record_failure(str(exception))
        else:
            self._record_success(result)
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self

    def _record_failure(self, _error: str):
        self.status = Status.FAILURE
        self.error = _error
        self.result = None
        return self

    def _record_success(self, result: Any):
        self.status = Status.SUCCESS
        self.result = result
        self.error = None
        return self

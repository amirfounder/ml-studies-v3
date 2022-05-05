from abc import abstractmethod, ABC
from datetime import datetime, timezone
from enum import Enum


class Model(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @classmethod
    def load(cls, obj: dict):
        return cls(**obj)

    def __iter__(self):
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, datetime):
                v = v.isoformat()
            if isinstance(v, Model):
                v = dict(v)
            if isinstance(v, dict):
                for _k, _v in v.items():
                    v[_k] = dict(_v or {})
            if isinstance(v, list):
                for i, _v in enumerate(v):
                    v[i] = dict(v or {})
            yield k, v


class Report(Model, ABC):
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'

    def __init__(self, **kwargs):
        self.status = kwargs.get('status')
        self.error = kwargs.get('error')
        self.has_been_attempted = kwargs.get('has_been_attempted', False)
        self.last_attempt_timestamp = kwargs.get('last_attempt_timestamp')
        self.additional_data = kwargs.get('additional_data', {})

    def open(self):
        self.has_been_attempted = True
        self.last_attempt_timestamp = datetime.now(timezone.utc)

    def fail(self, error: str):
        self.status = Report.FAILED
        self.error = error

    def success(self):
        self.status = Report.SUCCESS
        self.error = None

    def reset(self):
        self.status = None
        self.error = None
        self.last_attempt_timestamp = None
        self.has_been_attempted = False
        self.additional_data = {}


class ReportType(Enum):
    scrape_urls = 'scrape_urls_v1'
    extract_text = 'extract_text_v2'


class IndexEntry(Model):
    def __init__(self, **kwargs):
        self._index = kwargs['_index']
        self.reports: dict[str, Report] = kwargs['reports']
        self.url = kwargs['url']
        self.topic = kwargs['topic']

        self.scraped_html_path = kwargs['scraped_html_path']
        self.extracted_text_path = kwargs['extracted_text_path']
        self.preprocessed_text_path = kwargs['preprocessed_text_path'] if 'preprocessed_text_path' in kwargs else ''

    @classmethod
    def load(cls, obj: dict):
        for k, v in obj['reports'].items():
            obj['reports'][k] = Report.load(v)
        return super().load(obj)

    def __getitem__(self, item):
        # use dispatch from multipledispatch once this "switch" statements gets big enough
        if isinstance(item, ReportType):
            return self.reports[item.value]


class ArticleText(Model):
    def __init__(self, **kwargs):
        self.paragraphs = kwargs.get('paragraphs')
        self.paragraphs_text = kwargs.get('paragraphs_text')

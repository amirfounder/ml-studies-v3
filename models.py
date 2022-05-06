from abc import abstractmethod, ABC
from datetime import datetime, timezone
from enums import Worker


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


class IndexEntry(Model):
    def __init__(self, **kwargs):
        self._index = kwargs['_index']
        self.reports: dict[str, Report] = kwargs['reports']
        self.url = kwargs['url']
        self.topic = kwargs['topic']
        self.filename = kwargs['filename']

        self.scraped_html_path = f'data/cnn_articles_html/{self.filename}.html'
        self.extracted_text_path = f'data/cnn_articles_extracted_texts/{self.filename}.txt'
        self.preprocessed_text_path = f'data/cnn_articles_preprocessed_texts/{self.filename}.txt'
        self.processed_text_path = f'data/cnn_articles_processed_texts/{self.filename}.json'

    @classmethod
    def load(cls, obj: dict):
        for k, v in obj['reports'].items():
            obj['reports'][k] = Report.load(v)
        return super().load(obj)

    def __getitem__(self, item):
        if isinstance(item, Worker):
            return self.reports[item.value]


class ArticleText(Model):
    def __init__(self, **kwargs):
        self.paragraphs = kwargs.get('paragraphs')
        self.paragraphs_text = kwargs.get('paragraphs_text')

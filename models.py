from abc import abstractmethod, ABC
from datetime import datetime, timezone


class Model(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @classmethod
    def from_dict(cls, obj: dict):
        return cls(**obj)

    def __iter__(self):
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                v = v.isoformat()
            if isinstance(v, Model):
                v = dict(v)
            yield k, v


class IndexEntry(Model):
    def __init__(self, **kwargs):
        self.reports: dict[str, Report] = kwargs.get('reports')
        self.url = kwargs.get('url')

        self.scraped_html_path = kwargs.get('scraped_html_path')
        self.extracted_text_v1_path = kwargs.get('extracted_text_v1_path')
        self.extracted_text_v2_path = kwargs.get('extracted_text_v2_path')

    @classmethod
    def from_dict(cls, obj: dict):
        for k, v in obj['reports'].items():
            obj['reports'][k] = Report.from_dict(v)
        return super().from_dict(obj)


class Report(Model, ABC):
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    
    def __init__(self):
        self.status = None
        self.error = None
        self.has_been_attempted = False
        self.last_attempt_timestamp = None
        self.additional_data = {}

    def open(self):
        self.has_been_attempted = True
        self.last_attempt_timestamp = datetime.now(timezone.utc)
    
    def log_as_failed(self, error: str):
        self.status = Report.FAILED
        self.error = error

    def log_as_success(self):
        self.status = Report.SUCCESS
        self.error = None


class ArticleText(Model):
    def __init__(self, **kwargs):
        self.paragraphs = kwargs.get('paragraphs')
        self.paragraphs_text = kwargs.get('paragraphs_text')

from abc import abstractmethod, ABC
from datetime import datetime, timezone


class Dictable(ABC):
    def __iter__(self):
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                v = v.isoformat()
            yield k, v


class Model(Dictable, ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @classmethod
    def from_dict(cls, obj: dict):
        return cls(**obj)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        return self


class IndexEntryModel(Model):
    def __init__(self, **kwargs):
        self.reports: dict[str, Report] = {}
        self.url = kwargs.get('url')

        self.has_scraping_been_attempted = kwargs.get('has_scraping_been_attempted', False)
        self.has_text_extraction_v1_been_attempted = kwargs.get('has_text_extraction_v1_been_attempted', False)
        self.has_text_extraction_v2_been_attempted = kwargs.get('has_text_extraction_v2_been_attempted', False)

        self.scraped_html_path = kwargs.get('scraped_html_path')
        self.scrape_was_successful = kwargs.get('scrape_was_successful')
        self.scrape_error = kwargs.get('scrape_error')

        self.extracted_text_v1_path = kwargs.get('extracted_text_v1_path')
        self.text_extraction_v1_was_successful = kwargs.get('text_extraction_v1_was_successful')
        self.text_extraction_v1_error = kwargs.get('text_extraction_v1_error')
        self.text_extraction_v1_strategy_used = kwargs.get('text_extraction_v1_strategy_used')

        self.extracted_text_v2_path = kwargs.get('extracted_text_v2_path')
        self.text_extraction_v2_was_successful = kwargs.get('text_extraction_v2_was_successful')
        self.text_extraction_v2_error = kwargs.get('text_extraction_v2_error')

        # TODO : datetime.strftime the string to datetime if it's a string
        self.datetime_indexed = d if isinstance(d := kwargs.get('datetime_indexed'), datetime)\
            else None
        self.datetime_scraped = d if isinstance(d := kwargs.get('datetime_scraped'), datetime)\
            else None
        self.datetime_v1_text_extracted = d if isinstance(d := kwargs.get('datetime_v1_text_extracted'), datetime)\
            else None


class Report(Dictable, ABC):
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

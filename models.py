from abc import abstractmethod, ABC
from datetime import datetime


class Model(ABC):
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

    def __iter__(self):
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                v = v.isoformat()
            yield k, v


class IndexEntryModel(Model):
    def __init__(self, **kwargs):
        self.url = kwargs.get('url')

        self.has_been_scraped = kwargs.get('has_been_scraped', False)
        self.has_text_been_extracted = kwargs.get('has_text_been_extracted', False)

        self.scrape_was_successful = kwargs.get('scrape_was_successful')
        self.scraped_html_path = kwargs.get('scraped_html_path')
        self.scrape_error = kwargs.get('scrape_error')

        self.text_extraction_was_successful = kwargs.get('text_extraction_was_successful')
        self.text_extraction_path = kwargs.get('text_extraction_path')
        self.text_extraction_error = kwargs.get('text_extraction_error')

        self.datetime_indexed = t if isinstance(t := kwargs.get('datetime_indexed'), datetime) else None
        self.datetime_scraped = kwargs.get('datetime_scraped')
        self.datetime_text_extracted = kwargs.get('datetime_text_extracted')


class ExtractedArticleText(Model):
    def __init__(self, **kwargs):
        self.paragraphs = kwargs.get('paragraphs')
        self.related_articles = kwargs.get('related_articles')

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

        self.has_scraping_been_attempted = kwargs.get('has_scraping_been_attempted', False)
        self.has_text_extraction_been_attempted = kwargs.get('has_text_extraction_been_attempted', False)

        self.scraped_html_path = kwargs.get('scraped_html_path')
        self.scrape_was_successful = kwargs.get('scrape_was_successful')
        self.scrape_error = kwargs.get('scrape_error')

        self.extracted_text_path = kwargs.get('extracted_text_path')
        self.text_extraction_was_successful = kwargs.get('text_extraction_was_successful')
        self.text_extraction_error = kwargs.get('text_extraction_error')
        self.text_extraction_strategy_used = kwargs.get('text_extraction_strategy_used')

        # TODO : datetime.strftime the string to datetime if it's a string
        self.datetime_indexed = d if isinstance(d := kwargs.get('datetime_indexed'), datetime) else None
        self.datetime_scraped = d if isinstance(d := kwargs.get('datetime_scraped'), datetime) else None
        self.datetime_text_extracted = d if isinstance(d := kwargs.get('datetime_text_extracted'), datetime) else None


class ExtractedArticleText(Model):
    def __init__(self, **kwargs):
        self.paragraphs = kwargs.get('paragraphs')
        self.related_articles = kwargs.get('related_articles')

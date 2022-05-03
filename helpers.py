import json
from abc import abstractmethod, ABC
from datetime import datetime, timezone, timedelta
from os import listdir
from os.path import exists

LOGS_PATH = 'data/logs.log'
CNN_ARTICLE_HTML_INDEX_V1_PATH = 'data/index_v1.json'
CNN_ARTICLE_HTML_INDEX_V2_PATH = 'data/index_v2.json'


def read(path, mode='r', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        return f.read()


def write(path, contents, mode='w', encoding='utf-8'):
    with open(path, mode, encoding=encoding) as f:
        f.write(contents)


def try_load_json(o):
    try:
        return json.loads(o)
    except Exception:
        return {}


def next_file_path(data_dir, file_suffix):
    return 'data/' + data_dir + str(len(listdir('data/' + data_dir)) + 1) + file_suffix


def log(message, level='INFO'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write(LOGS_PATH, message, mode='a')


def worker(func):
    def wrapper(*args, **kwargs):
        log(f'Started worker: {func.__name__}')

        try:
            result = func(*args, **kwargs)
            log(f'Finished worker: {func.__name__}. Result: {json.dumps(result)}')
            return result

        except Exception as e:
            log(f'Unhandled worker exception: {str(e)}', level='error')

    return wrapper


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


def read_cnn_article_index() -> dict[str, IndexEntryModel]:
    index = {}
    if exists(CNN_ARTICLE_HTML_INDEX_V2_PATH):
        for k, v in try_load_json(read(CNN_ARTICLE_HTML_INDEX_V2_PATH)).items():
            index[k] = v
    return {}


def save_cnn_article_index(index: dict[str, IndexEntryModel]) -> None:
    write(CNN_ARTICLE_HTML_INDEX_V2_PATH, json.dumps({k: dict(v) for k, v in index.items()}))


def migrate_cnn_index_v1_to_v2():
    v1_index = json.loads(read(CNN_ARTICLE_HTML_INDEX_V1_PATH)) if exists(CNN_ARTICLE_HTML_INDEX_V1_PATH) else {}
    v2_index = json.loads(read(CNN_ARTICLE_HTML_INDEX_V2_PATH)) if exists(CNN_ARTICLE_HTML_INDEX_V2_PATH) else {}

    for v1_entry in v1_index.values():
        v2_entry = IndexEntryModel(
            url=v1_entry.get('url'),
            has_been_scraped=v1_entry.get('has_been_scraped'),
            scrape_was_successful=True if v1_entry.get('html_path') else False,
            scraped_html_path=v1_entry.get('html_path'),
            datetime_indexed=datetime(
                year=v1_entry.get('year'),
                month=v1_entry.get('month'),
                day=v1_entry.get('day'),
                hour=v1_entry.get('hour'),
                minute=v1_entry.get('minute'),
                second=v1_entry.get('second'),
                tzinfo=timezone.utc
            ) + timedelta(hours=5)
        )
        v2_index[v2_entry.url] = dict(v2_entry)
    write(CNN_ARTICLE_HTML_INDEX_V2_PATH, json.dumps(v2_index))


if __name__ == '__main__':
    pass

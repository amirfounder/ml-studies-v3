import json
from contextlib import contextmanager
from threading import Lock

from src.commons import error, write
from src.enums import Paths
from src.models import ArticleIndex, SentenceIndex


index_map = {
    'sentences': (SentenceIndex, Paths.SENTENCES_INDEX),
    'articles': (ArticleIndex, Paths.ARTICLES_INDEX)
}


@contextmanager
def get_index(name: str):
    with Lock():
        index_cls, path = index_map.get(name)
        index = index_cls(path.format())

        try:
            yield index

        except Exception as e:
            error('Exception occurred. (There are likely details in further logs ...)', e)

        finally:
            write(path.format(), json.dumps(dict(index)))

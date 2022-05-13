import json
from contextlib import contextmanager
from typing import Generator

from src.commons import error, write
from src.enums import Paths
from src.models import ArticleIndex, SentenceIndex


@contextmanager
def get_article_index() -> Generator[ArticleIndex, None, None]:
    path = str(Paths.ARTICLES_INDEX)
    index = ArticleIndex(path)

    try:
        yield index

    except Exception as e:
        error('Exception occurred. (There are likely details in further logs ...)', e)

    finally:
        write(path, json.dumps(dict(index)))


@contextmanager
def get_sentence_index() -> Generator[SentenceIndex, None, None]:
    # TODO - Because we will be loading all sentences chrome
    path = str(Paths.SENTENCES_INDEX)
    index = SentenceIndex(path)

    try:
        yield index

    except Exception as e:
        error('Exception occurred. (There are likely details in further logs ...)', e)

    finally:
        write(path, json.dumps(dict(index)))
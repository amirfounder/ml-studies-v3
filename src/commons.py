import json
from datetime import datetime, timezone
from os.path import exists
from typing import Optional

import spacy

nlp = spacy.load('en_core_web_sm')



def read(path, mode='r', encoding='utf-8'):
    if exists(path):
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


def now():
    return datetime.now(timezone.utc)


def log(message, level='info'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write('data/logs.log', message, mode='a')


def info(message):
    log(message, 'info')


def error(message: str, exception: Optional[Exception] = None):
    if exception:
        message = message.strip().removesuffix('.') + f'. Exception: {type(exception).__name__} - {str(exception)}'
    log(message, 'error')


def success(message):
    log(message, 'success')

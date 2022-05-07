import json
from datetime import datetime, timezone
from os.path import exists
from os import environ
from typing import Optional

import spacy

from .env import is_env_prod, is_env_dev
from .enums import Paths

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


def _log(message, level: str = 'info', env: str = None):
    env = (env or ('prod' if is_env_prod() else 'dev' if is_env_dev() else '--')).upper().ljust(10)
    level = level.upper().ljust(10)
    timestamp = now().isoformat().ljust(40)

    message = timestamp + env + level + message
    print(message)
    message += '\n'
    write(str(Paths.LOGGING), message, mode='a')


def info(message):
    _log(message, 'info')


def error(message: str, exception: Optional[Exception] = None):
    if exception:
        message = message.strip().removesuffix('.') + f'. Exception: {type(exception).__name__} - {str(exception)}'
    _log(message, 'error')


def success(message):
    _log(message, 'success')


def set_env_to_prod():
    info('Setting working environment to prod')
    environ['ML_STUDIES_ENV'] = 'prod'


def set_env_to_dev():
    info('Setting working environment to dev')
    environ['ML_STUDIES_ENV'] = 'dev'

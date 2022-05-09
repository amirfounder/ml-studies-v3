import json
from datetime import datetime, timezone
from os.path import exists
from os import environ, makedirs
from threading import current_thread
from typing import Optional

import spacy

from .env import env
from .enums import Paths

nlp = spacy.load('en_core_web_sm')


def read(path, mode='r', encoding='utf-8'):
    if exists(path):

        kwargs = dict(
            file=path,
            mode=mode,
            encoding=encoding
        )

        if mode.endswith('b'):
            del kwargs['encoding']

        with open(**kwargs) as f:
            return f.read()


def write(path, contents, mode='w', encoding='utf-8'):
    if not exists(path):
        dir_path = '/'.join(path.split('/')[:-1])
        if not exists(dir_path):
            makedirs(dir_path)

    kwargs = dict(
        file=path,
        mode=mode,
        encoding=encoding
    )

    if mode.endswith('b'):
        del kwargs['encoding']

    with open(**kwargs) as f:
        f.write(contents)


def try_load_json(o):
    try:
        return json.loads(o)
    except Exception:
        return {}


def now():
    return datetime.now(timezone.utc)


def _log(message, level: str = 'info'):
    _env = 'ENV: ' + env().upper().ljust(10)
    level = level.upper().ljust(10)
    timestamp = now().isoformat().ljust(40)
    thread_name = 'THREAD: ' + current_thread().name.ljust(30)

    message = timestamp + _env + thread_name + level + message
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

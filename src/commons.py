import json
import os
from datetime import datetime, timezone
from os.path import exists
from os import environ, makedirs
from threading import current_thread
from typing import Optional, Callable, Any
import numpy as np

import spacy

from .env import working_env
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


def makedirs_from_path(path: str):
    if not exists(path):
        dir_path = '/'.join(path.split('/')[:-1])
        if not exists(dir_path):
            makedirs(dir_path)


def write(path, contents, mode='w', encoding='utf-8'):
    makedirs_from_path(path)

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
    env = 'ENV: ' + str(working_env()).upper().ljust(10)
    level = level.upper().ljust(10)
    timestamp = now().isoformat().ljust(36)
    pid = 'PID: ' + str(os.getpid()).ljust(15)
    thread_name = 'THREAD: ' + current_thread().name.ljust(25)

    message = timestamp + env + pid + thread_name + level + message
    print(message)
    message += '\n'
    write(Paths.LOGGING.format(), message, mode='a')


def info(message):
    _log(message, 'info')


def error(message: str, exception: Optional[Exception] = None):
    if exception:
        message = message.strip().removesuffix('.') + f'. Exception: {type(exception).__name__} - {str(exception)}'
    _log(message, 'error')


def success(message):
    _log(message, 'success')


# TODO - Need to use json to manage subprocesses with os.getpid() when pipelines need to run concurrently


def set_env_to_prod():
    environ['ML_STUDIES_ENV'] = 'prod'
    info('Working environment set to prod.')


def set_env_to_dev():
    environ['ML_STUDIES_ENV'] = 'dev'
    info('Working environment set to dev.')


def set_current_pipeline_var(value: str):
    key = 'CURRENT_PIPELINE'
    info(f'Setting {key} env variable to: {value}')
    environ[key] = value


def set_current_worker_var(value: str):
    key = 'CURRENT_WORKER'
    info(f'Setting {key} env variable to: {value}')
    environ[key] = value


def get_sentence_similarity_score(sent1: list[str], sent2: list[str]):
    def custom_equal_fn(token1, token2):
        _d = get_levenshtein_distance(token1, token2)
        _r = _d / pow(len(token1), 2)
        return (1 - _r) > .95

    d = get_levenshtein_distance(sent1, sent2, equals_fn=custom_equal_fn)
    return 1 - d / pow(len(sent1), 2)


def get_levenshtein_distance(token1: Any, token2: Any, equals_fn: Callable[[Any, Any], bool] = None):
    """
    Uses Levenshtein distance formula to find the distance between 2 strings
    :param token1: The first token
    :param token2: The second token
    :param equals_fn: Custom equal operator formula. If none, evaluates n1 == n2 where n1 = token1 and n2 = token2
    :return:
    """
    rows = len(token1) + 1
    cols = len(token2) + 1
    matrix = np.zeros((rows, cols))

    for i in range(rows):
        matrix[i][0] = i

    for i in range(cols):
        matrix[0][i] = i

    for i in range(1, rows):
        for j in range(1, cols):
            c1 = token1[i - 1]
            c2 = token2[j - 1]
            if equals_fn(c1, c2) if equals_fn else c1 == c2:
                matrix[i][j] = matrix[i - 1][j - 1]
            else:
                a = matrix[i][j - 1]
                b = matrix[i - 1][j]
                c = matrix[i - 1][j - 1]
                matrix[i][j] = min(a, b, c) + 1

    return int(matrix[rows - 1][cols - 1])

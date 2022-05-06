import json
from datetime import datetime
from os.path import exists


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


def timeit(func):
    def inner(*args, **kwargs):
        result = None
        exception = None
        start = datetime.now()

        try:
            result = func(*args, **kwargs)

        except Exception as e:
            exception = e

        end = datetime.now()
        return result, exception, end - start
    return inner


def log(message, level='info'):
    message = datetime.now().isoformat().ljust(30) + level.upper().ljust(10) + message
    print(message)
    message += '\n'
    write('data/logs.log', message, mode='a')

import time
from threading import Thread, enumerate as enum_threads, current_thread
from typing import Callable

from src.commons import now, info, error, success
from src.enums import ReportTypes
from src.models import IndexEntry, Report


def try_catch(func):
    def inner(*args, **kwargs):
        result, exception = None, None

        try:
            result = func(*args, **kwargs)

        except Exception as e:
            exception = e

        return result, exception
    return inner


def timeit(func):
    def inner(*args, **kwargs):
        start = now()
        result, exception = try_catch(func)(*args, **kwargs)
        end = now()
        return result, exception, end - start
    return inner


def ml_studies_fn(func, component, **decorator_kwargs):
    def inner(*args, **kwargs):
        if not decorator_kwargs.get('silent_start', True):
            info(f'Starting {component}: {func.__name__}')
        result, exception, elapsed = timeit(func)(*args, **kwargs)
        if exception:
            error(f'Error occurred at {component}: {func.__name__} (Elapsed: {str(elapsed)})', exception)
        else:
            success(f'Successfully completed {component}: {func.__name__} (Elapsed: {str(elapsed)})')
        return result, exception
    return inner


def pipeline(func):
    return ml_studies_fn(func, 'pipeline')


def worker(func):
    return ml_studies_fn(func, 'worker')


def task(silent_start=True):
    def outer(func):
        return ml_studies_fn(func, 'task', silent_start=silent_start)
    return outer


def subtask(func):
    return ml_studies_fn(func, 'subtask')


def log_report(name: ReportTypes):
    def outer(func):
        def inner(*args, **kwargs):
            report = Report.open()
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            r, e = func(*args, **kwargs)
            report.close(r, e)
            entry.reports[name.value] = report
            return r, e
        return inner
    return outer


_threads: dict[tuple[int, str], list[Thread]] = {}


def join_threads(func: Callable):
    k = (id(func), current_thread().name)

    if k in _threads:
        threads_to_join = _threads[k]
        for t in threads_to_join:
            t.join()
        del _threads[k]


def threaded(max_threads: int = None):
    """
    Note: This decorator must be the last of the decorators used on a fn as the threaded fn map uses the id of the
    inner functions of this decorator. Failure to do so will result in an almost guaranteed failed thread cleanup.
    """
    _id = 0

    def next_id():
        nonlocal _id
        return_val = str(_id)
        _id += 1
        return return_val

    def outer(func):
        nonlocal _id, max_threads

        def inner(*args, **kwargs):
            global _threads
            nonlocal _id, max_threads

            prefix = 'ml-studies-thread-'

            thread = Thread(
                target=func,
                args=args,
                kwargs=kwargs,
                daemon=True,
                name=prefix + next_id()
            )

            k = (id(inner), current_thread().name)

            if k not in _threads:
                _threads[k] = []

            if max_threads:
                while len([t for t in enum_threads() if t.name.startswith(prefix)]) > max_threads:
                    time.sleep(1)

            _threads[k].append(thread)
            thread.start()

        return inner
    return outer

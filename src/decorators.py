import time
from threading import Thread, enumerate as enum_threads

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


def threaded(max_threads: int = 50):
    _id = 1

    def next_id():
        nonlocal _id
        _id += 1
        return str(_id)

    threads = []

    def outer(func):
        nonlocal _id, threads, max_threads

        def inner(*args, **kwargs):
            nonlocal _id, threads, max_threads

            prefix = 'ml-studies-thread-'

            thread = Thread(
                target=func,
                args=args,
                kwargs=kwargs,
                daemon=True,
                name=prefix + next_id()
            )
            threads.append(thread)
            thread.start()

            while len([t for t in enum_threads() if t.name.startswith(prefix)]) < max_threads:
                time.sleep(1)

        return inner
    return outer

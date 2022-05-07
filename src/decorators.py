from src.commons import now, info, error, success
from src.enums import Reports
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


def ml_studies_fn(func, component):
    def inner(*args, **kwargs):
        info(f'Starting {component}: {func.__name__}')
        result, exception, elapsed = timeit(func)(*args, **kwargs)
        if exception:
            error(f'Error occurred at {component}: {func.__name__} ({str(elapsed)})', exception)
        else:
            success(f'Successfully completed pipeline: {func.__name__} ({str(elapsed)})')
        return result, exception
    return inner


def pipeline(func):
    return ml_studies_fn(func, 'pipeline')


def worker(func):
    return ml_studies_fn(func, 'worker')


def task(func):
    return ml_studies_fn(func, 'task')


def subtask(func):
    return ml_studies_fn(func, 'subtask')


def log_report(name: Reports):
    def outer(func):
        def inner(*args, **kwargs):
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            r, e = func(*args, **kwargs)
            entry.reports[name.value] = Report.open().close(r, e)
            return r, e
        return inner
    return outer

from src.commons import now, info, error, success


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


def ml_studies_decorator(func, name, component):
    def inner(*args, **kwargs):
        info(f'Starting {component}: {name}')
        result, exception, elapsed = timeit(func)(*args, **kwargs)
        if exception:
            error(f'Error occurred at {component}: {name} ({str(elapsed)})', exception)
        else:
            success(f'Successfully completed pipeline: {name} ({str(elapsed)})')
        return result, exception
    return inner


def pipeline(func):
    return ml_studies_decorator(func, func.__name__, 'pipeline')


def worker(func):
    return ml_studies_decorator(func, func.__name__, 'worker')


def task(func):
    return ml_studies_decorator(func, func.__name__, 'task')


def subtask(func):
    return ml_studies_decorator(func, func.__name__, 'subtask')

import threading
from time import sleep
from typing import Callable, Optional

from models import IndexEntry, Report
from enums import WorkerNames, OutputPaths
from commons import timeit, log


def worker(name: str | WorkerNames = None):
    def inner(func):
        return _Worker(func, name.value if isinstance(name, WorkerNames) else name)
    return inner


class _Worker:
    def __init__(self, func, name: Optional[WorkerNames]):
        self.func = func
        self.name = name or func.__name__
        self.task_name = None

    def __call__(self, *args, **kwargs):
        return self.worker(*args, **kwargs)

    def worker(self, *args, **kwargs):
        log(f'Started worker: {self.name}')
        result, exception, elapsed = timeit(self.func)(*args, **kwargs)

        if exception:
            message = f'Exception occurred: {type(exception).__name__} {str(exception)}'
            level = 'error'
        else:
            message = 'Finished worker'
            level = 'success'

        log('{}. Worker: {}. Time Elapsed: {}'.format(message, self.name, str(elapsed)), level=level)
        return result

    def task(self, func):
        self.task_name = func.__name__

        def inner(*args, **kwargs):
            entry = kwargs.get('entry') or next(iter([a for a in args if isinstance(a, IndexEntry)]), None)
            if not entry:
                raise Exception('Must pass "entry of class <IndexEntry>" to worker task')

            output_path = OutputPaths[WorkerNames(self.name).name].value.format(entry.output_filename)
            report = Report(output_path=output_path)
            report.open()

            result, exception, elapsed = timeit(func)(*args, **kwargs)

            if exception:
                report.fail(str(exception))
                message = f'Exception occurred: {type(exception).__name__} {str(exception)}'
                level = 'error'
            else:
                report.success()
                message = 'Finished task'
                level = 'success'

            template = '{}. Worker: {}. Task: {}. Time Elapsed: {}'
            log(template.format(message, self.name, self.task_name, str(elapsed)), level=level)

            if self.name in [w.value for w in WorkerNames]:
                entry.reports[self.name] = report

            return result

        inner.__name__ = self.task_name
        return inner

    def subtask(self, func):
        subtask_name = func.__name__

        def inner(*args, **kwargs):
            result, exception, elapsed = timeit(func)(*args, **kwargs)
            if exception:
                message = f'Exception occurred: {type(exception).__name__} {str(exception)}'
                level = 'error'
            else:
                message = 'Finished subtask'
                level = 'success'

            template = '{}. Worker: {}. Task: {}. Subtask: {}. Time Elapsed: {}'
            log(template.format(message, self.name, self.task_name, subtask_name, str(elapsed)), level=level)
            return result
        return inner


def run_concurrently(t_args_list: list[tuple[Callable, tuple, dict]], max_concurrent_threads=100):
    ts = []

    for i, t_args in enumerate(t_args_list):
        t_kwargs = {
            'target': t_args[0],
            'daemon': True,
            'name': 'ml-studies-thread-' + str(i)
        }
        if t_args[1]:
            t_kwargs['args'] = t_args[1] if isinstance(t_args[1], tuple) else (t_args[1],)
        if t_args[2]:
            t_kwargs['kwargs'] = t_args[2]

        t = threading.Thread(**t_kwargs)
        t.start()

        while len([t for t in threading.enumerate() if t.name.startswith('ml-studies-thread')]) >\
                max_concurrent_threads:
            sleep(1)

    for t in ts:
        t.join()

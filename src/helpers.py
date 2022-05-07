import threading
from time import sleep
from typing import Callable


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

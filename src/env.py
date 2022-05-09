from os import environ


def env(default: str = None):
    return environ.get('ML_STUDIES_ENV', default)


def is_env_prod():
    return env() == 'prod'


def is_env_dev():
    return env() == 'dev'

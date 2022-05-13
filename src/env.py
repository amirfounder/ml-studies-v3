from os import environ


def working_env(default: str = None):
    return environ.get('ML_STUDIES_ENV', default)


def is_env_prod():
    return working_env() == 'prod'


def is_env_dev():
    return working_env() == 'dev'

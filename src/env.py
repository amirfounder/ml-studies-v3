from os import environ


def is_env_prod():
    return environ.get('ML_STUDIES_ENV') == 'prod'


def is_env_dev():
    return environ.get('ML_STUDIES_ENV') == 'dev'

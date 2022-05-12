from src.commons import get_levenshtein_distance as fn


def test_get_levenshtein_distance():

    t1 = 'hello'
    t2 = 'hell'
    d = fn(t1, t2)

    assert d == 1

    t1 = ''
    t2 = 'hi'

    d = fn(t1, t2)

    assert d == 2

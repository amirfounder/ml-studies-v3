from src.commons import get_levenshtein_distance as fn, get_sentence_similarity_score


def test_get_levenshtein_distance():

    t1 = 'hello'
    t2 = 'hell'
    d = fn(t1, t2)

    assert d == 1

    t1 = ''
    t2 = 'hi'

    d = fn(t1, t2)

    assert d == 2


def test_get_sentence_similarity_score():
    s1 = ['Hello', 'my', 'name', 'is', 'matt']
    s2 = ['Hello', 'my', 'name', 'is', 'amir']

    score = get_sentence_similarity_score(s1, s2)
    assert score == .96

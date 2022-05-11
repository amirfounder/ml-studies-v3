import json
import re

import bs4
import requests

from ..commons import write, read, nlp
from ..decorators import task, log_report, threaded
from ..enums import ReportTypes, Paths
from ..models import IndexEntry


@threaded(max_threads=50)
@log_report(ReportTypes.SCRAPE_ARTICLE)
@task()
def scrape_html(entry: IndexEntry):
    output_path = Paths.SCRAPE_HTMLS_OUTPUT.format(**dict(entry))
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)
    

@threaded(max_threads=100)
@log_report(ReportTypes.EXTRACT_TEXT)
@task()
def extract_text(entry: IndexEntry):
    input_path = Paths.SCRAPE_HTMLS_OUTPUT.format(**dict(entry))
    output_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')

    text = soup.text
    text = re.sub(' {2,}', ' ', text)
    text = re.sub('\n{2,}', '\n', text)
    text = re.sub('\t{2,}', ' ', text)
    text.removeprefix('\n')
    text.removesuffix('\n')

    write(output_path, text)


@threaded(max_threads=100)
@log_report(ReportTypes.ANALYZE_TEXT)
@task()
def analyze_text(entry: IndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))
    output_path = Paths.ANALYZE_TEXTS_OUTPUT.format(**dict(entry))

    text = read(input_path)
    doc = nlp(text)
    tokens = [
        token for token in doc if
        not token.is_stop and
        not token.is_punct and
        not token.like_url and
        not token.like_email and
        not token.text.startswith('@') and
        not token.is_space
    ]

    lemmas = [token.lemma_ for token in tokens]

    lengths = {}
    occurrences = {}
    for lemma in lemmas:
        lengths[lemma] = len(lemma)
        if lemma not in occurrences:
            occurrences[lemma] = 0
        occurrences[lemma] += 1

    total = len(lemmas)
    frequencies = {}
    for lemma, _occurrences in occurrences.items():
        frequencies[lemma] = _occurrences / total

    models = {
        lemma: {
            'occurrences': occurrences[lemma],
            'frequency': frequencies[lemma],
            'length': lengths[lemma],
            'syllables': None
        } for lemma in lemmas
    }

    views = {
        'occurrences': occurrences,
        'frequencies': frequencies,
        'syllables': None,  # TODO implement this?
        'lengths': lengths
    }

    contents = {
        'views': views,
        'models': models
    }

    write(output_path, json.dumps(contents))


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SENTIMENT_ANALYSIS)
@task()
def create_sentiment_analysis(entry: IndexEntry):
    standard_sentiment = None
    fine_grained_sentiment = None
    emotion = None
    intent = None
    aspect_based_sentiment = None


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SUMMARY)
@task()
def create_summary(entry: IndexEntry):
    pass

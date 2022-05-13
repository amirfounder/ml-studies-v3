import json
import re

import bs4
import requests
import contractions

from ..commons import write, read, nlp
from ..index_manager import get_index
from ..decorators import task, log_report, threaded
from ..enums import ReportTypes, Paths
from ..env import is_env_dev
from ..models import ArticleIndexEntry, SentenceIndexEntry


@threaded(max_threads=1)
@log_report(ReportTypes.SCRAPE_ARTICLE)
@task()
def scrape_html(entry: ArticleIndexEntry):
    output_path = Paths.SCRAPE_HTMLS_OUTPUT.format(source=entry.source, filename=entry.filename)
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)


@threaded(max_threads=100)
@log_report(ReportTypes.EXTRACT_TEXT)
@task()
def extract_text(entry: ArticleIndexEntry):
    input_path = Paths.SCRAPE_HTMLS_OUTPUT.format(**dict(entry))
    output_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')

    text = soup.text
    text = re.sub(' {2,}', ' ', text)
    text = re.sub('\n{2,}', '\n', text)
    text = re.sub('\t{2,}', ' ', text)
    text.removeprefix('\n')
    text.removesuffix('\n')
    text = contractions.fix(text)

    write(output_path, text)


@threaded(max_threads=1 if is_env_dev() else 100)
@log_report(ReportTypes.ANALYZE_TEXT)
@task()
def analyze_text(entry: ArticleIndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))
    output_path = Paths.ANALYZE_TEXTS_OUTPUT.format(**dict(entry))

    text = read(input_path)
    doc = nlp(text)

    lemmatized_sentences = []

    with get_index('sentences') as sentence_index:
        for sentence in doc.sents:
            
            lemmas = [
                token.lemma_.lower() for token in sentence if
                not token.is_stop and
                not token.is_punct and
                not token.like_url and
                not token.like_email and
                not token.text.startswith('@') and
                not token.is_space
            ]

            sequence = ' '.join(lemmas)
            
            if sequence in sentence_index and \
                    entry.filename not in sentence_index[sequence].occurred_in_articles:
                sentence_index[sequence].occurrences += 1
                sentence_index[sequence].occurred_in_articles.append(entry.filename)
                continue

            sentence_index[sequence] = SentenceIndexEntry(
                occurrences=1,
                occurred_in_articles=[entry.filename],
                non_lemmatized_sequence=sentence.text
            )
            lemmatized_sentences.append(lemmas)

    lemmas = []
    for lemmatized_sentence in lemmatized_sentences:
        lemmas.extend(lemmatized_sentence)

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

    lemmas = {
        lemma: {
            'occurrences': occurrences[lemma],
            'frequency': frequencies[lemma],
            'length': lengths[lemma],
            'syllables': None
        } for lemma in lemmas
    }

    contents = {
        'lemmas': lemmas,
        'lemmatized_sentences': {i: sentence for i, sentence in enumerate(lemmatized_sentences)}
    }

    write(output_path, json.dumps(contents))


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SENTIMENT_ANALYSIS)
@task()
def create_sentiment_analysis(entry: ArticleIndexEntry):
    standard_sentiment = None
    fine_grained_sentiment = None
    emotion = None
    intent = None
    aspect_based_sentiment = None


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SUMMARY)
@task()
def create_summary(entry: ArticleIndexEntry):
    pass

import json
import re

import bs4
import requests
import contractions

from textblob import TextBlob

from ..commons import write, read, nlp, info, try_load_json
from ..index_manager import get_index
from ..decorators import task, log_report, threaded
from ..enums import ReportTypes, Paths
from ..models import ArticleIndexEntry, SentenceIndexEntry


@threaded()
@log_report(ReportTypes.SCRAPE_ARTICLE)
@task()
def scrape_html(entry: ArticleIndexEntry):
    output_path = Paths.SCRAPE_HTMLS_OUTPUT.format(**dict(entry))
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)


@threaded()
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
    text = text.removeprefix('\n')
    text = text.removesuffix('\n')
    text = contractions.fix(text)

    write(output_path, text)


@threaded()
@log_report(ReportTypes.ANALYZE_TEXT)
@task()
def analyze_text(entry: ArticleIndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))
    output_path = Paths.ANALYZE_TEXTS_OUTPUT.format(**dict(entry))

    text = read(input_path)
    doc = nlp(text)

    lemmatized_sentences = []

    with get_index('sentences') as sentence_index:
        prev_sentences_count = sentence_index.sentences_count
        for i, sentence in enumerate(doc.sents):
            
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

            lemmatized_sentences.append((i, lemmas))

        info(f'New sentences index: {str(sentence_index.sentences_count - prev_sentences_count)}')

    lemmas = []
    for _, lemmatized_sentence in lemmatized_sentences:
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
        'lemmatized_sentences': {i: sentence for i, sentence in lemmatized_sentences}
    }

    write(output_path, json.dumps(contents))


@threaded()
@log_report(ReportTypes.CREATE_SENTIMENT_ANALYSIS)
@task()
def create_sentiment_analysis(entry: ArticleIndexEntry):
    input_path = Paths.ANALYZE_TEXTS_OUTPUT.format(**dict(entry))
    output_path = Paths.SENTIMENT_ANALYSES_OUTPUT.format(**dict(entry))

    processed_data = try_load_json(read(input_path))
    sentences = processed_data['lemmatized_sentences']
    sentences = [(index, ' '.join(sequence)) for index, sequence in sentences.items()]

    analysis_output = {
        'standard_sentiment': {
            'textblob': {}
        },
        'fine_grained_sentiment': None,
        'emotion': None,
        'intent': None,
        'aspect_based_sentiment': None
    }

    reference = analysis_output['standard_sentiment']['textblob']

    article = '. '.join([s for _, s in sentences])
    article_blob = TextBlob(article)

    reference['sentences'] = {}
    reference['overall'] = {
        'polarity': article_blob.polarity,
        'subjectivity': article_blob.subjectivity
    }

    for i, sentence in sentences:
        sentence_blob = TextBlob(sentence)

        polarity_value = sentence_blob.polarity
        polarity_deviation = reference['overall']['polarity'] - polarity_value

        subjectivity_value = sentence_blob.subjectivity
        subjectivity_deviation = reference['overall']['subjectivity'] - subjectivity_value

        reference['sentences'][i] = {
            'original_sentence': None,
            'lemmatized_sentence': None,
            'polarity': {
                'value': polarity_value,
                'deviation': polarity_deviation
            },
            'subjectivity': {
                'value': subjectivity_value,
                'deviation': subjectivity_deviation
            }
        }

    write(output_path, json.dumps(analysis_output))


@threaded()
@log_report(ReportTypes.CREATE_SUMMARY)
@task()
def create_summary(entry: ArticleIndexEntry):
    pass

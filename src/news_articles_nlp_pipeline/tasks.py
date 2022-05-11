import json
import re

import bs4
import requests
import contractions

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


@log_report(ReportTypes)
@task()
def image_article(entry: IndexEntry):
    # output_path = Paths

    # TODO for optimization. for now, wait 3 seconds
    # start server? (here or in pipeline?)
    # server.add_listener('webpage_loaded')

    # open(entry.url)
    # server.wait_for_listener_response('webpage_loaded') # todo part of todo mentioned above
    # paths = {}
    # more_to_scroll = True
    # i = 1
    # prev_img = screenshot()
    # path = path_template.format(filename=i)
    # save(path, prev_img)
    # paths[i] = path

    # while more_to_scroll:
    #     i += 1
    #     mouse.scroll(10)
    #     current_img = screenshot()
    #     path = path_template.format(filename=i)
    #     paths[i] = path
    #     save(path, current_img)
    #     more_to_scroll = (similarity_score(prev_img, current_img) < 90) or i < 20
    #     prev_img = current_img

    # image = stitch_images_pipeline(paths=paths.values())
    # path = path_template.format(filename='stitched')
    # paths['stitched'] = path
    # save(path, image.bytes, mode='wb')
    # text_sections = image.get_text_sections()
    # main_text = [section.is_main for section in text_sections]
    # approved = get_approval_from_supervisor()
    pass


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
    text = contractions.fix(text)

    write(output_path, text)


@threaded(max_threads=1)
@log_report(ReportTypes.ANALYZE_TEXT)
@task()
def analyze_text(entry: IndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(**dict(entry))
    output_path = Paths.ANALYZE_TEXTS_OUTPUT.format(**dict(entry))

    text = read(input_path)
    doc = nlp(text)

    sentences = [s.text for s in doc.sents]

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
        'models': models,
        'sentences': sentences
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

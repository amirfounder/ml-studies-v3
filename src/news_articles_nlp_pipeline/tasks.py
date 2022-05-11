import pickle
import re

import bs4
import requests
from wordcloud import WordCloud
from PIL import Image

from .subtasks import clean_tokens
from ..commons import write, read, nlp, makedirs_from_path
from ..decorators import task, log_report, threaded
from ..enums import ReportTypes, Paths
from ..models import IndexEntry


@threaded(max_threads=50)
@log_report(ReportTypes.SCRAPE_ARTICLE)
@task()
def scrape_html(entry: IndexEntry):
    output_path = Paths.SCRAPE_HTMLS_OUTPUT.format(source=entry.source, filename=entry.filename)
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)
    

@threaded(max_threads=100)
@log_report(ReportTypes.EXTRACT_TEXT)
@task()
def extract_text(entry: IndexEntry):
    input_path = Paths.SCRAPE_HTMLS_OUTPUT.format(source=entry.source, filename=entry.filename)
    output_path = Paths.EXTRACT_TEXTS_OUTPUT.format(source=entry.source, filename=entry.filename)

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')
    write(output_path, soup.text)


@threaded(max_threads=100)
@log_report(ReportTypes.PROCESS_TEXT)
@task()
def process_text(entry: IndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(source=entry.source, filename=entry.filename)
    output_path = Paths.PROCESS_TEXTS_OUTPUT.format(source=entry.source, filename=entry.filename)

    text = read(input_path)
    text = re.sub(' {2,}', ' ', text)
    text = re.sub('\n{2,}', '\n', text)
    text = re.sub('\t{2,}', ' ', text)

    doc = nlp(text)
    write(output_path, contents=pickle.dumps(doc), mode='wb')


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_WORDCLOUD)
@task()
def create_wordcloud(entry: IndexEntry):
    input_path = Paths.PROCESS_TEXTS_OUTPUT.format(source=entry.source, filename=entry.filename)
    output_csv_path = Paths.WORDCLOUD_OUTPUTS.format(**dict(entry))
    output_img_path = Paths.WORDCLOUD_IMAGES_OUTPUT.format(**dict(entry))

    doc = pickle.loads(read(input_path, mode='rb'))
    tokens = clean_tokens(doc)[0]
    lemmas = [token.lemma_.lower() for token in tokens]

    lemma_map = {}
    lemmas_len = len(lemmas)

    for lemma in lemmas:
        if lemma not in lemma_map:
            lemma_map[lemma] = dict(
                count=0,
                frequency=None,
                lemma=lemma
            )
        lemma_map[lemma]['count'] += 1

    for k, v in lemma_map.items():
        lemma_map[k]['frequency'] = v['count'] / lemmas_len

    wc = WordCloud(background_color='white', height=500, width=500, prefer_horizontal=.9)
    wc.generate_from_text(' '.join(lemmas))
    makedirs_from_path(output_img_path)
    wc.to_file(output_img_path)

    rows = [['lemma', 'count', 'frequency']]
    for lemma_obj in list(lemma_map.values()):
        row = []
        for k in rows[0]:
            row.append('"' + str(lemma_obj[k]) + '"')
        rows.append(row)

    contents = '\n'.join([','.join(row) for row in rows])
    write(output_csv_path, contents)


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
@log_report(ReportTypes.CREATE_N_GRAM_ANALYSIS)
@task()
def create_n_gram_analysis(entry: IndexEntry):
    pass


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SUMMARY)
@task()
def create_summary(entry: IndexEntry):
    pass

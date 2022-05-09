import pickle

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
    
    doc = nlp(read(input_path))
    write(output_path, contents=pickle.dumps(doc), mode='wb')


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_WORDCLOUD)
@task()
def create_wordcloud(entry: IndexEntry):
    pass


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_SENTIMENT_ANALYSIS)
@task()
def create_sentiment_analysis(entry: IndexEntry):
    pass


@threaded(max_threads=100)
@log_report(ReportTypes.CREATE_EMOTIONAL_ANALYSIS)
@task()
def create_emotional_analysis(entry: IndexEntry):
    pass


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

import pickle

import bs4
import requests

from ..commons import write, read, nlp
from ..decorators import task, log_report
from ..enums import ReportTypes, Paths
from ..models import IndexEntry


@log_report(ReportTypes.SCRAPE_ARTICLES)
@task
def scrape_html(entry: IndexEntry):
    output_path = Paths.SCRAPE_HTMLS_OUTPUT.format(filename=entry.filename)
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)
    

@log_report(ReportTypes.EXTRACT_TEXTS)
@task
def extract_text(entry: IndexEntry):
    input_path = Paths.SCRAPE_HTMLS_OUTPUT.format(filename=entry.filename)
    output_path = Paths.EXTRACT_TEXTS_OUTPUT.format(filename=entry.filename)

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')
    write(output_path, soup.text)


@log_report(ReportTypes.PROCESS_TEXTS)
@task
def process_text(entry: IndexEntry):
    input_path = Paths.EXTRACT_TEXTS_OUTPUT.format(filename=entry.filename)
    output_path = Paths.PROCESS_TEXTS_OUTPUT.format(filename=entry.filename)
    
    doc = nlp(read(input_path))
    with open(output_path, 'wb') as f:
        pickle.dump(doc, f)

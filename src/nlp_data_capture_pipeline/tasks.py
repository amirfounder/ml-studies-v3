import pickle

import bs4
import requests

from ..commons import write, read, nlp
from ..decorators import task, log_report
from ..enums import OutputPaths, ReportTypes
from ..models import IndexEntry


@log_report(ReportTypes.SCRAPE_ARTICLES)
@task
def scrape_html(entry: IndexEntry):
    output_path = OutputPaths.SCRAPE_HTMLS.format(entry.filename)
    
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(output_path, resp.text)
    

@log_report(ReportTypes.EXTRACT_TEXTS)
@task
def extract_text(entry: IndexEntry):
    input_path = OutputPaths.SCRAPE_HTMLS.value.format(entry.filename)
    output_path = OutputPaths.EXTRACT_TEXTS.value.format(entry.filename)

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')
    write(output_path, soup.text)


@log_report(ReportTypes.PROCESS_TEXTS)
@task
def process_text(entry: IndexEntry):
    input_path = OutputPaths.EXTRACT_TEXTS.format(entry.filename)
    output_path = OutputPaths.PROCESS_TEXTS.format(entry.filename)
    
    doc = nlp(read(input_path))
    with open(output_path, 'wb') as f:
        pickle.dump(doc, f)




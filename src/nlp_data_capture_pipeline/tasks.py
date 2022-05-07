import pickle
import time

import bs4
import requests

from ..commons import write, read, nlp
from ..decorators import task, log_report
from ..enums import OutputPaths, Reports
from ..models import IndexEntry


@log_report(Reports.ExtractTexts)
@task
def extract_text(entry: IndexEntry):
    input_path = OutputPaths.SCRAPE_HTMLS.value.format(entry.output_filename)
    output_path = OutputPaths.EXTRACT_TEXTS.value.format(entry.output_filename)

    soup = bs4.BeautifulSoup(read(input_path), 'html.parser')
    write(output_path, soup.text)


@log_report(Reports.ScrapeHtml)
@task
def scrape_html(entry: IndexEntry):
    resp = requests.get(entry.url)
    resp.raise_for_status()
    write(OutputPaths.SCRAPE_HTMLS.format(entry.output_filename), resp.text)
    time.sleep(1)


@log_report(Reports.ProcessTexts)
@task
def process_text(entry: IndexEntry):
    doc = nlp(read(entry.reports[scrape_html.__name__].output_path))
    with open(entry.reports[process_text.__name__].output_path, 'wb') as f:
        pickle.dump(doc, f)




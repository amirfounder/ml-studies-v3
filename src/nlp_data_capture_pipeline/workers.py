import feedparser

from ..commons import info
from ..decorators import worker
from ..models import Index, IndexEntry, Report
from ..enums import WorkerNames, OutputPaths, Status, Reports
from .tasks import get_cnn_rss_urls, get_cnn_money_rss_urls, scrape_html, extract_text, process_text


@worker
def index_rss_entries():
    with Index() as index:
        topics_urls = [*get_cnn_rss_urls()[0], *get_cnn_money_rss_urls()[0]]
        topics_entries = []

        for topic, rss_url in topics_urls:
            topics_entries.append((topic, feedparser.parse(rss_url).entries))

        count = 0
        for topic, entries in topics_entries:
            for entry in entries:
                url = entry['link']

                if url not in index and 'cnn.com' in url[:20]:
                    next_file_name = str(len(index) + 1)

                    reports = {}
                    for n, v in [(n.name, n.value) for n in WorkerNames]:
                        reports[v] = Report(output_path=OutputPaths[n].value.format(next_file_name))

                    index[url] = IndexEntry(
                        _index=index,
                        url=url,
                        topic=topic,
                        reports=reports,
                        output_filename=next_file_name
                    )

        info(f'New entries indexed: {count}')


@worker
def scrape_htmls():

    with Index(filter_callback=lambda e: not e.reports[scrape_htmls.__name__].has_been_attempted) as index:
        for entry in index.values():
            r, e = scrape_html(entry)
            entry.reports[scrape_htmls] = Report.open().close(r, e)


@worker
def extract_texts():

    def filter_callback(_entry: IndexEntry):
        return (
            _entry.reports[extract_texts.__name__].status == Status.SUCCESS and
            not _entry.reports[extract_texts.__name__].has_been_attempted
        )

    with Index(filter_callback=filter_callback) as index:
        for entry in index.entries():
            r, e = extract_text(entry)
            entry.reports[extract_texts] = Report.open().close(r, e)


@worker
def process_texts():

    def filter_callback(_entry: IndexEntry):
        return (
            entry[WorkerNames.EXTRACT_TEXTS].status == Status.SUCCESS and
            not entry[WorkerNames.PROCESS_TEXTS].status == Status.SUCCESS
        )

    with Index(filter_callback) as index:
        for entry in index.values():
            r, e = process_text(entry)
            entry.reports[Reports.ProcessTexts] = Report.open().close(r, e)

import feedparser

from ..commons import info
from ..decorators import worker
from ..models import IndexEntry, IndexManager
from ..enums import Status, ReportTypes
from .tasks import scrape_html, extract_text, process_text
from .subtasks import get_cnn_rss_urls, get_cnn_money_rss_urls


@worker
def index_rss_entries():
    with IndexManager('cnn') as index:
        prev_entries_count = index.entries_count
        entries = index.entries
        
        topics_urls = [
            *get_cnn_rss_urls()[0],
            *get_cnn_money_rss_urls()[0]
        ]
        
        topics_entries = []
        for topic, rss_url in topics_urls:
            topics_entries.append((topic, feedparser.parse(rss_url).entries))

        for topic, new_entries in topics_entries:
            for entry in new_entries:
                url = entry['link']

                if url not in entries and 'cnn.com' in url[:20]:
                    next_file_name = str(len(entries) + 1)

                    entries[url] = IndexEntry(
                        _index=entries,
                        url=url,
                        topic=topic,
                        filename=next_file_name
                    )

        info(f'New entries indexed: {prev_entries_count - index.entries}')


@worker
def scrape_htmls():

    def filter_callback(_entry: IndexEntry):
        return not _entry.reports[ReportTypes.SCRAPE_ARTICLES.value].has_been_attempted

    with IndexManager(filter_callback=filter_callback) as index:
        for entry in index.entries.values():
            scrape_html(entry)


@worker
def extract_texts():

    def filter_callback(_entry: IndexEntry):
        return (
            _entry.reports[ReportTypes.EXTRACT_TEXTS.value].status == Status.SUCCESS and
            not _entry.reports[ReportTypes.EXTRACT_TEXTS.value].has_been_attempted
        )

    with IndexManager(filter_callback=filter_callback) as index:
        for entry in index.entries.values():
            extract_text(entry)


@worker
def process_texts():

    def filter_callback(_entry: IndexEntry):
        return (
            entry.reports[ReportTypes.EXTRACT_TEXTS.value].status == Status.SUCCESS and
            not entry.reports[ReportTypes.PROCESS_TEXTS.value].status == Status.SUCCESS
        )

    with IndexManager(filter_callback=filter_callback) as index:
        for entry in index.entries.values():
            process_text(entry)

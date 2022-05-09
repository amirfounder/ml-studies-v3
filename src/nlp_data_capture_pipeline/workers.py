from ..commons import info
from ..decorators import worker, join_threads
from ..env import is_env_dev
from ..models import IndexEntry, get_index
from ..enums import Status, ReportTypes
from .tasks import scrape_html, extract_text, process_text, create_wordcloud
from .subtasks import get_cnn_rss_urls, get_cnn_money_rss_urls, scrape_rss_entries


@worker
def index_newest_articles():
    with get_index('all') as index:
        prev_entries_count = index.entries_count
        entries = index.get_entries()
        
        topics_urls = [
            *get_cnn_rss_urls()[0],
            *get_cnn_money_rss_urls()[0]
        ]
        
        topics_entries = []
        for topic, rss_url in topics_urls:
            new_entries, exception = scrape_rss_entries(rss_url)
            topics_entries.append((topic, new_entries))

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

        info(f'New entries indexed: {index.entries_count - prev_entries_count}')


@worker
def scrape_articles():

    def filter_fn(_entry: IndexEntry):
        return (
            (
                int(_entry.filename) <= 10 and
                not _entry.reports[ReportTypes.SCRAPE_ARTICLE.value].has_been_attempted
            )
            if is_env_dev() else
            (
                not _entry.reports[ReportTypes.SCRAPE_ARTICLE.value].has_been_attempted
            )
        )

    with get_index('cnn') as index:
        for entry in index.get_entries(filter_fn=filter_fn).values():
            scrape_html(entry)


@worker
def extract_texts():

    def filter_fn(_entry: IndexEntry):
        return (
            _entry.reports[ReportTypes.SCRAPE_ARTICLE.value].status == Status.SUCCESS
            # not _entry.reports[ReportTypes.EXTRACT_TEXT.value].has_been_attempted
        )

    with get_index('cnn') as index:
        for entry in index.get_entries(filter_fn=filter_fn).values():
            extract_text(entry)
        
        join_threads(extract_text)


@worker
def process_texts():

    def filter_fn(_entry: IndexEntry):
        return (
            _entry.reports[ReportTypes.EXTRACT_TEXT.value].status == Status.SUCCESS and
            not _entry.reports[ReportTypes.PROCESS_TEXT.value].status == Status.SUCCESS
        )

    with get_index('cnn') as index:
        for entry in index.get_entries(filter_fn=filter_fn).values():
            process_text(entry)


@worker
def create_wordclouds():

    def filter_fn(_entry: IndexEntry):
        return (
            _entry.reports[ReportTypes.PROCESS_TEXT.value].status == Status.SUCCESS and
            not _entry.reports[ReportTypes.CREATE_WORDCLOUD.value].has_been_attempted
        )

    with get_index('cnn') as index:
        for entry in index.get_entries(filter_fn=filter_fn).values():
            create_wordcloud(entry)

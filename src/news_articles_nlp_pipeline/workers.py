from ..commons import info
from ..context_managers import get_article_index
from ..decorators import worker, join_threads
from ..env import is_env_dev
from ..models import ArticleIndexEntry
from ..enums import Status, ReportTypes
from .tasks import scrape_html, extract_text, analyze_text
from .subtasks import get_cnn_rss_urls, get_cnn_money_rss_urls, scrape_rss_entries


@worker
def index_newest_articles():
    with get_article_index() as index:
        prev_entries_count = index.articles_count
        entries = index.get_articles()
        
        topics_urls = [
            *get_cnn_rss_urls()[0],
            *get_cnn_money_rss_urls()[0]
        ]
        
        topics_entries = []
        for topic, rss_url in topics_urls:
            new_entries, exception, _ = scrape_rss_entries(rss_url)
            topics_entries.append((topic, new_entries))

        for topic, new_entries in topics_entries:
            for entry in new_entries:
                url = entry['link']

                if url not in entries and 'cnn.com' in url[:20]:
                    next_file_name = str(len(entries) + 1)

                    entries[url] = ArticleIndexEntry(
                        _index=entries,
                        url=url,
                        topic=topic,
                        filename=next_file_name,
                        source='cnn'
                    )

        info(f'New entries indexed: {index.articles_count - prev_entries_count}')


@worker
def scrape_articles():

    def filter_callback(_entry: ArticleIndexEntry):
        first_ten = int(_entry.filename) <= 10
        attempted = _entry.reports[ReportTypes.SCRAPE_ARTICLE.value].has_been_attempted
        return first_ten and not attempted if is_env_dev() else not attempted

    with get_article_index() as index:
        for entry in index.get_articles(filter_callback=filter_callback).values():
            scrape_html(entry)

        join_threads(scrape_html)


@worker
def extract_texts():

    def filter_callback(_entry: ArticleIndexEntry):
        prev_success = _entry.reports[ReportTypes.SCRAPE_ARTICLE.value].status == Status.SUCCESS
        attempted = _entry.reports[ReportTypes.EXTRACT_TEXT.value].has_been_attempted
        return prev_success if is_env_dev() else prev_success and not attempted

    with get_article_index() as index:
        for entry in index.get_articles(filter_callback=filter_callback).values():
            extract_text(entry)
        
        join_threads(extract_text)


@worker
def analyze_texts():

    def filter_callback(_entry: ArticleIndexEntry):
        prev_success = _entry.reports[ReportTypes.EXTRACT_TEXT.value].status == Status.SUCCESS
        attempted = _entry.reports[ReportTypes.ANALYZE_TEXT.value].has_been_attempted
        return prev_success if is_env_dev() else prev_success and not attempted

    with get_article_index() as index:
        for entry in index.get_articles(filter_callback=filter_callback).values():
            analyze_text(entry)

        join_threads(analyze_text)


@worker
def create_sentiment_analyses():
    pass


@worker
def create_summaries():
    pass

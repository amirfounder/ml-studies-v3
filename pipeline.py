import time

import schedule

from helpers import worker
from workers import (
    index_latest_rss_entries,
    extract_text_from_article_v2,
    scrape_latest_urls_from_index,
    preprocess_extracted_text_v1
)


@worker()
def pipeline():
    index_latest_rss_entries()
    scrape_latest_urls_from_index()
    extract_text_from_article_v2()
    preprocess_extracted_text_v1()


if __name__ == '__main__':
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

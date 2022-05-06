import time

import schedule

from helpers import worker
from workers import (
    index_rss_entries,
    extract_texts,
    scrape_htmls,
    preprocess_texts,
    sync_index
)


@worker()
def pipeline():
    index_rss_entries()
    scrape_htmls()
    extract_texts()
    preprocess_texts()


if __name__ == '__main__':
    sync_index()
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

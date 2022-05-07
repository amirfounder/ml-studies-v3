import time

import schedule

from pipeline import pipeline
# from workers import sync_index

if __name__ == '__main__':
    # sync_index()
    pipeline()
    schedule.every(30).minutes.do(pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

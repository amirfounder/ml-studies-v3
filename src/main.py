import time

import schedule

from nlp_data_capture_pipeline.pipeline import nlp_data_capture_pipeline


if __name__ == '__main__':
    nlp_data_capture_pipeline()
    schedule.every(30).minutes.do(nlp_data_capture_pipeline)

    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        finally:
            print('Program closed.')

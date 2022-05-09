import sys
import time

import schedule

from src.nlp_data_capture_pipeline.pipeline import nlp_data_capture_pipeline
from src.commons import set_env_to_dev, set_env_to_prod

if __name__ == '__main__':
    set_env_to_prod() if '--prod' in sys.argv[1:] else set_env_to_dev()

    nlp_data_capture_pipeline()
    schedule.every(30).minutes.do(nlp_data_capture_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

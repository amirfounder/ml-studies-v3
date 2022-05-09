import sys
import time

import schedule

from src.news_articles_nlp_pipeline.pipeline import news_articles_nlp_pipeline
from src.commons import set_env_to_dev, set_env_to_prod

if __name__ == '__main__':
    set_env_to_prod() if '--prod' in sys.argv[1:] else set_env_to_dev()

    news_articles_nlp_pipeline()
    schedule.every(30).minutes.do(news_articles_nlp_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)

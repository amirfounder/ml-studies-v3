# ML Studies

## Entities:

### Index

An index is the holy grail of our data -- It contains references to all data collected.
Future iterations will improve on the index.

### Report

A report is a summary of a task.

### Pipeline

A pipeline is a series of functions (AKA: workers) that run one after the other.

### Worker

A worker is a function that runs one or more tasks, often concurrently in threads.

### Task

A task a piece of code that runs. This should be thread safe.

## Example:

```python
from src import *


@worker
def scrape_rss_feeds():
    
    @scrape_rss_feeds.task
    def scrape_cnn_rss():
        pass
    
    @scrape_rss_feeds.task
    def scrape_fox_rss():
        pass
    
    @scrape_rss_feeds.task
    def main():
        pass
    
    with Index() as index:
        entries_to_index = [
            **scrape_cnn_rss,
            **scrape_fox_rss
        ]
        for entry in entries_to_index:
            pass
    
    ...

@worker
def scrape_urls():
    ...

@worker
def extract_text():
    ...

@worker
def process_text():
    ...

@pipeline
def nlp_extract_from_web_pipeline():
    scrape_rss_feeds()
    scrape_urls()
    extract_text()
    process_text()

```
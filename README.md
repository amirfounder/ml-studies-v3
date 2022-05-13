# ML Studies (for users)

## Usage

Close your eyes and type the following command:

```commandline
python main.py --prod
```

No setup needed (I think).
This program should build out any additional directory structure as needed if it doesn't already exist.

# ML Studies (for devs)

## Design

### Design Pattern
The primary design pattern used here is the pipe and filter design patter.
Every pipeline is a... pipeline and every worker is a filter.
I added tasks and subtasks which are used for concurrency operations.
*(See the `Decorators` section below for more details)*

### Programming Paradigm
The primary paradigm used here is functional programming.
OOP is ONLY used for modeling data in this project.

## Entities:

### Index

An index is the holy grail of our data -- It contains references to all data collected.
Future iterations will improve on the index.

### Report

A report is a summary of a task.

## Decorators

### Pipeline

A pipeline is a series of functions (AKA: workers) that run one after the other.
Each pipeline has a module of its own in the  `src` directory of the package.

### Worker

A worker is a function that runs one or more tasks and subtasks, often concurrently in threads.
Can be thought of as a batch runner - running a batch of N tasks at each step of the pipeline.

### Task

A task a piece of code that runs in a worker.
This function should implement thread / process safe protocols.

### Subtask

A function called by the task.
The lowest level in the categorical hierarchy of functions.
This function should also (like a task) implement thread / process safe protocols.

# todo

- parameter to allow index to flush regularly (will need as we scale the amount of news articles scraped)
- custom thread to return values and store them into a pool of results that is retrieved by the `join_threads` fn
- file locks

# major change / decision log

## removing pickle files
initially, we were saving the spacy doc to a pickle file in a worker called process_texts.
after saving 1000 files, the size was ~2.7gb.
for comparison, the extracted text size of 1000 articles was ~10mb.
we are now having the processing of the data (removing redundant whitespaces) as part of the extract text worker

## 

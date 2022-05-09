# ML Studies

## Usage

Close your eyes and type the following command:

```commandline
python main.py --prod
```

No setup needed (I think).
This program should build out any additional directory structure as needed if it doesn't already exist.

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

### Task

A task a piece of code that runs.
This function should implement thread / process safe protocols.

### Subtask

A function called by the task.
The lowest level in the categorical hierarchy of functions.
This function should also (like a task) implement thread / process safe protocols.

# todo

- parameter to allow index to flush regularly
- custom thread to return values and store them into a pool of results that is retrieved by the `join_threads` fn
- file locks
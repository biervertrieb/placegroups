# Contribution Guide

## Developing inside a Container

The Visual Studio Code Remote - Containers extension lets you use a Docker container as a full-featured development environment. It allows you to open any folder inside (or mounted into) a container and take advantage of Visual Studio Code's full feature set.

## Variant 1 - Source Code Repository on local machine
This is the recommended variant.

### Requirements

- install Git
- install Docker Desktop
- install VS Code
- install Remote Development extension pack for VS Code

### Setup

- go to your workspace directory
- clone https://github.com/biervertrieb/placegroups
- open VS Code inside workspace directory and press Ctrl+Shift+P
- execute "Remote-Containers: Reopen in Container"

## Variant 2 - Source Code Repository inside container

### Requirements

- install Docker Desktop
- install VS Code
- install Remote Development extension pack for VS Code

### Setup

- open VS Code and press Ctrl+Shift+P
- execute "Remote-Containers: Clone repository in Container Volume..."
- choose https://github.com/biervertrieb/placegroups as repository and select a branch

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Best Practices

- never commit to "main" or "dev" branch
- create your own branches and commit changes
- pull from origin before creating a new branch
- write Unit Tests for every piece of code
- never copy and paste duplicate code
- write reusable code
- automate as much as possible

## Workflow for adding features and refactoring code

![](Feature.png)

- change branch to dev and pull from origin
- create a new branch and name it appropriately according to your task
- write code, test, run, verify
- you may commit as much incremental changes as you like
- publish / push your new branch to origin
- finish your task and...
- create a pull request to merge your branch into the dev branch

## Workflow for Hotfixing and trivial changes

![](Hotfies_3.drawio.png)

- change branch to main and pull from origin
- create a new branch and name it appropriately according to your task
- write code, test, run, verify
- you may commit as much incremental changes as you like
- publish / push your new branch to origin
- finish your task and...
- create TWO pull requests to merge your branch into the dev and main branch

## Unit Testing

- spawn a new Container with JDK, Maven and all files from src/
- execute command mvnw test

## Debugging

- spawn a container with JDK, Maven and all files from src/
- execute mvnw package
- spawn a container with JDK and copy the .jar from last container
- set the container environment variables to enable debug mode in the container JVM
- execute the jar and run the inside the container
- attach the Java Debugger in remote mode to JVM in the new container

## Run the app on your local machine

- spawn a container with JDK, Maven and all files from src/
- execute mvnw package
- spawn a container with JDK and copy the .jar from last container
- execute the jar and run the inside the container

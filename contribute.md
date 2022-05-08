# Contribution Guide

## Developing inside a Container

The Visual Studio Code Remote - Containers extension lets you use a Docker container as a full-featured development environment. It allows you to open any folder inside (or mounted into) a container and take advantage of Visual Studio Code's full feature set.

## Requirements

- install Docker Desktop
- install VS Code
- install Remote Development extension pack for VS Code

## Initial Setup

- go to your workspace directory
- open VS Code inside workspace directory and press Ctrl+Shift+P
- execute "Remote-Containers: Clone Repository in Container Volume..."

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

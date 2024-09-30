# Contributor Guide

Thank you for your interest in contributing to this project! Below are some guidelines and requirements to collaborate effectively.

## Prerequisites

Before making your first commit, make sure you have the following tools installed:

- Install Python 3
  ```sh
  sudo apt install python3
- Install pip
  ```sh
  curl "https://bootstrap.pypa.io/get-pip.py" >> get-pip.py && python3 get-pip.py
- Include this in your PATH
  ```sh
  export PATH=$PATH:$HOME/.local/bin
- Install Poetry
  ```sh
  pip install poetry
- At the root of the project, activate the Python environment.
  ```sh
  poetry shell
- Install the Python dependencies
  ```sh 
  poetry install
  
## Installing Git Hooks

To ensure code quality, we use Git hooks to run `flake8` before every commit. Follow these steps to install the pre-commit hook:

1. After cloning the repository, run the following script to install the Git hook:

   ```bash
   ./scripts/install-hooks.sh
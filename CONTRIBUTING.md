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

1. **Assign execution permissions to the script** (only needed once after cloning the repository):

    ```bash
    chmod +x ./scripts/install-hooks.sh
   
2. **Run the script**:
    ```bash
    ./scripts/install-hooks.sh

## Pull Request Guidelines
### Branches for Pull Requests
1. Target Branch: All PRs should be directed to the **develop** branch, where ongoing work and testing are performed before merging into the main production branch. Create a branch from **develop** in your fork. Once complete, open a PR to merge this branch into **develop**.
   
2. Updating Documentation: If your contribution affects the documentation, ensure that the relevant texts are updated to reflect the changes. This may include modifying README.md or other relevant files as needed.
3. Code Style: Please ensure your code follows the project's style guidelines.

### Submitting the Pull Request
1. Open an Issue First: Before starting work, search through existing issues or open a new one to describe the enhancement or bug. This allows the community to discuss and confirm the need for your contribution.
2. Reference the Issue in Your PR: When submitting a PR, link it to the relevant issue using keywords like "Fixes #IssueNumber" to automatically close the issue upon merging.
3. Review Process: PRs require at least two approvals from core contributors before being merged. The maintainers may request changes or additional tests if necessary.

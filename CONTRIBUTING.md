# Contributing to dbt-deltastream

Thank you for your interest in contributing to `dbt-deltastream`! We appreciate your help in making this adapter even better.

## Prerequisites

Before you begin, ensure you have the following tools installed:

*   **uv:** A fast Python package installer and resolver.
    *   Installation instructions: [https://github.com/astral-sh/uv#installation](https://github.com/astral-sh/uv#installation)
*   **changie:** A tool for managing changelogs.
    *   Installation instructions: [https://changie.dev/guide/installation/](https://changie.dev/guide/installation/)

## Development Setup

1.  **Fork the repository:**
    Since direct pushes to the main repository are not allowed, you'll need to fork it first. Go to [https://github.com/deltastreaminc/dbt-deltastream](https://github.com/deltastreaminc/dbt-deltastream) and click the "Fork" button.
2.  **Clone your fork:**
    ```bash
    git clone https://github.com/YOUR_USERNAME/dbt-deltastream.git
    cd dbt-deltastream
    ```
    Replace `YOUR_USERNAME` with your GitHub username.
3.  **Configure remotes:**
    ```bash
    git remote add upstream https://github.com/deltastreaminc/dbt-deltastream.git
    ```
4.  **Install dependencies:**
    This project uses `uv` for dependency management.
    ```bash
    uv sync
    ```
5.  **Activate the virtual environment:**
    `uv` creates a virtual environment in `.venv`. Activate it using:
    ```bash
    source .venv/bin/activate
    ```

## Running Tests

To run all tests:
```bash
uv run pytest
```

## Testing with a Sample Project (Editable Install)

After making changes to the adapter in your local fork, you might want to test these changes in a separate dbt project. You can do this by performing an "editable" installation of your local `dbt-deltastream` fork into the virtual environment of your sample dbt project.

1.  **Navigate to your sample dbt project directory** in your terminal.
2.  **Set up a virtual environment for your sample project (if you haven't already):**
    *   Create a virtual environment using `uv`:
        ```bash
        uv venv
        ```
        This will create a `.venv` directory in your sample project.
    *   Activate the virtual environment:
        ```bash
        source .venv/bin/activate
        ```
    *   If you already have a virtual environment, ensure it's activated.
3.  **Install your local `dbt-deltastream` fork in editable mode:**
    ```bash
    uv pip install -e /path/to/your/local/fork/dbt-deltastream
    ```
    Replace `/path/to/your/local/fork/dbt-deltastream` with the actual absolute path to the directory where you cloned your `dbt-deltastream` fork (the one containing the `pyproject.toml` of `dbt-deltastream`).

    For example, if your fork is in `/Users/YOUR_USERNAME/dev/dbt-deltastream`, the command would be:
    ```bash
    uv pip install -e /Users/YOUR_USERNAME/dev/dbt-deltastream
    ```

    This command creates a link to your local `dbt-deltastream` source code. Any changes you make in your `dbt-deltastream` fork will be immediately available in your sample dbt project, allowing you to test your adapter modifications by running dbt commands (e.g., `dbt run`, `dbt test`) within that sample project.

## Testing with Provided Examples

The `dbt-deltastream` repository includes several example projects in the `/examples` directory. These can be used to test your adapter changes without needing a separate sample project setup.

1.  **Ensure your main `dbt-deltastream` development virtual environment is activated:**
    If you followed the "Development Setup" section, you should have a virtual environment in `.venv` at the root of the `dbt-deltastream` repository. Make sure it's active:
    ```bash
    source .venv/bin/activate 
    ```
    (Assuming you are at the root of the `dbt-deltastream` repository).
    Because you are using your local fork directly, any changes you make to the adapter code will be used when you run these examples.

2.  **Choose an example project:**
    We recommend starting with the `hello_deltastream` example as it's the simplest:
    ```bash
    cd examples/hello_deltastream
    ```

3.  **Configure your credentials:**
    Each example project contains a `profiles.yml` file. You will need to edit this file to add your DeltaStream credentials.
    Open `examples/hello_deltastream/profiles.yml` and update the `token`, `organization_id`, and other relevant fields under the `outputs.dev` section.

    ```yaml
    # example content of profiles.yml
    hello_deltastream: # this is the profile name from dbt_project.yml
      target: dev
      outputs:
        dev:
          type: deltastream # ensure this is deltastream
          url: <your_deltastream_api_endpoint_url> # e.g., https://api.deltastream.io
          token: <your_deltastream_token>
          organization_id: <your_deltastream_organization_id>
          # Add other necessary parameters like database, schema, role if required
          database: <your_database_name>
          schema: <your_schema_name> # this is often your username or a dedicated schema
          role: <your_deltastream_role>
    ```
    **Important:** Do not commit your credentials to Git. An alternative is to create another profiles.yml file with a different name (e.g., `profiles_dev.yml`) and use that for testing. You can then specify the profiles directory when running dbt commands:

    ```bash
    DBT_PROFILES_DIR=./my_profile dbt run
    ```

5.  **Run dbt commands:**
    You can now run dbt commands within the example project directory to test your adapter:
    ```bash
    dbt run
    dbt test
    # etc.
    ```

## Making Changes

1.  **Create a new branch:**
    ```bash
    git checkout -b feature/your-feature-name
    ```
2.  **Make your changes:** Implement your feature or bug fix.
3.  **Add a changelog entry:**
    This project uses `changie` to manage the changelog. To add a new change:
    ```bash
    changie new
    ```
    `changie` will then prompt you for the following, based on the `.changie.yaml` configuration:
    *   **Author:** Your GitHub Username(s) (separated by a single space if multiple).
    *   **Issue:** The GitHub Issue Number(s) your change addresses (optional, separated by a single space if multiple).
    *   **Kind:** The type of change. You'll be able to choose from:
        *   `Breaking Changes`
        *   `Features`
        *   `Fixes`
        *   `Docs`
        *   `Under the Hood`
        *   `Dependencies`
        *   `Security`
        *   `Removed`
    *   **Body:** A concise description of your change. This will be used in the changelog.

    This command creates a new YAML file in the `.changes` directory (e.g., `.changes/my-change.yaml`). This file should be committed along with your code changes.

    Example of a `changie new` interaction and the generated file:
    ```
    $ changie new
    Author: your-github-username
    Issue: 123 456
    Kind: Fixes
    Body: Correctly handle null values in a specific macro.
    ```
    This would create a file like `.changes/fix-nulls-and-update-docs.yaml` with content similar to:
    ```yaml
    author: your-github-username
    issue: "123 456"
    kind: Fixes
    body: Correctly handle null values in a specific macro.
    time: 2025-05-21T10:00:00Z # Timestamp will be automatically generated
    ```
4.  **Write tests for your changes:**
    We highly encourage writing tests for any new features or bug fixes.
    *   Tests are located in the `tests/` directory.
    *   Add new tests to existing files if relevant, or create new test files following the existing naming conventions (e.g., `test_my_new_feature.py`).
    *   Ensure your tests cover the changes you've made and pass successfully.
5.  **Format and lint your code:**
    ```bash
    uv run ruff format
    uv run ruff check --fix
    uv run mypy
    ```
6.  **Run tests:** Ensure all tests pass, including the ones you've added.
    ```bash
    uv run pytest
    ```
7.  **Commit your changes:**
    ```bash
    git add .
    git commit -m "feat: Your descriptive commit message"
    ```
8.  **Push to your fork:**
    ```bash
    git push origin feature/your-feature-name
    ```
9.  **Open a Pull Request:** Go to the original repository on GitHub and open a Pull Request from your fork.

## Pull Request Process

1.  **Changelog Check:** Ensure your Pull Request includes a `changie` entry in the `.changes` directory.
2.  **Review:** Your Pull Request will be reviewed by the maintainers. Address any feedback provided.
3.  **Merge:** Once the Pull Request is approved and ready to merge, a maintainer will merge it.

## Release Process (for Maintainers)

1.  **Run Release PR Workflow:** After merging a Pull Request with `changie` changes, [trigger the "changie release PR" GitHub Actions workflow](https://github.com/deltastreaminc/dbt-deltastream/actions/workflows/changie_generate_release_pr.yml).
2.  **Review Generated PR:** Wait for the workflow to complete. It will generate a new Pull Request ([in the repository PRs](https://github.com/deltastreaminc/dbt-deltastream/pulls)) with the updated changelog and version bump. Review this PR.
3. **Changelog release:** The changie release will automatically update the changelog file and push the releases in Github.
4.  **Pypi Release:** [The Pypi release workflow](https://github.com/deltastreaminc/dbt-deltastream/actions/workflows/release.yml) will then be triggered. An approval will be required to proceed with publishing the new version of the adapter to the public repository.

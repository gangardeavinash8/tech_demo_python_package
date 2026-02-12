# Metadata Reader Project

This project fetches metadata from various cloud storage providers (AWS S3, Azure Blob Storage, SharePoint, Databricks) and outputs a unified JSON report.

## Prerequisites

- **Python 3.8+** installed
- **VS Code** installed
- **Git** installed

## Step 1: Clone & Open in VS Code
1.  Open VS Code.
2.  File > Open Folder... > Select `metadata_reader`.

## Step 2: Set Up Virtual Environment
Running a virtual environment (`.venv`) ensures dependencies are installed locally.

1.  Open Terminal in VS Code (`Ctrl+` ` or `Cmd+J`).
2.  Run:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Mac/Linux
    # .venv\Scripts\activate   # On Windows
    ```

## Step 3: Install Dependencies
Install the required packages:

```bash
pip install -r requirements.txt
```

**OR: Install directly via the package file (`.whl`):**
```bash
# This installs version 0.3.0
pip install dist/metadata_reader-0.3.0-py3-none-any.whl
```
*(If `requirements.txt` is missing, use `pip install .` to install from `pyproject.toml`)*

## Step 4: Configure Environment Variables
1.  Copy `.env.example` to a new file named `.env`:
    ```bash
    cp .env.example .env
    ```
2.  Open `.env` in VS Code.
3.  Fill in your credentials for AWS, Azure, SharePoint, and Databricks.

## Step 5: Run the Project

### Option A: Using VS Code Run Button (Recommended)
1.  Open `fetch_complete_report.py`.
2.  Press **F5** (or click the **Run and Debug** icon on the left sidebar).
3.  Select **"Run: Fetch All Metadata"**.

### Option B: Using Terminal
Run the script directly:
```bash
python fetch_complete_report.py
```

## Troubleshooting
- **Missing Dependencies**: Ensure your virtual environment is activated (`source .venv/bin/activate`) before running.
- **Permission Errors**: Verify your `.env` credentials have the correct permissions (e.g., `Sites.Read.All` for SharePoint).

## Packaging for Distribution

To install this package on a **different laptop** or in a **different folder**:

1.  **Build the Package**:
    ```bash
    pip install build
    python -m build
    ```
    This creates a `.whl` file in the `dist/` folder (e.g., `dist/metadata_reader-0.3.0-py3-none-any.whl`).

2.  **Install on Target Machine**:
    - Copy the `.whl` file to the other laptop.
    - Run:
      ```bash
      pip install metadata_reader-0.3.0-py3-none-any.whl
      ```

## Usage Example

I have included an example script and environment template in the `usage_example/` folder.

1.  **Create a folder** for your run (e.g., `my_run`).
2.  **Copy** `usage_example/run_metadata.py` and `usage_example/.env.template` to your folder.
3.  **Rename** `.env.template` to `.env` and fill in your credentials.
4.  **Run the script**:
    ```bash
    python run_metadata.py > output.json
    ```


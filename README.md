supply-chain-analytics-databricks

## Virtual environment

Create a project virtual environment and activate it before installing packages:

- Create: `python3 -m venv .venv`
- Upgrade pip inside venv: `.venv/bin/python -m pip install --upgrade pip`
- Activate (Linux/macOS): `source .venv/bin/activate`
- Activate (Windows PowerShell): `./.venv/Scripts/Activate.ps1`
- Install requirements: `pip install -r requirements.txt`

Recommended: add `.venv/` to `.gitignore` to avoid committing the environment.

# BackTestingEngine

Local-only Streamlit app for cleaning raw CSV files, viewing chart data, and saving trade signals into local files.

## Current Version

- App version: `2026.03.25.143118`
- GitHub repo: `rishabhhurkat-coder/BacktestingEngine`
- Installer file: `BackTestingEngine Installer.cmd`
- Launcher file: `Run BackTestingEngine.bat`

## What Is Included

- App entry point: `streamlit_app.py`
- Main application logic: `app/main.py`
- Built TV chart component: `tv_chart_component/frontend/build`
- Local workspace: `Main Folder`
- Local Streamlit config: `.streamlit/`

## Main Folder Layout

```text
Main Folder
|-- Raw Files
|-- Input Files
`-- Output Files
```

- `Raw Files`: downloaded raw CSV fragments
- `Input Files`: cleaned merged chart-ready CSVs
- `Output Files`: saved trade/signal CSVs used by the app

## Local-Only Behavior

- This copy is intended to run on your computer only.
- Streamlit is configured to bind to `localhost` only.
- All working data stays in the local `Main Folder`.

## Run Locally

Use either:

```bash
python scripts/launch_streamlit.py
```

or:

```text
Run BackTestingEngine.bat
```

Then open:

```text
http://localhost:8501
```

## Install Workflow

Use the public installer link from the latest GitHub Release.

Flow:

1. Download `BackTestingEngine Installer.cmd`
2. Run the installer
3. The installer downloads the latest package automatically
4. The app installs or updates locally
5. The desktop shortcut is refreshed

## Update Workflow

Normal user flow:

1. Open the app
2. If internet is available and a newer GitHub Release exists, the app prompts to update
3. Click `Update Now`
4. The updater downloads the latest package, keeps `Main Folder`, and reopens the app

Manual publish flow on the main machine:

```powershell
$env:GITHUB_TOKEN="YOUR_TOKEN"
python "D:\EMA 200 Trades - Local\scripts\publish_github_release.py"
```

That command:

- builds a new version
- creates the release package zip
- rebuilds the installer
- uploads both to the latest GitHub Release

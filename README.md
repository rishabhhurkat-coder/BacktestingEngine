# EMA 200 Trades - Local

Local-only Streamlit app for cleaning raw 3-minute CSV files, viewing EMA charts, and saving trade signals into local files.

## What Is Included

- Local app entry point: `streamlit_app.py`
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
- `Output Files`: saved signal CSVs used by the app

## Local-Only Behavior

- This copy is intended to run on your computer only.
- Streamlit is configured to bind to `localhost` only.
- No Git or GitHub setup is required for this folder.
- All data stays in the local `Main Folder`.

## Run

Use either:

```bash
python scripts/launch_streamlit.py
```

or:

```text
scripts\Open EMA Viewer.bat
```

Then open:

```text
http://localhost:8501
```

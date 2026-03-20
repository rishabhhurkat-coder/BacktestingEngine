import streamlit.components.v1 as components
from pathlib import Path

build_dir = Path(__file__).parent / "frontend" / "build"

tv_chart_component = components.declare_component(
    "tv_chart_component",
    path=str(build_dir)
)
import os
from pathlib import Path

import streamlit.components.v1 as components


def _resolve_build_dir() -> Path:
    candidates: list[Path] = []

    env_path = os.getenv("TV_CHART_BUILD_DIR")
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(Path(__file__).parent / "tv_chart_component" / "frontend" / "build")
    candidates.append(Path("C:/tv_chart_component/frontend/build"))

    for candidate in candidates:
        if (candidate / "index.html").exists():
            return candidate

    return candidates[-1]


build_dir = _resolve_build_dir()

tv_chart_component = components.declare_component(
    "tv_chart_component",
    path=str(build_dir),
)

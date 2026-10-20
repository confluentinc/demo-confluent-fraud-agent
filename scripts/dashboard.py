"""
Launch the Streamlit fraud dashboard (dashboard/app.py).

Runs Streamlit headless (which also skips its first-run email prompt), waits for
it to come up, then opens the browser.

Usage:
  uv run dashboard                 # http://localhost:8501
  uv run dashboard --no-browser    # don't open a browser
  uv run dashboard --port 8502
"""

import argparse
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser

from .terraform import get_project_root


def _open_when_ready(url: str) -> None:
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{url}/_stcore/health", timeout=2) as response:
                if response.status == 200:
                    webbrowser.open(url)
                    return
        except Exception:
            time.sleep(1)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Launch the fraud-detection dashboard")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--no-browser", action="store_true", help="Don't open a browser")
    args = parser.parse_args(argv)

    root = get_project_root()
    if not (root / ".env").exists():
        print("Error: .env not found. Run `uv run deploy` first.")
        sys.exit(1)

    url = f"http://localhost:{args.port}"
    print(f"Starting the dashboard at {url} (Ctrl+C to stop)")
    if not args.no_browser:
        threading.Thread(target=_open_when_ready, args=(url,), daemon=True).start()
    command = [
        sys.executable, "-m", "streamlit", "run", str(root / "dashboard" / "app.py"),
        "--server.headless", "true",
        "--server.port", str(args.port),
        "--browser.gatherUsageStats", "false",
    ]  # fmt: skip
    try:
        sys.exit(subprocess.call(command, cwd=root))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

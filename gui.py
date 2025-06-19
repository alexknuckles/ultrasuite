import os
import threading

import webview

from app import app


def start_server():
    app.run(port=5000, threaded=True)


if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_server, daemon=True)
    flask_thread.start()

    icon_path = os.path.join(os.path.dirname(__file__), "static", "app-icon.ico")

    # Older pywebview versions set the icon via start(), while newer releases
    # accept the icon parameter on create_window(). Detect support so the
    # application launches regardless of installed version.
    if "icon" in webview.create_window.__code__.co_varnames:
        webview.create_window(
            "ultrasuite",
            "http://127.0.0.1:5000",
            width=1200,
            height=800,
            icon=icon_path,
        )
        webview.start()
    else:
        webview.create_window(
            "ultrasuite",
            "http://127.0.0.1:5000",
            width=1200,
            height=800,
        )
        try:
            webview.start(icon=icon_path)
        except TypeError:
            # Fallback for very old pywebview versions without the icon argument
            webview.start()

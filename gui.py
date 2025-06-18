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
    webview.create_window(
        "ultrasuite",
        "http://127.0.0.1:5000",
        width=1200,
        height=800,
        icon=icon_path,
    )
    webview.start()

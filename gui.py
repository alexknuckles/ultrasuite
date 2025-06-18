import threading

import webview

from app import app


def start_server():
    app.run(port=5000, threaded=True)


if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_server, daemon=True)
    flask_thread.start()

    webview.create_window(app.name, "http://127.0.0.1:5000", width=1200, height=800)
    webview.start()

import threading

import webview

from app import app


def start_server():
    app.run(port=5000, threaded=True)


class Api:
    """Expose functions to JavaScript."""

    def open_pdf_window(self, url: str) -> None:
        """Open the generated PDF inside a new pywebview window."""
        webview.create_window("Report PDF", url, width=900, height=700)


if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_server, daemon=True)
    flask_thread.start()

    api = Api()
    webview.create_window(
        app.name, "http://127.0.0.1:5000", width=1200, height=800, js_api=api
    )
    webview.start()

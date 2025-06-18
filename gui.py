import threading
import urllib.request

import webview

from app import app


class Api:
    """Functions exposed to the embedded browser."""

    def save_pdf(self, url: str) -> bool:
        """Prompt the user to save the PDF from the given URL."""
        window = webview.windows[0]
        path = window.create_file_dialog(
            webview.SAVE_DIALOG,
            save_filename="report.pdf",
            file_types=("PDF Files (*.pdf)",),
        )
        if not path:
            return False

        if isinstance(path, (list, tuple)):
            path = path[0]

        full_url = url
        if url.startswith("/"):
            full_url = "http://127.0.0.1:5000" + url

        with urllib.request.urlopen(full_url) as resp, open(path, "wb") as fh:
            fh.write(resp.read())

        return True


def start_server():
    app.run(port=5000, threaded=True)


if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_server, daemon=True)
    flask_thread.start()

    api = Api()
    webview.create_window(
        app.name, "http://127.0.0.1:5000", width=1200, height=800, js_api=api
    )
    webview.start()

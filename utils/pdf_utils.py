"""PDF generation helpers."""

from io import BytesIO

from xhtml2pdf import pisa

from .helpers import fetch_resources


def create_pdf(html: str) -> BytesIO:
    """Return PDF data for the given HTML string."""
    output = BytesIO()
    pisa.CreatePDF(html, dest=output, link_callback=fetch_resources)
    output.seek(0)
    return output

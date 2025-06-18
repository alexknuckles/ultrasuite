# Agent Instructions
- Keep application files at the repository root (app.py, templates/)
- Store static assets such as images in the `static/` directory
- Run `python -m py_compile app.py database.py` before committing any changes
- Do not commit binary files like `.zip` archives or SQLite databases
- Keep the existing code style when editing code or documentation
- UI appearance may be modernized in templates and stylesheets
- Define theme colors as CSS variables (`--ultra-primary`, `--ultra-highlight`)
  in `static/styles.css` and reference them throughout the UI
- Ensure all new reports can be accessed programmatically and include
  export/settings options

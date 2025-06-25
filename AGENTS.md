🧠 Codex Agents for Report Dashboard

This file defines agent responsibilities and expectations for maintaining a clean, consistent, and extensible report dashboard codebase.

📂 StructureEnforcer

Goal:Ensure project file structure is organized and intuitive:

Keep app.py, database.py, and templates/ in the root

Store static assets (e.g., CSS, JS, images) in static/

Group non-web utilities or modules in a utils/ folder if added

Avoid committing .zip, .sqlite, or other binary data

🔍 StyleGuard

Goal:Maintain consistent coding and documentation style:

Adhere to PEP8 and project’s existing style patterns

Retain existing comment format and naming conventions

Use black/flake8 to auto-check style on save or pre-commit

Inline comments should be full-sentence where helpful

💾 BuildChecker

Goal:Ensure runtime integrity before commits:

Run python -m py_compile app.py database.py on every commit

Optional: run python -m unittest discover if unit tests are added

Block commits if compile or tests fail

🎨 UIStylist

Goal:Allow modernizing UI while keeping branding consistent:

CSS updates should use defined theme variables in static/styles.css:

--ultra-primary

--ultra-highlight

All visual changes must fallback gracefully on older browsers

Avoid inline styles unless dynamically injected via Jinja

📤 ExportAgent

Goal:Ensure all reports are accessible and usable externally:

All new reports must support:

JSON and CSV export

Basic settings/config menu

Reports should expose a consistent query interface (/api/report/{name})

🤪 ReportTester

Goal:Ensure correctness and stability of new report types:

Validate expected inputs/outputs per report

Include at least one sample query in report docstring

Add test data or mocks for CI testing if feasible

🧼 AssetCleaner

Goal:Keep the repo free of unnecessary or bloated files:

Reject binaries in commits unless explicitly required

Add .gitattributes and .gitignore to prevent accidental commits

Warn if static/ or templates/ contain unused or orphaned files

⚙️ CI Runner (planned)

Goal:Enable automated quality checks with each commit:

Compile Python files and verify CSS variables are used

Run any test_*.py or pytest modules if present

Can be triggered via GitHub Actions or local scripts/check.sh

📝 MetaWriter (optional)

Goal:Generate documentation and metadata:

Add docstrings to all modules and top-level functions

Auto-generate README.md from project metadata and usage examples

Optional: extract route map (app.url_map) and output as Markdown

🕵️‍♂️ AuditBot (optional)

Goal:Catch common issues early:

Scan for unused imports, dead routes, unreferenced templates

Detect inconsistencies between CSS variables and usage

Flag reports that don’t offer export or API endpoints


# docs/ — Developer Notes

## How the docs work

Sphinx uses `automodule` directives in `source/api/h3_toolkit.rst` to import
every Python module at build time and extract docstrings automatically.

**You do not need to edit any `.rst` file when you:**
- Rename a class or function → rebuild, Sphinx picks up the new name
- Change a docstring or add a parameter → rebuild, done

**You DO need to edit `.rst` files when:**

| Change | What to update |
|--------|----------------|
| Add a new Python module | Append an `automodule` block to `source/api/h3_toolkit.rst` |
| Add a new notebook example | Drop the `.ipynb` into `source/usage/` — picked up automatically |
| Add/replace a diagram | Put the file in `images/` and add `.. image:: ../../images/filename.svg` in the class docstring |
| Bump package version | Update `release` in `source/conf.py` **and** `version` in `pyproject.toml` |

## Build commands

```bash
# One-time build
cd docs && make html
open build/html/index.html   # macOS

# Live-reload (rebuilds on every file save)
poetry run sphinx-autobuild docs/source docs/build/html

# Clean stale artefacts (useful after renaming files)
cd docs && make clean && make html
```

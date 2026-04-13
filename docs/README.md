# docs/ — Developer Notes & Deployment Guide

## 📚 How the Documentation Works

Sphinx uses `automodule` directives in `source/api/h3_toolkit.rst` to import
every Python module at build time and extract docstrings automatically.

### When You Don't Need to Edit `.rst` Files

You do **NOT** need to edit any `.rst` file when you:
- Rename a class or function → rebuild, Sphinx picks up the new name automatically
- Change a docstring or add a parameter → rebuild, done
- Modify function signatures → autodoc regenerates documentation

### When You MUST Edit `.rst` Files

You **DO** need to edit `.rst` files when:

| Change | File to Update | Action |
|--------|---|---|
| Add a new Python module | `source/api/h3_toolkit.rst` | Append an `automodule` directive block |
| Add a new notebook example | `source/usage/` | Drop the `.ipynb` file here (auto-discovered) |
| Add/replace a diagram | `images/` | Place `.svg` file, add `.. image::` in docstring |
| Bump package version | `source/conf.py` **and** `pyproject.toml` | Update both `release` and `version` |

---

## 🏗️ Local Development Commands

### One-time Build (Preview)

```bash
cd docs && make html
open build/html/index.html   # macOS
xdg-open build/html/index.html   # Linux
```

### Live-Reload Development (Recommended)

Automatically rebuilds docs whenever you save a file:

```bash
poetry run sphinx-autobuild docs/source docs/build/html
# Visit http://127.0.0.1:8000/ in your browser
# Changes are reflected immediately
```

### Clean & Rebuild (For Fixing Build Issues)

After renaming or restructuring files:

```bash
cd docs && make clean && make html
```

---

## 📦 Release Workflow: Update Version & Deploy Docs

Complete step-by-step workflow to bump package version and publish to ReadTheDocs.

### Step 1: Bump the Package Version

#### 1a. Update pyproject.toml

```bash
# Decide your version (semantic versioning: MAJOR.MINOR.PATCH)
# Example: 0.3.12 → 0.3.13 (patch bump)
# or: 0.3.12 → 0.4.0 (minor bump)
# or: 0.3.12 → 1.0.0 (major bump)

# Edit pyproject.toml and change the version:
nano pyproject.toml
# Change this line:
# version = "0.3.12"  →  version = "0.3.13"
```

#### 1b. Update docs/source/conf.py

```bash
# Sync the version with pyproject.toml
nano docs/source/conf.py
# Change this line (around line 17):
# release = '0.3.12'  →  release = '0.3.13'
```

**⚠️ Critical:** These **two** files must have the **same version** or docs will show wrong version.

#### 1c. Verify Version Sync

```bash
# Check pyproject.toml
grep 'version = ' pyproject.toml
# Output: version = "0.3.13"

# Check conf.py
grep "release = " docs/source/conf.py
# Output: release = '0.3.13'
```

### Step 2: Commit Version Changes

```bash
git add pyproject.toml docs/source/conf.py
git commit -m "chore: bump version from 0.3.12 to 0.3.13"
```

### Step 3: Create Git Tag (Important for Release)

ReadTheDocs uses Git tags to trigger version releases:

```bash
# Create an annotated tag
git tag -a v0.3.13 -m "Release version 0.3.13"

# Verify tag was created
git tag -l | grep v0.3.13
```

### Step 4: Push Changes & Tag to GitHub

```bash
# Push commits
git push origin restructure-2  # (or your current branch)

# Push the tag — THIS TRIGGERS ReadTheDocs BUILD
git push origin v0.3.13
```

**🚀 At this point, ReadTheDocs automatically:**
1. Detects the new tag
2. Triggers a build
3. Publishes docs to https://h3-toolkit.readthedocs.io/en/v0.3.13/
4. (Optionally) marks it as the latest version

---

## 🌐 ReadTheDocs Setup & Configuration

### How ReadTheDocs is Connected

**Project URL:** https://h3-toolkit.readthedocs.io/

**Key Configuration File:** `.readthedocs.yml`

```yaml
# ReadTheDocs reads this file on each push
version: 2
build:
  os: ubuntu-22.04
  tools:
    python: "3.12"
sphinx:
  configuration: docs/source/conf.py
python:
  install:
    - requirements: docs/requirements.txt
```

**What it does:**
- Runs `pip install -r docs/requirements.txt` in a virtual environment
- Builds Sphinx docs using `docs/source/conf.py`
- Publishes to `https://h3-toolkit.readthedocs.io/`

### Viewing Different Versions

After tagging and pushing:

```
https://h3-toolkit.readthedocs.io/en/latest/      # Master branch
https://h3-toolkit.readthedocs.io/en/v0.3.13/     # Specific version (tag)
https://h3-toolkit.readthedocs.io/en/stable/      # Latest stable release
```

**To set "stable" version:** Go to ReadTheDocs admin → Versions → mark a version as "stable"

---

## 📋 Complete Release Checklist

Use this checklist for releases:

```
✅ Step 1: Update Code & Tests
  - [ ] Merge all feature branches to restructure-2
  - [ ] Run all tests: poetry run pytest tests/
  - [ ] Verify no broken tests

✅ Step 2: Update Version Numbers
  - [ ] Update version in pyproject.toml
  - [ ] Update release in docs/source/conf.py
  - [ ] Verify both files have SAME version: grep 'version\|release' pyproject.toml docs/source/conf.py
  - [ ] Commit: git commit -m "chore: bump version to X.Y.Z"

✅ Step 3: Build & Preview Docs Locally
  - [ ] poetry run sphinx-autobuild docs/source docs/build/html
  - [ ] Open http://127.0.0.1:8000/ and verify docs look correct
  - [ ] Check version displays correctly (usually in top-left or page title)

✅ Step 4: Create Git Tag & Push
  - [ ] git tag -a vX.Y.Z -m "Release version X.Y.Z"
  - [ ] git push origin restructure-2
  - [ ] git push origin vX.Y.Z  ← THIS TRIGGERS ReadTheDocs

✅ Step 5: Verify ReadTheDocs Build
  - [ ] Wait 1-2 minutes for build
  - [ ] Visit https://h3-toolkit.readthedocs.io/en/latest/
  - [ ] Verify docs are updated and version is correct
  - [ ] (Optional) Visit https://h3-toolkit.readthedocs.io/en/vX.Y.Z/ to check tag-specific version

✅ Step 6: (Optional) Mark as Stable
  - [ ] Go to https://readthedocs.org/projects/h3-toolkit/versions/
  - [ ] Find version vX.Y.Z
  - [ ] Check "Active" and optionally set as "Default" (stable)
```

---

## ⚠️ Common Mistakes & Troubleshooting

### Version Mismatch Error

**Problem:** Docs show wrong version or version string doesn't update

**Solution:**
```bash
# Ensure BOTH files match
grep 'version =' pyproject.toml
grep 'release =' docs/source/conf.py
# They should output the same version string
```

### ReadTheDocs Build Fails

**Check:**
1. View build log at https://readthedocs.org/projects/h3-toolkit/builds/
2. Common causes:
   - Missing imports in docstrings
   - Missing dependencies in `docs/requirements.txt`
   - Broken `.. automodule::` directives

**Fix:**
```bash
# Build locally first to catch errors
cd docs && make clean && make html
# Fix any errors, then re-push
```

### Docs Don't Update After Push

**Possible causes:**
1. Didn't push the **tag**: `git push origin v0.3.13`
2. Didn't wait long enough (wait 1-2 min for build)
3. ReadTheDocs build failed (check build log)

**Force rebuild:**
Visit https://readthedocs.org/projects/h3-toolkit/ → Admin → "Build" button

### New Version Not Listed on ReadTheDocs

**Solution:**
1. After pushing tag, visit https://readthedocs.org/projects/h3-toolkit/versions/
2. If version appears but is inactive: click "Activate"
3. Optionally set as "Default Version" for `https://h3-toolkit.readthedocs.io/`

---

## 🔗 Useful Links

- **Published Docs:** https://h3-toolkit.readthedocs.io/
- **ReadTheDocs Admin:** https://readthedocs.org/projects/h3-toolkit/
- **GitHub Repository:** https://github.com/CityScience-TaipeiTech/H3-ToolKits
- **Sphinx Documentation:** https://www.sphinx-doc.org/
- **ReadTheDocs Guide:** https://docs.readthedocs.io/en/stable/

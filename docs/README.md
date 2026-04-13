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

### Step 4: Build Distribution Package with Poetry

Before publishing, build the package:

```bash
# Clean any old builds
rm -rf dist/ build/ *.egg-info

# Build distribution (wheel and source)
poetry build

# Verify build succeeded
ls -lh dist/
# Output should show:
# h3_toolkit-0.3.13-py3-none-any.whl
# h3_toolkit-0.3.13.tar.gz
```

### Step 5: Publish to PyPI (Using Poetry)

#### Option A: Using Poetry's Official PyPI (Recommended)

```bash
# Poetry publishes to PyPI by default
# Make sure you've configured PyPI credentials:
# poetry config pypi-token.pypi <your-pypi-token>

# Publish the built package
poetry publish

# You'll see output like:
# Publishing h3_toolkit (0.3.13) to PyPI
# Done!
```

**To get your PyPI token:**
1. Go to https://pypi.org/account/
2. Login with your account
3. Go to "API tokens" section
4. Create a new token with "Entire account" scope
5. Store it securely (you'll need it only once)

#### Option B: Check PyPI Before Publishing (Recommended)

```bash
# Test publish to TestPyPI first (staging environment)
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config pypi-token.testpypi <your-testpypi-token>

# Publish to TestPyPI to verify
poetry publish -r testpypi

# Visit https://test.pypi.org/project/h3-toolkit/ to verify
# Then publish to real PyPI
poetry publish
```

### Step 6: Push Changes & Tag to GitHub

```bash
# Push commits
git push origin restructure-2  # (or your current branch)

# Push the tag — THIS TRIGGERS ReadTheDocs BUILD
git push origin v0.3.13
```

**🚀 What happens automatically:**

**GitHub:**
1. Tag detected
2. (Optional) Auto-create GitHub Release with changelog

**ReadTheDocs:**
1. Detects the new tag
2. Triggers a build
3. Publishes docs to https://h3-toolkit.readthedocs.io/en/v0.3.13/
4. (Optionally) marks it as the latest version

**PyPI:**
- Your package is now available at: https://pypi.org/project/h3-toolkit/0.3.13/
- Users can install: `pip install h3-toolkit==0.3.13`

---

## 🌐 ReadTheDocs Setup & Configuration

### What is ReadTheDocs?

**ReadTheDocs** is a **free, automated documentation hosting service** that:
- ✅ Automatically builds your docs whenever you push to GitHub
- ✅ Hosts multiple versions (latest, stable, and past versions)
- ✅ Provides full-text search functionality
- ✅ Handles SSL/HTTPS automatically
- ✅ Integrates with GitHub webhooks (no manual action needed)

### How h3-toolkit is Connected to ReadTheDocs

**Project URL:** https://h3-toolkit.readthedocs.io/

**ReadTheDocs Admin:** https://readthedocs.org/projects/h3-toolkit/

**Key Configuration File:** `.readthedocs.yml`

```yaml
# ReadTheDocs reads this file on each push
version: 2

# Build environment specification
build:
  os: ubuntu-22.04
  tools:
    python: "3.12"      # Python version to use

# Sphinx configuration
sphinx:
  configuration: docs/source/conf.py  # Where Sphinx looks for config

# Dependencies for building docs
python:
  install:
    - requirements: docs/requirements.txt  # What to pip install
```

### What Happens on Each Push

```
You push to GitHub
        ↓
GitHub sends webhook to ReadTheDocs
        ↓
ReadTheDocs checks .readthedocs.yml
        ↓
Spins up Ubuntu 22.04 container with Python 3.12
        ↓
Runs: pip install -r docs/requirements.txt
        ↓
Runs: sphinx-build (from docs/source/conf.py)
        ↓
Publishes HTML to https://h3-toolkit.readthedocs.io/
        ↓
(If git tag detected) Also publishes to version-specific URL
```

### Version URLs & What They Mean

```
https://h3-toolkit.readthedocs.io/en/latest/
  ↑
  Automatically built from master/main branch (or chosen default branch)
  Updates with every push to main
  Shows: "version: latest"

https://h3-toolkit.readthedocs.io/en/stable/
  ↑
  Points to the latest release version (if configured)
  Updated only when you mark a release as "stable"
  Shows: "version: stable"

https://h3-toolkit.readthedocs.io/en/v0.3.13/
  ↑
  Built from git tag v0.3.13
  Never changes - permanent snapshot of that version
  Shows: "version: v0.3.13"

https://h3-toolkit.readthedocs.io/en/v0.3.12/
  ↑
  Built from git tag v0.3.12
  Historical version - users can reference older docs
  Shows: "version: v0.3.12"
```

### Managing Versions on ReadTheDocs

**Step 1: View all versions**
```
https://readthedocs.org/projects/h3-toolkit/versions/
```

**Step 2: Activate/Deactivate versions**
- By default, ReadTheDocs activates all versions
- You can deactivate old versions if they're no longer relevant
- Check "Active" to include version in version switcher

**Step 3: Set "latest" and "stable" versions**
```
https://readthedocs.org/projects/h3-toolkit/versions/
  ↓
Find your version (e.g., v0.3.13)
  ↓
Click "Edit" 
  ↓
Check "Active"
Check "Set as Default Version" (if you want it as "stable")
```

### Viewing Documentation Build History

**Check if your push built successfully:**

```
https://readthedocs.org/projects/h3-toolkit/builds/
```

You'll see:
- Build status (✅ Success, ❌ Failed)
- Which branch/tag triggered the build
- Build duration
- Full build log (useful for debugging)

### Example: Release Workflow & Docs URLs

When you release v0.3.13:

```
Day 1: Release v0.3.13
  git push origin v0.3.13
        ↓
  ReadTheDocs builds docs
        ↓
  https://h3-toolkit.readthedocs.io/en/v0.3.13/  ← New version available
  https://h3-toolkit.readthedocs.io/en/latest/   ← Still points to main
  
Day 2: Merge v0.3.13 to main
  git push origin main
        ↓
  ReadTheDocs rebuilds "latest" docs
        ↓
  https://h3-toolkit.readthedocs.io/en/latest/   ← Updated with v0.3.13 content
  
Day 3: (Optional) Mark v0.3.13 as stable
  Visit ReadTheDocs admin → Set v0.3.13 as Default
        ↓
  https://h3-toolkit.readthedocs.io/en/stable/   ← Points to v0.3.13
```

### Why Version-Specific URLs Matter

Users can:
- Link to **specific version docs** in bug reports
  Example: "This feature works in https://h3-toolkit.readthedocs.io/en/v0.3.13/ but not in v0.3.12"
- Reference **historical documentation** they're using
  Example: User still on v0.3.10 can read docs for that specific version
- See what's **changed between versions**
  They can compare API docs across different releases

---

## 📋 Complete Release Checklist

Use this checklist for releases (both PyPI and ReadTheDocs):

```
✅ Step 1: Update Code & Tests
  - [ ] Merge all feature branches to restructure-2
  - [ ] Run all tests: poetry run pytest tests/
  - [ ] Verify no broken tests

✅ Step 2: Update Version Numbers
  - [ ] Update version in pyproject.toml
  - [ ] Update release in docs/source/conf.py
  - [ ] Verify both files have SAME version
      grep 'version\|release' pyproject.toml docs/source/conf.py
  - [ ] Commit: git commit -m "chore: bump version to X.Y.Z"

✅ Step 3: Build & Preview Docs Locally
  - [ ] poetry run sphinx-autobuild docs/source docs/build/html
  - [ ] Open http://127.0.0.1:8000/ and verify docs look correct
  - [ ] Check version displays correctly (usually in top-left)

✅ Step 4: Build Package Distribution
  - [ ] rm -rf dist/ build/ *.egg-info
  - [ ] poetry build
  - [ ] Verify: ls -lh dist/
  - [ ] Should show: h3_toolkit-X.Y.Z-py3-none-any.whl and .tar.gz

✅ Step 5: Publish to PyPI
  - [ ] poetry publish
  - [ ] Wait for confirmation message
  - [ ] Visit https://pypi.org/project/h3-toolkit/X.Y.Z/
  - [ ] Verify package appears and looks correct

✅ Step 6: Create Git Tag
  - [ ] git tag -a vX.Y.Z -m "Release version X.Y.Z"
  - [ ] Verify tag: git tag -l | grep vX.Y.Z

✅ Step 7: Push Commits & Tag to GitHub
  - [ ] git push origin restructure-2
  - [ ] git push origin vX.Y.Z  ← TRIGGERS ReadTheDocs automatically

✅ Step 8: Verify ReadTheDocs Build (1-2 min wait)
  - [ ] Visit https://h3-toolkit.readthedocs.io/en/latest/
  - [ ] Verify docs are updated and version is correct
  - [ ] Visit https://h3-toolkit.readthedocs.io/en/vX.Y.Z/
  - [ ] Verify tag-specific version docs are built

✅ Step 9: (Optional) Mark as Stable Release
  - [ ] Go to https://readthedocs.org/projects/h3-toolkit/versions/
  - [ ] Find version vX.Y.Z
  - [ ] Check "Active" and set as "Default" if stable

✅ Step 10: Verify Installation Works
  - [ ] pip install h3-toolkit==X.Y.Z
  - [ ] python -c "import h3_toolkit; print(h3_toolkit.__version__)"
  - [ ] Verify correct version displays
```

---

## 📦 PyPI Publishing Guide

### What is PyPI?

**PyPI** (Python Package Index) is the **official Python package repository** where:
- ✅ Users install packages with `pip install h3-toolkit`
- ✅ All public Python packages are indexed
- ✅ Versioning is managed (v0.3.12, v0.3.13, etc.)
- ✅ Package metadata is stored (dependencies, author, license, etc.)

**Your Package:** https://pypi.org/project/h3-toolkit/

### How Poetry Publishes to PyPI

Poetry streamlines the entire PyPI publishing process:

```
poetry build         ← Creates distribution files (.whl and .tar.gz)
    ↓
poetry publish       ← Uploads to PyPI using your token
    ↓
Package appears on PyPI & available via pip
```

### Setting Up PyPI Publishing

**One-time setup (per machine):**

```bash
# 1. Get your PyPI token
#    Visit: https://pypi.org/account/
#    Go to: API tokens → Create token
#    Scope: "Entire account" (or "h3-toolkit" project-specific)
#    Copy the token (looks like: pypi-AgEIcHlwaS5vcmc...)

# 2. Configure Poetry with your token
poetry config pypi-token.pypi pypi-AgEIcHlwaS5vcmc...

# Verify configuration (token is hidden for security)
poetry config pypi-token.pypi
# Output: pypi-AgEIcHlwaS5vcmc...
```

### Publishing a New Version

```bash
# Prerequisites:
# ✅ Version updated in pyproject.toml (e.g., 0.3.12 → 0.3.13)
# ✅ Version updated in docs/source/conf.py
# ✅ Changes committed to git

# Step 1: Build the distribution package
poetry build
# Creates:
#   dist/h3_toolkit-0.3.13-py3-none-any.whl  (binary wheel)
#   dist/h3_toolkit-0.3.13.tar.gz             (source distribution)

# Step 2: Verify the build
ls -lh dist/h3_toolkit-0.3.13*
# Check file sizes seem reasonable (should be similar)

# Step 3: Publish to PyPI
poetry publish

# Expected output:
# Publishing h3_toolkit (0.3.13) to PyPI
# Done!

# Step 4: Verify on PyPI (wait ~30 seconds for indexing)
# Visit: https://pypi.org/project/h3-toolkit/0.3.13/
# Check: Package info, download links, metadata
```

### Testing Installation (After Publishing)

```bash
# In a NEW virtual environment (to test fresh install):
python -m venv test_env
source test_env/bin/activate  # On Windows: test_env\Scripts\activate

# Install your just-published version
pip install h3-toolkit==0.3.13

# Test import
python -c "import h3_toolkit; print(h3_toolkit.__version__)"
# Output: 0.3.13

# Deactivate test environment
deactivate
```

### Understanding Distribution Files

When you run `poetry build`, two files are created:

```
h3_toolkit-0.3.13-py3-none-any.whl
  ↑ Wheel (compiled binary)
  ├─ Faster to install (no compilation needed)
  ├─ Preferred by pip
  ├─ Format: setuptools built package
  └─ Used 90% of the time

h3_toolkit-0.3.13.tar.gz
  ↑ Source distribution (tarball)
  ├─ Contains original source code
  ├─ Contains metadata and build scripts
  ├─ Used if user has specific build requirements
  ├─ Slower (requires compilation on install)
  └─ Used 10% of the time
```

PyPI uploads **both**. Users get whichever is most appropriate.

### Managing Package Versions on PyPI

**View all released versions:**
```
https://pypi.org/project/h3-toolkit/#history
```

**Yanking (hiding) a bad release:**
```
https://pypi.org/project/h3-toolkit/
  ↓
Go to version history
  ↓
Find problematic version (e.g., 0.3.13)
  ↓
Click "Yank" to hide it from pip install
  ↓
Users on 0.3.13 can still use it, but:
  - New users won't install it by default
  - pip install h3-toolkit will skip it
```

### Continuous Publishing Workflow

**Typical release cycle:**

```
Monday: Finish feature, test locally
Tuesday: Update version (0.3.12 → 0.3.13)
         poetry build && poetry publish  ← On PyPI
         git tag -a v0.3.13
         git push origin v0.3.13  ← Triggers ReadTheDocs
Wednesday: Docs built automatically
Thursday: Announce release to users
         Users can: pip install h3-toolkit==0.3.13
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
   - Python version mismatch

**Fix:**
```bash
# Build locally first to catch errors
cd docs && make clean && make html
# Fix any errors, then re-push
```

**If still failing:**
```bash
# Check if your local Python version matches ReadTheDocs (3.12)
python --version
# Should be 3.12.x

# Clear Poetry cache and rebuild
poetry lock --no-update
poetry install
```

### PyPI Publishing Errors

**Error: "Invalid distribution"**
```bash
# Solution: Rebuild with clean dist folder
rm -rf dist/ build/ *.egg-info
poetry build
poetry publish
```

**Error: "Could not find a version that satisfies the requirement"**
```bash
# Solution: Version probably not indexed yet. Wait 1-2 minutes.
# Then try: pip install h3-toolkit --no-cache-dir
```

**Error: "401 Unauthorized"**
```bash
# Solution: PyPI token is invalid or expired
# 1. Go to https://pypi.org/account/
# 2. Revoke old token
# 3. Create new token
# 4. Update Poetry: poetry config pypi-token.pypi <new-token>
```

**Error: "package version already exists"**
```bash
# Solution: You're trying to publish the same version twice
# 1. Update version in pyproject.toml (e.g., 0.3.13 → 0.3.14)
# 2. Update version in docs/source/conf.py
# 3. poetry build && poetry publish
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

---

## ⚡ 快速參考卡：完整發佈流程（10 分鐘）

### 發佈新版本到 PyPI + ReadTheDocs

```bash
# 假設版本升級：0.3.12 → 0.3.13

# Step 1: 更新版本號
nano pyproject.toml        # version = "0.3.13"
nano docs/source/conf.py   # release = '0.3.13'
git add pyproject.toml docs/source/conf.py
git commit -m "chore: bump version to 0.3.13"

# Step 2: 打包
rm -rf dist/ build/
poetry build
ls dist/  # 驗證看到 .whl 和 .tar.gz

# Step 3: 發佈到 PyPI（全世界都能 pip install）
poetry publish
# 用戶現在可以: pip install h3-toolkit==0.3.13

# Step 4: 建立 Git Tag 並推送（觸發 ReadTheDocs）
git tag -a v0.3.13 -m "Release version 0.3.13"
git push origin restructure-2
git push origin v0.3.13    # ← 這行觸發 ReadTheDocs 自動構建

# Step 5: 驗證
# 1-2 分鐘後檢查：
# PyPI: https://pypi.org/project/h3-toolkit/0.3.13/
# ReadTheDocs: https://h3-toolkit.readthedocs.io/en/latest/
```

### 三個重要檔案必須同步

```bash
# 驗證版本一致性（都應該是 0.3.13）
grep 'version = ' pyproject.toml          # version = "0.3.13"
grep 'release = ' docs/source/conf.py     # release = '0.3.13'
```

### 發佈後驗證清單

```
✅ PyPI 上有新版本
   https://pypi.org/project/h3-toolkit/0.3.13/

✅ pip install 可以安裝
   pip install h3-toolkit==0.3.13

✅ ReadTheDocs 自動構建
   https://h3-toolkit.readthedocs.io/en/latest/

✅ 版本標籤可見
   https://h3-toolkit.readthedocs.io/en/v0.3.13/

✅ 版本切換器可見
   訪問文檔 → 右下角 "v0.3.13" 下拉選單
```

---

## 📊 PyPI vs ReadTheDocs 對比

| 功能 | PyPI | ReadTheDocs |
|------|------|---|
| **用途** | 代碼分發 | 文檔託管 |
| **命令** | `poetry publish` | 自動 (push 時) |
| **用戶操作** | `pip install h3-toolkit` | 訪問網站或搜索引擎 |
| **觸發條件** | 手動執行 | Git push/tag 自動觸發 |
| **版本管理** | 需同步 pyproject.toml | 需同步 conf.py |
| **可用方式** | 從任何地方 pip install | 從瀏覽器查看 |
| **重要性** | 高（用戶用這個安裝包） | 中（參考文檔） |

---

## 🔗 Useful Links

- **Published Docs:** https://h3-toolkit.readthedocs.io/
- **ReadTheDocs Admin:** https://readthedocs.org/projects/h3-toolkit/
- **PyPI Package:** https://pypi.org/project/h3-toolkit/
- **GitHub Repository:** https://github.com/CityScience-TaipeiTech/H3-ToolKits
- **Sphinx Documentation:** https://www.sphinx-doc.org/
- **ReadTheDocs Guide:** https://docs.readthedocs.io/en/stable/
- **PyPI Help:** https://packaging.python.org/tutorials/packaging-projects/

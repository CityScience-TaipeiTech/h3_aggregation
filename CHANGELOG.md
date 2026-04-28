## [unreleased]

### 🚀 Features

- Update ruff lint

### 📚 Documentation

- Update README.md and CONTRIBUTING.md
- Add .github/ for people to contribute

### ⚙️ Miscellaneous Tasks

- Remove E501
## [0.3.15] - 2026-04-14

### ⚙️ Miscellaneous Tasks

- Bump version from 0.3.14 to 0.3.15
## [0.3.14] - 2026-04-13

### 🚀 Features

- Add timerange parameter support to HBase client
- Implement map boundary and view state calculation
- Implement color setting with mapclassify and matplotlib
- Implement show_h3 core visualization function
- Add show() method to H3Toolkit class

### 🐛 Bug Fixes

- Singleton issue, pass all test, and change aggregation method name from splitEqually to EqualSplit, SumUp to Sum
- Fill null to 0 problem, make it explicitly decided by the user
- Respect explicit source_resolution parameter in process_from_h3
- Resolve asyncio event loop conflicts in Jupyter/async environments
- Lazy initialize asyncio.Semaphore to bind to correct event loop
- Handle multi-timestamp data in HBase fetch
- Correct data coverage calculation in test_data_coverage
- H3Toolkit.show() now returns Deck object for Jupyter display
- Visualization issues

### 💼 Other

- New h3 function
- Conditionally select the aggregation function and scale up by getting data from hbase
- Folder structure
- Folder structure
- Add semaphone when using async
- Change functional to OOP
- Geom_to_wkb failed to be converted
- Core selected_cols concat failed
- Add docs file and readthedoc setting file
- From raster
- First Version Check
- First version documentation done
- Restructure Done
- Version tag to 0.3.1
- Sphinx_rtd_theme version to 3.0.0rc1
- Add new feature for set rowkeys directly by fetch_from_hbase
- Add custom name for process_from_raster function
- Fetch data from hbase bug
- Self.result can't be None
- The potential string return
- Remove the geo parameter from process_from_raster
- Problems of not reading docstring
- Problems of not reading docstring 2
- Problems of not reading docstring 3
- Problems of not reading docstring 4
- Picture of aggregation functions
- Apply function for modifing self.result
- Aiohttp version to 3.10.2
- Don't send the empty rowkeys
- 03_hbase_data
- Add visualization optional dependency group

### 🚜 Refactor

- Use test_resolution_10.csv for HBase test row keys
- Remove redundant test_hbase_quick.py script

### 📚 Documentation

- Add complete version bump & ReadTheDocs deployment guide
- Add comprehensive PyPI and ReadTheDocs guide
- Clarify HBase API token registration process
- Consolidate HBase testing docs into tests/README.md
- Add visualization integration design spec
- Update visualization design with classifier, k, and cmap parameters
- Add visualization integration implementation plan
- Add Concepts section with method chaining design pattern
- Fix navigation sidebar ordering and toctree structure
- Fix sidebar navigation for notebook pages
- Fix API documentation sidebar navigation
- Remove duplicate toctree entries in usage examples
- Fix API Documentation sidebar navigation
- Reorder documentation sections to Concepts -> API -> Usage
- Maintain consistent sidebar ordering across all pages
- Simplify sidebar structure and add RTD theme options
- Remove numbered option from toctree

### 🧪 Testing

- Add visualization module test framework
- Implement dependency check tests
- Verify all visualization tests pass with no regressions

### ⚙️ Miscellaneous Tasks

- Add comment for pivot
- Bump version from 0.3.12 to 0.3.13

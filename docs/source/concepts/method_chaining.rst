Method Chaining & Return Values
================================

H3Toolkit is designed around the **method chaining pattern** for a fluent, readable API. Understanding the return values of different methods is key to using the toolkit effectively.

Design Philosophy
-----------------

The toolkit follows this principle:

- **Most methods return `H3Toolkit`** (self) to enable method chaining
- **`get_result()`** returns the processed DataFrame/GeoDataFrame
- **`show()`** intelligently returns based on usage:
  - Returns `pydeck.Deck` when displaying maps in Jupyter
  - Returns `H3Toolkit` when saving to HTML files

This design allows you to chain operations naturally while still being able to extract results or visualize data.

Method Return Values
--------------------

Data Processing & Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These methods perform operations on the H3Toolkit's internal data and return `self`:

- **`set_aggregation_strategy()`** – Configure aggregation functions for data processing
- **`set_hbase_client()`** – Configure HBase connection parameters
- **`process_from_vector()`** – Convert vector geometry to H3 indexes
- **`process_from_raster()`** – Convert raster data to H3 indexes
- **`process_from_h3()`** – Aggregate or rescale H3 data to different resolutions
- **`fetch_from_hbase()`** – Fetch H3 data from HBase storage
- **`send_to_hbase()`** – Write H3 data to HBase storage
- **`apply()`** – Apply custom Polars transformations to data

**Return type:** `H3Toolkit` (enables chaining)

Result Extraction
~~~~~~~~~~~~~~~~~

- **`get_result(return_geometry=False)`** – Extract the final processed data

  - **Return type:** `polars.DataFrame` or `geopandas.GeoDataFrame`
  - Use this at the end of a chain to get your actual data

Visualization
~~~~~~~~~~~~~

- **`show(target_col, classifier='NaturalBreaks', k=5, cmap='Oranges', save_to=None, **pydeck_kwargs)`**

  - **When `save_to=None`** (display map):
    - **Return type:** `pydeck.Deck`
    - Automatically renders as interactive map in Jupyter notebooks

  - **When `save_to='file.html'`** (save map):
    - **Return type:** `H3Toolkit` (self)
    - Enables continued method chaining after saving

Usage Patterns
--------------

Complete Chaining Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~

Process data through multiple steps, then extract results:

.. code-block:: python

    from h3_toolkit import H3Toolkit
    from h3_toolkit.aggregation import SumUp

    result_df = (
        toolkit
        .set_aggregation_strategy({'population': SumUp()})
        .process_from_vector(geo_df, resolution=12)
        .process_from_h3(target_resolution=10)
        .get_result()  # Extract DataFrame at the end
    )

Visualization with Chaining
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Chain operations, then visualize or save the map:

.. code-block:: python

    # Display interactive map in Jupyter
    toolkit.show('population', classifier='Quantiles', k=5)

    # Or save map to HTML file and continue chaining
    result = (
        toolkit
        .process_from_vector(geo_df)
        .show('population', save_to='map.html')  # Returns toolkit
        .get_result()  # Can still continue chaining
    )

Complete Workflow
~~~~~~~~~~~~~~~~~

Chain everything from data loading to visualization:

.. code-block:: python

    result = (
        toolkit
        .set_aggregation_strategy({'p_cnt': SumUp()})
        .process_from_vector(geo_df, resolution=12)
        .process_from_h3(target_resolution=10)
        .show('p_cnt', cmap='viridis', save_to='output.html')  # Returns toolkit
        .apply(lambda df: df.filter(pl.col('p_cnt') > 1000))  # Filter data
        .get_result()  # Get final filtered DataFrame
    )

Return Value Summary Table
---------------------------

.. list-table:: H3Toolkit Method Return Values
   :header-rows: 1
   :widths: 30 20 50

   * - Method Group
     - Return Type
     - Purpose

   * - **Data Processing**
     - `H3Toolkit`
     - Enable method chaining

   * - `process_from_vector()`
     - `H3Toolkit`
     - Convert vector → H3

   * - `process_from_raster()`
     - `H3Toolkit`
     - Convert raster → H3

   * - `process_from_h3()`
     - `H3Toolkit`
     - Aggregate/rescale H3 data

   * - **Data Source**
     - `H3Toolkit`
     - Enable method chaining

   * - `fetch_from_hbase()`
     - `H3Toolkit`
     - Load data from HBase

   * - `send_to_hbase()`
     - `H3Toolkit`
     - Write data to HBase

   * - **Configuration**
     - `H3Toolkit`
     - Enable method chaining

   * - `set_aggregation_strategy()`
     - `H3Toolkit`
     - Configure aggregations

   * - `set_hbase_client()`
     - `H3Toolkit`
     - Configure HBase client

   * - `apply()`
     - `H3Toolkit`
     - Apply custom transformations

   * - **Result Extraction**
     - `DataFrame`
     - Get final data

   * - `get_result()`
     - `DataFrame` or `GeoDataFrame`
     - Extract processed data

   * - **Visualization**
     - `Deck` or `H3Toolkit`
     - Display or save map

   * - `show(save_to=None)`
     - `pydeck.Deck`
     - Display map in Jupyter

   * - `show(save_to='file')`
     - `H3Toolkit`
     - Save map + enable chaining

Benefits of This Design
-----------------------

1. **Fluent API** – Chain operations naturally, left-to-right
2. **Readable Code** – Operations read like a pipeline
3. **Flexible Extraction** – Extract data at any point in the chain
4. **Smart Visualization** – Return values adapt to your use case

.. code-block:: python

    # Easy to read and understand
    result = (
        toolkit
        .process_from_vector(geo_df)
        .show('value')  # Displays map immediately
    )

    # Same code, different intent – save instead
    result = (
        toolkit
        .process_from_vector(geo_df)
        .show('value', save_to='map.html')  # Saves to file
        .get_result()  # Get data
    )

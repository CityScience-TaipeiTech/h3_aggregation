# H3-Toolkit 可視化模組整合設計

**日期：** 2026-04-13  
**作者：** Huang SyuanBo  
**版本：** 1.0

---

## 1. 概述

將 H3 hexagon 數據的可視化功能集成到 h3-toolkit 中，支持使用者在 Jupyter Notebook 中直接展示地理數據，或導出為 HTML 文件。通過可選依賴機制，使未需要可視化功能的用戶無需安裝額外的庫。

**目標：**
- 提供簡潔的 API：`toolkit.show(target_col)`
- 自動計算地圖邊界和初始視圖狀態
- 支持 Jupyter 展示和 HTML 保存
- 雙層依賴檢查（import 時 + 調用時）

---

## 2. 需求規格

### 2.1 功能需求

| 需求 | 說明 |
|------|------|
| **Jupyter 直接展示** | 在 Notebook 中調用 `toolkit.show()` 自動渲染地圖 |
| **HTML 導出** | 支持 `toolkit.show(save_to='map.html')` 保存地圖 |
| **自動邊界計算** | 根據 hexagon 數據自動計算地圖中心和縮放級別 |
| **多種分類方法** | 支援 mapclassify 的各種分類器（NaturalBreaks、Quantiles、JenksNaturalBreaks 等） |
| **可自訂分類數** | 允許通過 `k` 參數設定分類數量（預設 5） |
| **顏色映射選擇** | 支援 matplotlib 所有顏色映射（Oranges、Viridis、RdYlGn 等） |
| **自訂參數** | 允許傳遞 pydeck 參數自訂地圖樣式、傾斜角、方向等 |
| **獨立函數** | 同時提供 `h3_toolkit.visualization.show_h3()` 獨立函數 |
| **依賴檢查** | Import 時和調用時都檢查依賴，給出明確安裝指令 |

### 2.2 安裝方式

```bash
# 包含可視化
pip install h3-toolkit[visualization]

# 不包含可視化（預設）
pip install h3-toolkit
```

---

## 3. 架構設計

### 3.1 文件結構

```
h3_toolkit/
├── __init__.py
├── core.py              （修改：新增 show() 方法）
├── hbase.py
├── aggregation.py
├── utils.py
├── exceptions.py
└── visualization.py     （新增）

pyproject.toml           （修改：新增 [tool.poetry.extras]）
```

### 3.2 依賴管理

**pyproject.toml 修改：**

```toml
[tool.poetry.dependencies]
python = "^3.10.12"
polars = "1.2.1"
geopandas = "^1.0.1"
h3ronpy = "^0.21.0"
aiohttp = ">=3.11.14, <4.0.0"
tqdm = "^4.66.5"

[tool.poetry.extras]
visualization = ["pydeck", "mapclassify"]
```

**可視化依賴：**
- `pydeck`：H3 hexagon 圖層渲染
- `mapclassify`：NaturalBreaks 數據分類

---

## 4. API 設計

### 4.1 H3Toolkit.show() 方法

```python
def show(
    self,
    target_col: str,
    h3_col: str = 'hex_id',
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
    save_to: str | None = None,
    **pydeck_kwargs
) -> 'H3Toolkit':
    """
    可視化 H3 hexagon 數據層。
    
    自動根據數據範圍計算地圖邊界和初始視圖狀態。支援多種分類方法和顏色映射。
    
    Args:
        target_col (str):
            要著色的數據列名。
        h3_col (str, optional):
            H3 hexagon ID 列名。預設 'hex_id'。
        classifier (str, optional):
            mapclassify 分類方法名稱。預設 'NaturalBreaks'。
            可用選項：'NaturalBreaks', 'EqualInterval', 'Quantiles', 
            'StdMean', 'JenksNaturalBreaks', 'FisherJenks' 等。
            詳見：https://pysal.org/mapclassify/api.html
        k (int, optional):
            分類數量。預設 5。必須 >= 2。
        cmap (str, optional):
            matplotlib 顏色映射名稱。預設 'Oranges'。
            可用顏色映射：https://matplotlib.org/stable/users/explain/colors/colormaps.html
            常用例子：'Viridis', 'Plasma', 'Blues', 'Reds', 'RdYlGn' 等。
        save_to (str | None, optional):
            HTML 輸出路徑。若為 None，則在 Jupyter 中直接展示。
            預設 None。
        **pydeck_kwargs:
            傳給 pdk.Deck() 的額外參數，如 map_style、pitch 等。
    
    Returns:
        H3Toolkit: 返回自身，支援鏈式調用。
    
    Raises:
        ValueError: 若 self.result 為空（未經過數據處理）或 k < 2。
        ImportError: 若缺少可視化依賴（pydeck, mapclassify）。
        ValueError: 若分類器名稱無效或顏色映射不存在。
    
    Examples:
        >>> toolkit = H3Toolkit()
        >>> toolkit.process_from_vector(geo_df)
        >>> toolkit.fetch_from_hbase(...)
        
        >>> # 使用預設 NaturalBreaks (k=5) 和 Oranges 顏色
        >>> toolkit.show('population_count')
        
        >>> # 自訂分類方法和分類數
        >>> toolkit.show(
        ...     'population_count',
        ...     classifier='Quantiles',
        ...     k=7
        ... )
        
        >>> # 自訂顏色映射
        >>> toolkit.show(
        ...     'population_count',
        ...     cmap='RdYlGn',
        ...     k=6
        ... )
        
        >>> # 保存為 HTML 並自訂地圖樣式
        >>> toolkit.show(
        ...     'population_count',
        ...     classifier='JenksNaturalBreaks',
        ...     cmap='Viridis',
        ...     k=5,
        ...     save_to='map.html',
        ...     map_style='mapbox://styles/mapbox/satellite-v9',
        ...     pitch=45
        ... )
    """
```

### 4.2 visualization 模組的公開 API

```python
def show_h3(
    data: pl.DataFrame,
    target_col: str,
    h3_col: str = 'hex_id',
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
    save_to: str | None = None,
    **pydeck_kwargs
) -> pdk.Deck:
    """
    獨立的可視化函數，用於渲染 H3 hexagon 層。
    
    Args:
        data (pl.DataFrame): 包含 H3 hexagon ID 和數值數據的 Polars DataFrame。
        target_col (str): 要著色的列名。
        h3_col (str, optional): hexagon ID 列名。預設 'hex_id'。
        classifier (str, optional): mapclassify 分類方法。預設 'NaturalBreaks'。
        k (int, optional): 分類數量。預設 5。
        cmap (str, optional): matplotlib 顏色映射。預設 'Oranges'。
        save_to (str | None, optional): 保存 HTML 的路徑。若為 None 則返回 pdk.Deck。
        **pydeck_kwargs: 傳給 pdk.Deck 的參數。
    
    Returns:
        pdk.Deck: pydeck Deck 對象，可直接在 Jupyter 中展示或保存。
    """
```

---

## 5. 實現細節

### 5.1 visualization.py 的結構

```python
# 1. 依賴檢查函數
def _check_dependencies() -> None:
    """在 import 時檢查可視化依賴。"""
    # 檢查 pydeck, mapclassify 是否已安裝
    # 若缺少，給出明確的安裝指令

# 2. 邊界計算
def _calculate_initial_view_state(hex_ids: list[str]) -> dict:
    """
    根據 hex_ids 自動計算地圖邊界和初始視圖狀態。
    
    返回 pydeck.ViewState 所需的字典：
    {
        "longitude": float,  # 地圖中心經度
        "latitude": float,   # 地圖中心緯度
        "zoom": float,       # 縮放級別 (0-20)
        "pitch": float,      # 傾斜角度 (0-60)
        "bearing": float     # 方向角 (0-359)
    }
    """

# 3. 顏色設置
def _set_color(
    data: pl.DataFrame,
    target_col: str,
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
) -> pl.DataFrame:
    """
    使用指定的 mapclassify 分類方法和 matplotlib 顏色映射對數據著色。
    
    Args:
        data: 輸入 DataFrame
        target_col: 要分類的列名
        classifier: mapclassify 分類器名稱（如 'NaturalBreaks', 'Quantiles'）
        k: 分類數量
        cmap: matplotlib 顏色映射名稱
    
    Returns:
        添加 'color' 列的 DataFrame，值為 RGBA 四元組 [R, G, B, A]。
    """

# 4. 核心可視化函數
def show_h3(
    data: pl.DataFrame,
    target_col: str,
    h3_col: str = 'hex_id',
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
    save_to: str | None = None,
    **pydeck_kwargs
) -> pdk.Deck:
    """
    渲染 H3 hexagon 層並返回或保存 pdk.Deck。
    
    實現流程：
    1. 檢查依賴
    2. 驗證參數（classifier 和 cmap 有效性，k >= 2）
    3. 調用 _set_color() 進行分類著色
    4. 計算地圖邊界
    5. 創建 H3HexagonLayer
    6. 創建 pdk.Deck 對象
    7. 若 save_to 非空，保存 HTML；否則返回 Deck 對象
    """
```

### 5.2 雙層依賴檢查

**Layer 1：Import 時檢查**（visualization.py 頂部）
```python
def _check_dependencies():
    missing = []
    try:
        import pydeck
    except ImportError:
        missing.append('pydeck')
    
    try:
        import mapclassify
    except ImportError:
        missing.append('mapclassify')
    
    if missing:
        raise ImportError(
            f"Missing visualization dependencies: {', '.join(missing)}. "
            f"Install with: pip install h3-toolkit[visualization]"
        )

_check_dependencies()  # 在模組頂層執行
```

**Layer 2：調用時檢查**（show_h3() 函數內部）
```python
def show_h3(...):
    try:
        _check_dependencies()
    except ImportError as e:
        raise ImportError(
            f"Cannot visualize data: {e}\n"
            f"Missing optional dependencies. "
            f"Install with: pip install h3-toolkit[visualization]"
        ) from e
    # 繼續執行...
```

### 5.3 邊界計算算法

使用 h3ronpy 的幾何功能獲取每個 hexagon 的邊界：

```python
def _calculate_initial_view_state(hex_ids: list[str]) -> dict:
    import h3ronpy.polars
    
    # 1. 獲取所有 hexagon 的邊界
    boundaries = []
    for hex_id in hex_ids:
        boundary = h3.cell_to_boundary(hex_id)  # 返回 [(lat, lon), ...]
        boundaries.extend(boundary)
    
    # 2. 計算邊界框
    lats = [point[0] for point in boundaries]
    lons = [point[1] for point in boundaries]
    
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)
    
    # 3. 計算中心點
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2
    
    # 4. 根據邊界計算縮放級別
    # 使用標準公式：zoom = log2(360 * 2^8 / (max_lon - min_lon))
    # 加上安全邊距（0.1 倍）
    lat_diff = max_lat - min_lat
    lon_diff = max_lon - min_lon
    max_diff = max(lat_diff, lon_diff) * 1.1
    
    zoom = 8 - math.log2(max_diff / 360)
    zoom = max(0, min(20, zoom))  # 限制在 0-20 範圍
    
    return {
        "longitude": center_lon,
        "latitude": center_lat,
        "zoom": zoom,
        "pitch": 0,
        "bearing": 0
    }
```

---

## 6. 使用示例

### 6.1 基本使用（Jupyter 直接展示）

```python
from h3_toolkit import H3Toolkit

toolkit = H3Toolkit()
toolkit.process_from_vector(geo_df, resolution=10)
toolkit.fetch_from_hbase('table', 'family', ['column'])

# 直接在 Jupyter 展示（使用預設 NaturalBreaks, k=5, Oranges 顏色）
toolkit.show('column')
```

### 6.2 自訂分類方法和分類數

```python
# 使用 Quantiles（四分位）分類，分為 4 類
toolkit.show(
    'population_count',
    classifier='Quantiles',
    k=4
)

# 使用 JenksNaturalBreaks，分為 7 類
toolkit.show(
    'population_count',
    classifier='JenksNaturalBreaks',
    k=7
)
```

### 6.3 自訂顏色映射

```python
# 使用 Viridis（彩虹色）
toolkit.show(
    'population_count',
    cmap='Viridis',
    k=6
)

# 使用 RdYlGn（紅-黃-綠）
toolkit.show(
    'population_count',
    cmap='RdYlGn',
    k=5
)

# 使用 Blues（漸層藍色）
toolkit.show(
    'temperature',
    cmap='Blues',
    k=8
)
```

### 6.4 保存為 HTML

```python
toolkit.show(
    'population_count',
    save_to='output/map.html'
)
```

### 6.5 組合自訂參數

```python
# 自訂分類、顏色、地圖樣式並保存
toolkit.show(
    'population_count',
    classifier='EqualInterval',
    k=6,
    cmap='RdYlGn',
    save_to='map.html',
    map_style='mapbox://styles/mapbox/satellite-v9',
    pitch=45,
    bearing=90
)
```

### 6.6 獨立使用可視化函數

```python
from h3_toolkit.visualization import show_h3

# 在 Jupyter 展示
deck = show_h3(
    data=df,
    target_col='value',
    classifier='Quantiles',
    k=5,
    cmap='Viridis'
)

# 保存為 HTML
show_h3(
    data=df,
    target_col='value',
    classifier='JenksNaturalBreaks',
    cmap='RdYlGn',
    save_to='map.html'
)
```

---

## 7. 錯誤處理

### 7.1 缺少依賴時

```
ImportError: Missing visualization dependencies: pydeck, mapclassify. 
Install with: pip install h3-toolkit[visualization]
```

### 7.2 無效的數據

```
ValueError: Column 'population_count' not found in data.
```

```
ValueError: No data to visualize. Process data first.
```

---

## 8. 測試策略

### 8.1 單元測試

- `test_check_dependencies()`: 測試依賴檢查機制
- `test_set_color()`: 測試顏色設置邏輯
- `test_calculate_initial_view_state()`: 測試邊界計算
- `test_show_h3_returns_deck()`: 測試返回 pdk.Deck 對象

### 8.2 集成測試

- `test_toolkit_show_jupyter()`: 在 Jupyter 環境中測試
- `test_toolkit_show_save_html()`: 測試 HTML 保存
- `test_toolkit_show_with_custom_params()`: 測試自訂參數

### 8.3 跳過依賴缺失時的測試

使用 `pytest.mark.skipif` 在缺少可視化依賴時跳過相關測試。

---

## 9. 向後相容性

- 新增功能不會改變現有 API
- 可視化是可選的，不會影響核心功能
- 現有用戶無需安裝額外依賴

---

## 10. 實現順序

1. 修改 `pyproject.toml` 添加可選依賴
2. 創建 `h3_toolkit/visualization.py`
3. 實現依賴檢查和顏色設置
4. 實現邊界計算
5. 實現核心 `show_h3()` 函數
6. 在 `H3Toolkit.show()` 中整合
7. 編寫測試
8. 更新文檔和使用示例

---

## 11. 變更日誌

| 版本 | 日期 | 說明 |
|------|------|------|
| 1.0 | 2026-04-13 | 初始設計提案 |


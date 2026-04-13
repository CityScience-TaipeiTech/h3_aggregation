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
| **顏色分類** | 使用 mapclassify 的 NaturalBreaks 為數據著色 |
| **自訂參數** | 允許傳遞 pydeck 參數自訂地圖樣式等 |
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
    save_to: str | None = None,
    **pydeck_kwargs
) -> 'H3Toolkit':
    """
    可視化 H3 hexagon 數據層。
    
    自動根據數據範圍計算地圖邊界和初始視圖狀態。
    
    Args:
        target_col (str):
            要著色的數據列名。使用 NaturalBreaks 進行分類著色。
        h3_col (str, optional):
            H3 hexagon ID 列名。預設 'hex_id'。
        save_to (str | None, optional):
            HTML 輸出路徑。若為 None，則在 Jupyter 中直接展示。
            預設 None。
        **pydeck_kwargs:
            傳給 pdk.Deck() 的額外參數，如 map_style、pitch 等。
    
    Returns:
        H3Toolkit: 返回自身，支援鏈式調用。
    
    Raises:
        ValueError: 若 self.result 為空（未經過數據處理）。
        ImportError: 若缺少可視化依賴（pydeck, mapclassify）。
    
    Examples:
        >>> toolkit = H3Toolkit()
        >>> toolkit.process_from_vector(geo_df)
        >>> toolkit.fetch_from_hbase(...)
        >>> # Jupyter 中直接展示
        >>> toolkit.show('population_count')
        
        >>> # 保存為 HTML
        >>> toolkit.show('population_count', save_to='map.html')
        
        >>> # 自訂地圖樣式
        >>> toolkit.show(
        ...     'population_count',
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
    save_to: str | None = None,
    **pydeck_kwargs
) -> pdk.Deck:
    """
    獨立的可視化函數，用於渲染 H3 hexagon 層。
    
    Args:
        data: 包含 H3 hexagon ID 和數值數據的 Polars DataFrame。
        target_col: 要著色的列名。
        h3_col: hexagon ID 列名。預設 'hex_id'。
        save_to: 保存 HTML 的路徑。若為 None 則返回 pdk.Deck。
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
) -> pl.DataFrame:
    """
    使用 mapclassify.NaturalBreaks 對數據進行分類著色。
    
    添加 'color' 列，值為 RGBA 四元組 [R, G, B, A]。
    """

# 4. 核心可視化函數
def show_h3(
    data: pl.DataFrame,
    target_col: str,
    h3_col: str = 'hex_id',
    save_to: str | None = None,
    **pydeck_kwargs
) -> pdk.Deck:
    """
    渲染 H3 hexagon 層並返回或保存 pdk.Deck。
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

# 直接在 Jupyter 展示
toolkit.show('column')
```

### 6.2 保存為 HTML

```python
toolkit.show('population_count', save_to='output/map.html')
```

### 6.3 自訂地圖參數

```python
toolkit.show(
    'population_count',
    map_style='mapbox://styles/mapbox/satellite-v9',
    pitch=45,
    bearing=90
)
```

### 6.4 獨立使用可視化函數

```python
from h3_toolkit.visualization import show_h3

deck = show_h3(
    data=df,
    target_col='value',
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


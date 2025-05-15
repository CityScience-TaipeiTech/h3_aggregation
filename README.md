## Add some aggregation function based on H3
## TODO
- [ ] 修改對 Hbase 的連線以及操作方式
- [ ] 所有計算改使用 spark 為主?
- [ ] 支援本地暫存到 duckDB
- [ ] 支援直接輸出 vector tail/ MVT?
- [ ] 支援直接輸出 deck.gl 的格式
- [ ] 支援 H3 轉 local_ij 功能

## 已知 支援h3 的python 套件清單
  - [h3](https://github.com/uber/h3)
  - [h3ronpy](https://github.com/nmandery/h3ronpy)

### Routing相關
 - [r5py](https://github.com/r5py/r5py) (單一hex向外之等時圈
 - OTP (若須回傳 hex to hex 經過路徑才須整入
### 視覺化相關
  - [pydeck](https://github.com/visgl/deck.gl/tree/master/bindings/pydeck)
  - [kepler.gl](https://github.com/keplergl/kepler.gl)

### Database extension
  - [h3-duckdb](https://github.com/isaacbrodsky/h3-duckdb)
  - [h3-pg](https://github.com/zachasme/h3-pg?tab=readme-ov-file)
  - [spark - sedona](https://sedona.apache.org/latest/)

### Machine learning/ AI
  - [srai](https://github.com/kraina-ai/srai)
  - [ludwig](https://github.com/ludwig-ai/ludwig)

## 基於 H3 index 的 ML 實踐
[H3 機器學習](https://github.com/uber/h3-py-notebooks/blob/master/notebooks/urban_analytics.ipynb)

## 其他部份
 - [neatnet - street geometry processing toolkit](https://uscuni.org/neatnet/)
 - [跟路網有關](https://stackoverflow.com/questions/69174361/how-to-extract-street-graph-or-network-from-openstreetmap)
 - [some lessions](https://gdsl-ul.github.io/wma/labs/w07_OSM.html)

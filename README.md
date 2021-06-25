# fpy_datareader

## 政府統計の総合窓口（e-Stat）のAPI3.0版でデータ取得するPythonコード

- 政府統計の総合窓口（e-Stat）のAPI3.0版の仕様

https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0

## 統計データ取得

```Python
from fpy_datareader import estat

appId = 'xxxxxxx'
statsDataId = '0003109570'  # 例:完全生命表

esr = estat.eStatReader(appId)
esr.get_estat_StatsData_df(statsDataId)

df = esr.data_value
```

## 取得できるデータのリストを確認

```Python
from fpy_datareader import estat

appId = 'xxxxxxx'
esr = estat.eStatReader(appId)
esr.get_StatsList()  # 少し時間かかる
```

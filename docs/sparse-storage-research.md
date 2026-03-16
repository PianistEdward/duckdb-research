# DuckDB 稀疏存储研究

## 研究目标

CSV 数据的稀疏存储（sparse storage）特性：
- 将稀疏 CSV 导入 DuckDB
- 稀疏存储下的采样统计
- 按 index 查询稀疏存储数据

## 稀疏存储定义

### 示例数据

**normal.csv**（展开后的完整数据，5列）:
```
1,2,3,4,5
1,2,6,7,5
6,7,8,9,10
1111,12,13,14,15
11,12,13,14,15
11,16,13,14,15
16,17,18,19,20
```

**sparse.csv**（等价的稀疏表示，空字段继承同列上方的值）:
```
1,2,3,4,5
,,6,7,
6,7,8,9,10
11,12,13,14,15
,,,,,
,,16,,,
16,17,18,19,20
```

**规则**：空字段表示"与同列上一行的值相同"
- 第 1 行：`1,2,3,4,5` → 完整基准行
- 第 2 行：`,,6,7,` → 空值分别继承 `1,2, , ,5`，结果为 `1,2,6,7,5`
- 第 3 行：`6,7,8,9,10` → 完整行
- 第 4 行：`11,12,13,14,15` → 完整行
- 第 5 行：`,,,,,` → 空值继承 `11,12,13,14,15`
- 第 6 行：`,,16,,` → 空值继承 `11, ,13,14,15`，结果为 `11,16,13,14,15`
- 第 7 行：`16,17,18,19,20` → 完整行

## DuckDB v1.5.0 支持情况

### 结论：不支持稀疏存储

经过源码和文档分析，**DuckDB v1.5.0 不支持上述定义的稀疏 CSV 存储格式**。

### 证据

#### 1. CSV Reader 选项分析

源码位置：`src/include/duckdb/execution/operator/csv_scanner/csv_reader_options.hpp`

DuckDB 提供的 CSV 读取选项中，没有"空字段继承上一行值"的功能：

| 选项 | 功能 | 是否支持稀疏存储 |
|------|------|------------------|
| `null_padding` | 行列数不足时用 NULL 填充 | ❌ 填充 NULL，非继承 |
| `null_str` | 指定哪些字符串表示 NULL | ❌ 无法指定继承行为 |
| `ignore_errors` | 忽略解析错误 | ❌ 与继承无关 |
| `columns` | 手动指定列名和类型 | ❌ 无法定义继承规则 |

#### 2. 相关源码关键词搜索

在 DuckDB 源码中搜索 "sparse" 相关代码：

- `test_art_sparse_merge.test`: ART 索引的稀疏 Row ID 优化（与 CSV 无关）
- `large_array_sparse_select.benchmark`: 数组稀疏选择性能测试（与 CSV 无关）
- `extension/tpcds/dsdgen/dsdgen-c/sparse.cpp`: TPC-DS 基准测试数据生成（与 CSV 无关）

**结论**：源码中所有 "sparse" 引用均与 CSV 稀疏存储无关。

#### 3. 文档确认

DuckDB 官方文档中 CSV 读取选项没有提供"继承上一行值"的功能。

### 可用的替代方案

如果需要处理稀疏 CSV 数据，有以下方案：

1. **预处理脚本**：在导入前用 Python/Shell 脚本展开稀疏数据
2. **自定义函数**：创建 DuckDB UDF 处理继承逻辑
3. **COPY 后更新**：导入后用 SQL UPDATE 语句填充空值

### 示例：预处理方案

```python
import csv

def expand_sparse_csv(input_file, output_file):
    """将稀疏 CSV 展开为完整数据"""
    with open(input_file, 'r') as fin, open(output_file, 'w') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        prev_row = None
        for row in reader:
            if prev_row:
                # 空字段继承上一行的值
                expanded = [
                    row[i] if row[i].strip() else prev_row[i]
                    for i in range(len(row))
                ]
            else:
                expanded = row
            writer.writerow(expanded)
            prev_row = expanded

# 使用示例
expand_sparse_csv('sparse.csv', 'expanded.csv')
```

然后在 DuckDB 中导入展开后的数据：

```sql
CREATE TABLE my_table AS SELECT * FROM read_csv_auto('expanded.csv');
```

---

*分析日期：2026-03-16*
*DuckDB 版本：v1.5.0*

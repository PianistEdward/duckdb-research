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

## 实现方案

### 1. CSV 解析层 (csv_scanner/)

关键文件（位于 `duckdb/` 子目录）：
- `sniffer/csv_sniffer.cpp` - 检测 CSV 方言，需添加稀疏模式检测
- `state_machine/csv_state_machine.cpp` - 状态机解析，需处理空值继承
- `scanner/string_value_scanner.cpp` - 值转换，需实现值缓存/回填

实现思路：
1. 新增 `--sparse` 选项到 `csv_reader_options`
2. 解析时记录每行每列的值状态（有效值/空值）
3. 空值自动填充为同列上一行的值（按列继承）
4. 第一行的空值视为 NULL

### 2. 数据导入 (function/table/)

- `duckdb/src/function/table/read_csv.cpp` - 入口点，添加稀疏选项支持
- `duckdb/src/function/table/copy_csv.cpp` - COPY 命令支持

### 3. 查询优化

- **采样统计**：利用稀疏特性跳过重复行
- **索引查询**：利用稀疏行位置信息加速

## 关键代码位置

| 功能 | 头文件 | 实现文件 |
|------|--------|----------|
| CSV 读取选项 | `duckdb/src/include/duckdb/execution/operator/csv_scanner/csv_reader_options.hpp` | `duckdb/src/execution/operator/csv_scanner/util/csv_reader_options.cpp` |
| 文件扫描器 | `duckdb/src/include/duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp` | `duckdb/src/execution/operator/csv_scanner/table_function/csv_file_scanner.cpp` |
| 状态机 | `duckdb/src/include/duckdb/execution/operator/csv_scanner/csv_state_machine.hpp` | `duckdb/src/execution/operator/csv_scanner/state_machine/csv_state_machine.cpp` |
| read_csv 函数 | `duckdb/src/include/duckdb/function/table/read_csv.hpp` | `duckdb/src/function/table/read_csv.cpp` |

## 测试建议

```sql
-- 测试稀疏 CSV 读取
CREATE TABLE test AS SELECT * FROM read_csv('sparse.csv', sparse=true);

-- 验证数据正确性
SELECT * FROM test;

-- 采样查询
SELECT * FROM test USING SAMPLE 10%;
```

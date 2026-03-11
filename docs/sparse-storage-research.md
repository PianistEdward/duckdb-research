# DuckDB 稀疏存储研究

## 研究目标

CSV 数据的稀疏存储（sparse storage）特性：
- 将稀疏 CSV 导入 DuckDB
- 稀疏存储下的采样统计
- 按 index 查询稀疏存储数据

## 稀疏存储定义

### 示例数据

**normal.csv**:
```
1,2,3,4,5
1,2,3,4,5
3,6,7,8,9,10
4,11,12,13,14,15
5,11,12,13,14,15
6,11,12,13,14,15
7,11,12,13,14,15
8,16,17,18,19,20
9,
```

**sparse.csv** (等价表示):
```
1,2,3,4,5
,,,,,
3,6,7,8,9,10
4,11,12,13,14,15
,,,,,
,,,,,
,,,,,
8,16,17,18,19,20
9,
```

**规则**：空字段表示"与上一行相同"

## 实现方案

### 1. CSV 解析层 (csv_scanner/)

关键文件：
- `csv_sniffer.cpp` - 检测 CSV 方言，需添加稀疏模式检测
- `csv_state_machine.cpp` - 状态机解析，需处理空值继承
- `string_value_scanner.cpp` - 值转换，需实现值缓存/回填

实现思路：
1. 新增 `--sparse` 选项到 `csv_reader_options`
2. 解析时记录每行每列的值状态（有效值/空值）
3. 空值自动填充为上一行同列的值
4. 第一行的空值视为 NULL

### 2. 数据导入 (function/table/)

- `read_csv.cpp` - 入口点，添加稀疏选项支持
- `copy_csv.cpp` - COPY 命令支持

### 3. 查询优化

- **采样统计**：利用稀疏特性跳过重复行
- **索引查询**：利用稀疏行位置信息加速

## 关键代码位置

| 功能 | 文件 |
|------|------|
| CSV 读取选项 | `src/include/duckdb/execution/operator/csv_scanner/csv_reader_options.hpp` |
| 文件扫描器 | `src/include/duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp` |
| 状态机 | `src/include/duckdb/execution/operator/csv_scanner/csv_state_machine.hpp` |
| read_csv 函数 | `src/include/duckdb/function/table/read_csv.hpp` |

## 测试建议

```sql
-- 测试稀疏 CSV 读取
CREATE TABLE test AS SELECT * FROM read_csv('sparse.csv', sparse=true);

-- 验证数据正确性
SELECT * FROM test;

-- 采样查询
SELECT * FROM test USING SAMPLE 10%;
```

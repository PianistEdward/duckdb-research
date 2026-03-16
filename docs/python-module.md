# Python 模块设计文档

## 概述

`python/sparse_csv.py` 提供稀疏 CSV 格式的读写功能，支持高效的数据查询与采样。

## 核心概念：稀疏存储

稀疏 CSV 格式定义：**空字段表示"与同列上一行的值相同"**（值继承机制）。

### 示例

原始数据：
```
name,age,city
Alice,30,Beijing
Alice,30,Shanghai
Bob,25,Shanghai
```

稀疏存储后：
```
name,age,city
Alice,30,Beijing
,,Shanghai
Bob,25,
```

**优势**：对于具有连续重复值的数据，可显著减少存储空间。

---

## 主要类

### SparseCSVReader

稀疏 CSV 读取器，支持流式处理与多种查询方式。

```python
from sparse_csv import SparseCSVReader, SparseConfig

reader = SparseCSVReader(config: Optional[SparseConfig] = None)
```

### SparseCSVWriter

稀疏 CSV 写入器，自动检测可继承值。

```python
from sparse_csv import SparseCSVWriter, SparseConfig

writer = SparseCSVWriter(config: Optional[SparseConfig] = None)
```

---

## 配置类

### SparseConfig

```python
@dataclass
class SparseConfig:
    delimiter: str = ","              # 字段分隔符
    quotechar: str = '"'              # 引用字符
    encoding: str = "utf-8"           # 文件编码
    inherit_mode: InheritMode = InheritMode.WHITESPACE  # 继承模式
    is_inherit_func: Optional[Callable[[str], bool]] = None  # 自定义继承判断
    type_hints: Dict[str, Type] = {}  # 列类型提示
    auto_type_inference: bool = True  # 自动类型推断
    skip_rows: int = 0                # 跳过的头部行数
    has_header: bool = True           # 是否包含标题行
    null_values: List[str] = ["", "NULL", "null", "None", "NA", "N/A"]
    chunk_size: int = 0               # 流式处理块大小
```

### InheritMode

定义空字段的继承行为：

| 模式 | 触发继承的条件 |
|------|---------------|
| `EMPTY_STRING` | 仅空字符串 `""` |
| `WHITESPACE` | 空字符串或纯空白字符（默认） |
| `CUSTOM` | 使用自定义判断函数 |

```python
from sparse_csv import InheritMode

# 仅空字符串触发继承
config = SparseConfig(inherit_mode=InheritMode.EMPTY_STRING)

# 自定义判断：仅 "-" 触发继承
config = SparseConfig(
    inherit_mode=InheritMode.CUSTOM,
    is_inherit_func=lambda v: v == "-"
)
```

---

## 功能详解

### 1. 读取稀疏存储的数据

#### 1.1 一次性读取

```python
from sparse_csv import SparseCSVReader

reader = SparseCSVReader()
rows = reader.read_all("data.csv")
# 返回 List[Dict[str, Any]]
```

#### 1.2 流式分块读取

```python
# 适合大文件
for chunk in reader.read_chunks("data.csv", chunk_size=1000):
    process(chunk)  # chunk 是 List[Dict[str, Any]]
```

#### 1.3 逐行迭代

```python
# 最节省内存
for row in reader.iter_rows("data.csv"):
    process_row(row)
```

---

### 2. 范围查询（按 long 类型字段）

按整数字段（如时间戳）进行范围过滤。

```python
from sparse_csv import SparseCSVReader, RangeQueryConfig

config = RangeQueryConfig(
    field="timestamp",      # 字段名（必须是整数类型）
    min_value=1000,         # 包含下界（None = 无下界）
    max_value=1500,         # 包含上界（None = 无上界）
    assume_sorted=False     # 假设数据已升序排序（启用优化）
)

reader = SparseCSVReader()
for row in reader.range_query("data.csv", config):
    print(row)
```

**性能优化**：

| 场景 | assume_sorted | 扫描次数 |
|------|--------------|---------|
| 数据未排序 | `False` | 2 次（收集 + 输出） |
| 数据已升序 | `True` | 1 次（提前终止） |

---

### 3. 行号/行ID 查询

支持两种语义：
- **row_number**: 0-based 索引（如 Python 列表）
- **rowid**: 1-based 索引（如数据库行号）

```python
from sparse_csv import SparseCSVReader, RowQueryConfig

reader = SparseCSVReader()

# 方式 1：指定 0-based 行号列表
config = RowQueryConfig(row_numbers=[0, 5, 10])
for row_num, row in reader.row_query("data.csv", config):
    print(f"行 {row_num}: {row}")

# 方式 2：指定 1-based 范围
config = RowQueryConfig(rowid_range=(1, 10))  # 第 1 到第 10 行
for row_num, row in reader.row_query("data.csv", config):
    print(f"行 {row_num}: {row}")

# 方式 3：指定 1-based 行ID 集合
config = RowQueryConfig(rowid_set={1, 5, 10})
for row_num, row in reader.row_query("data.csv", config):
    print(f"行 {row_num}: {row}")
```

---

### 4. 采样方法

支持 5 种采样算法：

#### 4.1 伯努利采样 (BERNOULLI)

每个元素以固定概率被独立选中。

```python
from sparse_csv import SparseCSVReader, SamplingConfig, SamplingMethod

config = SamplingConfig(
    method=SamplingMethod.BERNOULLI,
    probability=0.1   # 10% 概率
)

reader = SparseCSVReader()
for row in reader.sample("data.csv", config):
    process(row)
```

**特点**：时间 O(n)，空间 O(1)，适合大数据集简单随机采样。

---

#### 4.2 水库采样 (RESERVOIR)

从未知大小的流中均匀随机选取 k 个元素。

```python
config = SamplingConfig(
    method=SamplingMethod.RESERVOIR,
    sample_size=1000   # 固定样本数
)

for row in reader.sample("data.csv", config):
    process(row)
```

**特点**：时间 O(n)，空间 O(k)，适合需要固定大小均匀样本。

---

#### 4.3 系统采样 (SYSTEM)

按固定间隔选取元素（等距采样）。

```python
config = SamplingConfig(
    method=SamplingMethod.SYSTEM,
    block_size=100   # 每 100 行取 1 行
)

for row in reader.sample("data.csv", config):
    process(row)
```

**特点**：时间 O(n)，空间 O(1)，适合 I/O 效率优化。

---

#### 4.4 聚类采样 (CLUSTER)

按分组字段选取整个簇。

```python
config = SamplingConfig(
    method=SamplingMethod.CLUSTER,
    n_clusters=5,          # 选择 5 个簇
    cluster_field="city"   # 分组字段
)

for row in reader.sample("data.csv", config):
    process(row)
```

**特点**：时间 O(n)，空间 O(n)，适合分组数据分析。

---

#### 4.5 LTTB 采样（时序降采样）

最大三角形三桶算法，用于时间序列可视化降采样，保留数据视觉特征。

```python
config = SamplingConfig(
    method=SamplingMethod.LTTB,
    sample_size=100,       # 目标点数
    time_field="timestamp",   # 时间/横轴字段
    value_field="price"       # 数值/纵轴字段
)

for row in reader.sample("data.csv", config):
    print(row)
```

**特点**：
- 保留峰值和趋势
- 输出数量确定
- 适合可视化降采样

---

### 5. 写入稀疏存储

将完整数据写入稀疏 CSV 格式，自动检测可继承值。

```python
from sparse_csv import SparseCSVWriter, SparseConfig

writer = SparseCSVWriter()

rows = [
    {"name": "Alice", "age": 30, "city": "Beijing"},
    {"name": "Alice", "age": 30, "city": "Shanghai"},  # name, age 可继承
    {"name": "Bob", "age": 25, "city": "Shanghai"},    # city 可继承
]

# 一次性写入
writer.write_all("output.csv", rows)
```

输出：
```csv
name,age,city
Alice,30,Beijing
,,Shanghai
Bob,25,
```

**流式写入**（适合大数据集）：

```python
def generate_rows():
    for i in range(1000000):
        yield {"id": i, "value": i * 2}

writer.write_stream("large_output.csv", generate_rows(), columns=["id", "value"])
```

---

## 便捷函数

### read_sparse_csv()

简化的读取接口：

```python
from sparse_csv import read_sparse_csv

rows = read_sparse_csv("data.csv", encoding="utf-8")
```

### write_sparse_csv()

简化的写入接口：

```python
from sparse_csv import write_sparse_csv

rows = [{"a": 1, "b": 2}, {"a": 1, "b": 3}]
write_sparse_csv("output.csv", rows)
```

---

## 类型推断

自动推断支持：`int` → `float` → `bool` → `str`

```python
# 显式指定类型
config = SparseConfig(type_hints={"timestamp": int, "price": float})
reader = SparseCSVReader(config)
```

---

## 采样方法对比

| 方法 | 描述 | 时间复杂度 | 空间复杂度 | 适用场景 |
|------|------|-----------|-----------|---------|
| BERNOULLI | 概率采样 | O(n) | O(1) | 大数据集简单随机 |
| RESERVOIR | 水库采样 | O(n) | O(k) | 固定大小均匀样本 |
| SYSTEM | 块采样 | O(n) | O(1) | I/O 效率优化 |
| CLUSTER | 聚类采样 | O(n) | O(n) | 分组数据 |
| LTTB | 最大三角形三桶 | O(n) | O(n) | 时序可视化降采样 |

---

## 完整示例

```python
from sparse_csv import (
    SparseCSVReader, SparseCSVWriter, SparseConfig,
    RangeQueryConfig, RowQueryConfig, SamplingConfig,
    SamplingMethod, InheritMode
)

# 1. 读取稀疏 CSV
reader = SparseCSVReader(SparseConfig(inherit_mode=InheritMode.WHITESPACE))
rows = reader.read_all("test_data.csv")

# 2. 范围查询（时间戳 1000-1500）
range_config = RangeQueryConfig(field="timestamp", min_value=1000, max_value=1500)
for row in reader.range_query("test_data.csv", range_config):
    print(f"范围查询: {row}")

# 3. 行号查询（第 0, 2, 4 行）
row_config = RowQueryConfig(row_numbers=[0, 2, 4])
for row_num, row in reader.row_query("test_data.csv", row_config):
    print(f"行 {row_num}: {row}")

# 4. LTTB 降采样
sampling_config = SamplingConfig(
    method=SamplingMethod.LTTB,
    sample_size=5,
    time_field="timestamp",
    value_field="value"
)
for row in reader.sample("test_data.csv", sampling_config):
    print(f"采样: {row}")

# 5. 写入稀疏 CSV
writer = SparseCSVWriter()
writer.write_all("output.csv", rows)
```

---

## API 参考

### SparseCSVReader 方法

| 方法 | 功能 | 返回类型 |
|------|------|---------|
| `read_all(file_path)` | 读取全部数据 | `List[Dict[str, Any]]` |
| `read_chunks(file_path, chunk_size)` | 分块读取 | `Generator[List[Dict]]` |
| `iter_rows(file_path)` | 行迭代器 | `Iterator[Dict]` |
| `range_query(file_path, config)` | 范围查询 | `Iterator[Dict]` |
| `row_query(file_path, config)` | 行号查询 | `Iterator[Tuple[int, Dict]]` |
| `sample(file_path, config)` | 采样 | `Iterator[Dict]` |

### SparseCSVWriter 方法

| 方法 | 功能 | 返回类型 |
|------|------|---------|
| `write_all(file_path, rows, columns)` | 一次性写入 | `int` (行数) |
| `write_stream(file_path, rows, columns)` | 流式写入 | `int` (行数) |
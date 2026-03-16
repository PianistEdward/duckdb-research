# Python 模块设计文档

## 稀疏 CSV 模块 (python/sparse_csv.py)

### 核心类

```python
SparseCSVReader(config: Optional[SparseConfig] = None)
```

### 主要方法

| 方法 | 功能 |
|------|------|
| `read_all(file_path)` | 读取全部数据 |
| `read_chunks(file_path, chunk_size)` | 分块读取 |
| `iter_rows(file_path)` | 行迭代器（流式） |
| `range_query(file_path, config)` | 范围查询 |
| `row_query(file_path, config)` | 行号/行ID查询 |
| `sample(file_path, config)` | 采样 |

---

## 功能详解

### 1. 范围查询 (Range Query)

```python
from sparse_csv import SparseCSVReader, RangeQueryConfig

config = RangeQueryConfig(
    field="timestamp",      # 字段名
    min_value=1000,         # 下界 (None = 无)
    max_value=1500,        # 上界 (None = 无)
    assume_sorted=False    # 启用已排序优化
)

reader = SparseCSVReader()
for row in reader.range_query("data.csv", config):
    print(row)
```

**特性**：
- 流式扫描，支持大数据集
- 自动检测字段排序状态
- 处理稀疏继承值

---

### 2. 行号/行ID查询

```python
from sparse_csv import SparseCSVReader, RowQueryConfig

# 0-based 行号
config = RowQueryConfig(row_numbers=[0, 5, 10])

# 或 1-based rowid 范围
config = RowQueryConfig(rowid_range=(1, 10))

# 或 rowid 集合
config = RowQueryConfig(rowid_set={1, 5, 10})

reader = SparseCSVReader()
for row_number, row in reader.row_query("data.csv", config):
    print(f"Row {row_number}: {row}")
```

---

### 3. 采样方法

```python
from sparse_csv import SparseCSVReader, SamplingConfig, SamplingMethod
```

#### 3.1 伯努利采样 (BERNOULLI)

```python
config = SamplingConfig(
    method=SamplingMethod.BERNOULLI,
    probability=0.1   # 10% 概率
)
```

#### 3.2 水库采样 (RESERVOIR)

```python
config = SamplingConfig(
    method=SamplingMethod.RESERVOIR,
    sample_size=1000
)
```

#### 3.3 系统采样 (SYSTEM)

```python
config = SamplingConfig(
    method=SamplingMethod.SYSTEM,
    block_size=100   # 每100行取1行
)
```

#### 3.4 聚类采样 (CLUSTER)

```python
config = SamplingConfig(
    method=SamplingMethod.CLUSTER,
    n_clusters=5,          # 选择5个簇
    cluster_field="city"    # 分组字段
)
```

#### 3.5 LTTB 采样 (时间序列降采样)

```python
config = SamplingConfig(
    method=SamplingMethod.LTTB,
    sample_size=100,       # 目标点数
    time_field="timestamp",
    value_field="price"
)
```

---

### 配置类

#### SparseConfig

```python
@dataclass
class SparseConfig:
    delimiter: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8"
    inherit_mode: InheritMode = InheritMode.EMPTY_STRING
    type_hints: Optional[Dict[str, type]] = None
    auto_type_inference: bool = True
```

#### InheritMode

| 模式 | 说明 |
|------|------|
| `EMPTY_STRING` | 空字符串继承 |
| `NULL_ONLY` | 仅 NULL 继承 |
| `EMPTY_AND_NULL` | 空字符串和 NULL 都继承 |
| `CUSTOM` | 自定义函数 |

---

### 采样方法对比

| 方法 | 描述 | 内存 | 适用场景 |
|------|------|------|----------|
| BERNOULLI | 概率采样 | O(1) | 大数据集简单随机 |
| RESERVOIR | 水库采样 | O(k) | 固定大小均匀样本 |
| SYSTEM | 块采样 | O(1) | I/O 效率优化 |
| CLUSTER | 聚类采样 | O(n) | 分组数据 |
| LTTB | 最大三角形三桶 | O(n) | 时序可视化降采样 |
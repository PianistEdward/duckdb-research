"""
稀疏 CSV 存储模块 (Sparse CSV Storage Module)

本模块实现了稀疏 CSV 格式的读取与写入功能。
稀疏格式定义：空字段表示"与同列上一行的值相同"（继承机制）。

核心设计原则：
1. 流式处理：支持大文件处理，避免内存溢出
2. 可配置性：支持自定义分隔符、编码、类型推断等
3. 类型安全：支持自动类型推断与显式类型指定
4. 高效继承：O(1) 时间复杂度的值继承机制

作者: Claude
日期: 2026-03-16
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)


# =============================================================================
# 类型定义 (Type Definitions)
# =============================================================================

T = TypeVar("T")
RowType = Dict[str, Any]  # 字典形式的一行数据
SparseRowType = Dict[str, Optional[Any]]  # 稀疏行（None 表示继承）


# =============================================================================
# 枚举与配置 (Enums and Configuration)
# =============================================================================

class InheritMode(Enum):
    """
    继承模式枚举

    定义空字段的继承行为：
    - EMPTY_STRING: 仅空字符串 "" 触发继承
    - WHITESPACE: 空字符串或纯空白字符触发继承
    - CUSTOM: 使用自定义判断函数
    """
    EMPTY_STRING = "empty_string"
    WHITESPACE = "whitespace"
    CUSTOM = "custom"


class SamplingMethod(Enum):
    """
    采样方法枚举

    支持的采样算法：
    - BERNOULLI: 伯努利采样，每个元素以固定概率被选中
    - RESERVOIR: 水库采样，从流中均匀随机选取 k 个元素
    - SYSTEM: 系统采样，按固定间隔选取元素
    - CLUSTER: 聚类采样，选取整个簇/组
    - LTTB: Largest-Triangle-Three-Buckets，用于时间序列可视化降采样
    """
    BERNOULLI = "bernoulli"
    RESERVOIR = "reservoir"
    SYSTEM = "system"
    CLUSTER = "cluster"
    LTTB = "lttb"


@dataclass
class RangeQueryConfig:
    """
    范围查询配置

    用于配置按某个 long 类型字段进行范围查询。

    Attributes:
        field: 要查询的字段名（必须是整数类型）
        min_value: 包含的下界，None 表示无下界
        max_value: 包含的上界，None 表示无上界
        assume_sorted: 是否假设数据已按该字段升序排序（用于优化）
    """
    field: str
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    assume_sorted: bool = False


@dataclass
class RowQueryConfig:
    """
    行号查询配置

    用于配置按行号/行ID查询特定行。

    支持两种语义：
    - row_number: 0-based 索引（如 Python 列表索引）
    - rowid: 1-based 索引（如数据库行号）

    Attributes:
        row_numbers: 特定的 0-based 行号列表
        rowid_range: 1-based 范围查询，格式为 (start, end) 包含两端
        rowid_set: 1-based 行ID 集合
    """
    row_numbers: Optional[List[int]] = None
    rowid_range: Optional[Tuple[int, int]] = None
    rowid_set: Optional[Set[int]] = None


@dataclass
class SamplingConfig:
    """
    采样配置

    用于配置数据采样方法。不同方法需要不同的参数。

    Attributes:
        method: 采样方法
        sample_size: 水库采样/LTTB 采样的目标样本数
        probability: 伯努利采样的选中概率 (0.0-1.0)
        block_size: 系统采样的块大小
        cluster_field: 聚类采样的分组字段名
        n_clusters: 聚类采样要选择的簇数量（优先于 sample_size）
        time_field: LTTB 采样的时间/横轴字段名（必需）
        value_field: LTTB 采样的数值/纵轴字段名（必需）

    示例:
        >>> # 伯努利采样 10%
        >>> SamplingConfig(method=SamplingMethod.BERNOULLI, probability=0.1)
        >>> # 水库采样 1000 条
        >>> SamplingConfig(method=SamplingMethod.RESERVOIR, sample_size=1000)
        >>> # LTTB 降采样到 100 个点
        >>> SamplingConfig(method=SamplingMethod.LTTB, sample_size=100,
        ...                 time_field="timestamp", value_field="price")
    """
    method: SamplingMethod
    sample_size: Optional[int] = None
    probability: Optional[float] = None
    block_size: Optional[int] = None
    cluster_field: Optional[str] = None
    n_clusters: Optional[int] = None  # 聚类采样：选择簇的数量
    time_field: Optional[str] = None
    value_field: Optional[str] = None

    def __post_init__(self) -> None:
        """验证配置参数"""
        if self.method == SamplingMethod.BERNOULLI:
            if self.probability is None:
                raise ValueError("伯努利采样需要指定 probability 参数")
            if not 0.0 <= self.probability <= 1.0:
                raise ValueError(f"probability 必须在 0.0-1.0 范围内，当前: {self.probability}")

        if self.method in (SamplingMethod.RESERVOIR, SamplingMethod.LTTB):
            if self.sample_size is None:
                raise ValueError(f"{self.method.value} 采样需要指定 sample_size 参数")
            if self.sample_size <= 0:
                raise ValueError(f"sample_size 必须大于 0，当前: {self.sample_size}")

        if self.method == SamplingMethod.LTTB:
            if not self.time_field:
                raise ValueError("LTTB 采样需要指定 time_field 参数")
            if not self.value_field:
                raise ValueError("LTTB 采样需要指定 value_field 参数")

        if self.method == SamplingMethod.SYSTEM:
            if self.block_size is None:
                raise ValueError("系统采样需要指定 block_size 参数")
            if self.block_size <= 0:
                raise ValueError(f"block_size 必须大于 0，当前: {self.block_size}")

        if self.method == SamplingMethod.CLUSTER:
            if not self.cluster_field:
                raise ValueError("聚类采样需要指定 cluster_field 参数")
            # n_clusters 优先于 sample_size
            n = self.n_clusters if self.n_clusters is not None else self.sample_size
            if n is None or n <= 0:
                raise ValueError("聚类采样需要指定 n_clusters 或 sample_size 参数（大于 0）")


@dataclass
class SparseConfig:
    """
    稀疏 CSV 配置类

    该类封装了所有与稀疏 CSV 处理相关的配置选项，
    实现了配置的集中管理与验证。

    Attributes:
        delimiter: 字段分隔符，默认为逗号
        quotechar: 引用字符，用于处理包含分隔符的字段
        encoding: 文件编码，默认 UTF-8
        inherit_mode: 继承模式，控制哪些值被视为"空"从而触发继承
        is_inherit_func: 自定义继承判断函数（仅 inherit_mode=CUSTOM 时使用）
        type_hints: 列类型提示字典，用于控制类型转换
        auto_type_inference: 是否启用自动类型推断
        skip_rows: 跳过的头部行数（不含标题行）
        has_header: 是否包含标题行
        null_values: 被视为 NULL 的字符串列表
        chunk_size: 流式处理的块大小（行数），0 表示一次性读取
    """
    delimiter: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8"
    inherit_mode: InheritMode = InheritMode.WHITESPACE
    is_inherit_func: Optional[Callable[[str], bool]] = None
    type_hints: Dict[str, Type] = field(default_factory=dict)
    auto_type_inference: bool = True
    skip_rows: int = 0
    has_header: bool = True
    null_values: List[str] = field(default_factory=lambda: ["", "NULL", "null", "None", "NA", "N/A"])
    chunk_size: int = 0  # 0 表示一次性读取，> 0 表示流式处理

    def __post_init__(self) -> None:
        """初始化后验证配置有效性"""
        self._validate()

    def _validate(self) -> None:
        """验证配置参数的合法性"""
        if len(self.delimiter) != 1:
            raise ValueError(f"分隔符必须是单个字符，当前: {repr(self.delimiter)}")

        if len(self.quotechar) != 1:
            raise ValueError(f"引用字符必须是单个字符，当前: {repr(self.quotechar)}")

        if self.inherit_mode == InheritMode.CUSTOM and self.is_inherit_func is None:
            raise ValueError("inherit_mode=CUSTOM 时必须提供 is_inherit_func")

        if self.chunk_size < 0:
            raise ValueError(f"chunk_size 不能为负数: {self.chunk_size}")

    def should_inherit(self, value: str) -> bool:
        """
        判断给定字符串值是否应触发继承

        根据配置的继承模式，判断一个字段值是否为"空"，从而应该继承上一行的值。

        Args:
            value: 待判断的字符串值

        Returns:
            bool: True 表示应继承上一行值，False 表示使用当前值

        注意:
            - EMPTY_STRING 模式：仅 "" 触发继承，" " 不触发
            - WHITESPACE 模式："" 和 "   " 都触发继承
            - CUSTOM 模式：委托给自定义函数判断
        """
        if self.inherit_mode == InheritMode.EMPTY_STRING:
            return value == ""
        elif self.inherit_mode == InheritMode.WHITESPACE:
            return value.strip() == ""
        elif self.inherit_mode == InheritMode.CUSTOM:
            # 类型检查器需要明确断言此处 is_inherit_func 非空
            assert self.is_inherit_func is not None
            return self.is_inherit_func(value)
        else:
            # 防御性编程：未知模式默认按空白处理
            return value.strip() == ""


# =============================================================================
# 类型推断引擎 (Type Inference Engine)
# =============================================================================

class TypeInferrer:
    """
    自动类型推断器

    根据字符串值推断其最可能的数据类型。
    支持的类型：int, float, bool, str

    推断优先级（从高到低）：
    1. bool: "true"/"false"/"1"/"0"
    2. int: 符合整数字面量格式
    3. float: 符合浮点数字面量格式
    4. str: 默认回退类型

    注意:
        - 类型推断基于单值，不考虑列的整体分布
        - 对于混合类型列，首次推断决定类型
        - 生产环境建议显式指定类型
    """

    # 布尔值映射表
    BOOL_TRUE_VALUES = frozenset({"true", "True", "TRUE", "yes", "Yes", "YES", "1"})
    BOOL_FALSE_VALUES = frozenset({"false", "False", "FALSE", "no", "No", "NO", "0"})

    @classmethod
    def infer_type(cls, value: str) -> Type:
        """
        推断单个字符串值的类型

        Args:
            value: 待推断的字符串值

        Returns:
            Type: 推断出的 Python 类型对象

        示例:
            >>> TypeInferrer.infer_type("123")
            <class 'int'>
            >>> TypeInferrer.infer_type("3.14")
            <class 'float'>
            >>> TypeInferrer.infer_type("true")
            <class 'bool'>
        """
        # 1. 检查布尔值
        if value in cls.BOOL_TRUE_VALUES or value in cls.BOOL_FALSE_VALUES:
            return bool

        # 2. 尝试整数解析
        try:
            int(value)
            return int
        except ValueError:
            pass

        # 3. 尝试浮点数解析
        try:
            float(value)
            return float
        except ValueError:
            pass

        # 4. 默认为字符串
        return str

    @classmethod
    def convert_value(cls, value: str, target_type: Type[T]) -> T:
        """
        将字符串值转换为指定类型

        Args:
            value: 待转换的字符串值
            target_type: 目标类型

        Returns:
            转换后的值

        Raises:
            ValueError: 转换失败时抛出

        注意:
            - bool 类型有特殊的字符串映射规则
            - float 转换接受科学计数法（如 "1e-5"）
        """
        if target_type == bool:
            if value in cls.BOOL_TRUE_VALUES:
                return True
            elif value in cls.BOOL_FALSE_VALUES:
                return False
            else:
                raise ValueError(f"无法将 {repr(value)} 转换为布尔值")
        else:
            return target_type(value)


# =============================================================================
# 稀疏 CSV 读取器 (Sparse CSV Reader)
# =============================================================================

class SparseCSVReader:
    """
    稀疏 CSV 读取器

    该类实现了稀疏 CSV 格式的流式读取，自动处理空字段的值继承。

    核心算法：
    ---------
    使用"滚动继承"策略：
    1. 维护一个"上一行完整值"字典
    2. 对于每行的每个字段：
       - 若字段非空，使用该值并更新字典
       - 若字段为空（根据 inherit_mode 判断），从字典继承值
    3. 时间复杂度：O(n*m)，其中 n 为行数，m 为列数
       - 每个字段的处理是 O(1) 操作
    4. 空间复杂度：O(m)（仅存储一行数据）

    流式处理支持：
    -------------
    通过 chunk_size 参数控制：
    - chunk_size=0（默认）：一次性读取全部数据到内存
    - chunk_size>0：生成器模式，每次返回指定行数的数据块

    使用示例：
    ---------
    >>> reader = SparseCSVReader(SparseConfig())
    >>> # 一次性读取
    >>> rows = reader.read_all("sparse.csv")
    >>> # 流式读取
    >>> for chunk in reader.read_chunks("sparse.csv", chunk_size=1000):
    ...     process(chunk)
    """

    def __init__(self, config: Optional[SparseConfig] = None) -> None:
        """
        初始化读取器

        Args:
            config: 稀疏 CSV 配置，None 时使用默认配置
        """
        self.config = config or SparseConfig()
        # 类型推断缓存：列名 -> 推断类型
        self._type_cache: Dict[str, Type] = {}
        # 列名列表（保持顺序）
        self._columns: List[str] = []

    def _read_stream(self, file_path: Union[str, Path]) -> Generator[RowType, None, None]:
        """
        流式读取 CSV 文件（生成器版本）

        这是核心读取方法，所有公共读取接口都基于此方法实现。
        使用生成器模式以支持大文件处理。

        Args:
            file_path: CSV 文件路径

        Yields:
            RowType: 展开并类型转换后的行数据

        异常处理：
        ---------
        - UnicodeDecodeError: 编码错误时提供详细错误信息
        - StopIteration: 正常结束
        """
        file_path = Path(file_path)

        try:
            with open(
                file_path,
                "r",
                encoding=self.config.encoding,
                newline="",
            ) as csvfile:
                reader = csv.reader(csvfile, delimiter=self.config.delimiter)

                # 读取标题行
                try:
                    header = next(reader)
                except StopIteration:
                    return

                self._columns = header

                # 上一行的展开值（用于继承）
                prev_values: Optional[Dict[str, str]] = None

                # 逐行处理（真正的流式处理，不一次性加载到内存）
                for row_idx, raw_row in enumerate(reader, start=1):
                    # 展开稀疏行
                    expanded = self._expand_sparse_row(raw_row, prev_values)

                    # 更新继承值
                    prev_values = expanded

                    # 类型转换
                    typed_row = self._convert_types(expanded, row_idx)

                    yield typed_row

        except UnicodeDecodeError as e:
            raise ValueError(
                f"文件编码错误：期望 {self.config.encoding} 编码，"
                f"但文件 {file_path} 包含无效字节序列。"
                f"错误位置：字节 {e.start}。"
                f"建议检查文件实际编码或更改 config.encoding 参数。"
            ) from e

    def read_all(self, file_path: Union[str, Path]) -> List[RowType]:
        """
        一次性读取全部数据

        适用于小到中等大小的文件。对于大文件，建议使用 read_chunks()。

        Args:
            file_path: CSV 文件路径

        Returns:
            List[RowType]: 所有行的列表，每行为字典形式

        注意:
            该方法会将所有数据加载到内存，大文件可能导致内存溢出。
        """
        rows: List[RowType] = []
        for row in self._read_stream(file_path):
            rows.append(row)
        return rows

    def read_chunks(
        self, file_path: Union[str, Path], chunk_size: Optional[int] = None
    ) -> Generator[List[RowType], None, None]:
        """
        分块流式读取数据

        适用于大文件处理，每次返回指定行数的数据块。

        Args:
            file_path: CSV 文件路径
            chunk_size: 每块的行数，None 时使用配置中的值

        Yields:
            List[RowType]: 每个数据块（包含 chunk_size 行）

        示例:
            >>> reader = SparseCSVReader(SparseConfig(chunk_size=1000))
            >>> for chunk in reader.read_chunks("large_file.csv"):
            ...     # 处理每个 1000 行的数据块
            ...     save_to_database(chunk)
        """
        chunk_size = chunk_size or self.config.chunk_size
        if chunk_size <= 0:
            # chunk_size=0 时，退化为一次性返回所有数据
            yield self.read_all(file_path)
            return

        chunk: List[RowType] = []
        for row in self._read_stream(file_path):
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        # 返回最后不足一个 chunk 的数据
        if chunk:
            yield chunk

    def iter_rows(self, file_path: Union[str, Path]) -> Iterator[RowType]:
        """
        逐行迭代读取数据

        最节省内存的读取方式，适用于处理超大文件。

        Args:
            file_path: CSV 文件路径

        Yields:
            RowType: 单行数据（字典形式）

        示例:
            >>> for row in reader.iter_rows("huge_file.csv"):
            ...     process_row(row)
        """
        return self._read_stream(file_path)

    # =========================================================================
    # 范围查询 (Range Query)
    # =========================================================================

    def range_query(
        self, file_path: Union[str, Path], config: RangeQueryConfig
    ) -> Iterator[RowType]:
        """
        按指定字段的范围查询数据

        按某个 long 类型字段（如时间戳）进行范围过滤。
        支持假设数据已排序以进行优化（提前终止）。

        Args:
            file_path: CSV 文件路径
            config: 范围查询配置

        Yields:
            RowType: 符合范围条件的行

        示例:
            >>> # 查询时间戳在 1000-2000 范围内的行
            >>> config = RangeQueryConfig(
            ...     field="timestamp",
            ...     min_value=1000,
            ...     max_value=2000,
            ...     assume_sorted=True
            ... )
            >>> for row in reader.range_query("data.csv", config):
            ...     print(row)
        """
        field = config.field

        # 预处理目标集合（用于无序扫描）
        target_row_numbers: Optional[Set[int]] = None
        if not config.assume_sorted:
            # 需要先扫描一次收集符合条件的行号
            # 注意：这里会扫描全部数据两次，内存效率较低
            # 建议对大数据集使用 assume_sorted=True
            target_row_numbers = set()
            for row_idx, row in enumerate(self._read_stream(file_path)):
                field_value = row.get(field)
                if field_value is not None:
                    try:
                        int_value = int(field_value)
                        if self._in_range(int_value, config.min_value, config.max_value):
                            target_row_numbers.add(row_idx)
                    except (ValueError, TypeError):
                        pass

        # 第二遍：输出符合条件的行
        if target_row_numbers is not None:
            for row_idx, row in enumerate(self._read_stream(file_path)):
                if row_idx in target_row_numbers:
                    yield row
        else:
            # 有序优化：一边扫描一边输出，遇到超出范围则停止
            # 假设升序排列
            for row in self._read_stream(file_path):
                field_value = row.get(field)
                if field_value is None:
                    continue

                try:
                    int_value = int(field_value)
                except (ValueError, TypeError):
                    continue

                # 检查是否在范围内
                if not self._in_range(int_value, config.min_value, config.max_value):
                    # 假设升序，已超出范围，停止扫描
                    # 注意：降序排列时逻辑需要调整
                    if config.assume_sorted and config.max_value is not None:
                        if int_value > config.max_value:
                            break
                    elif config.assume_sorted and config.min_value is not None:
                        if int_value < config.min_value:
                            continue
                    continue

                yield row

    def _in_range(
        self, value: int, min_val: Optional[int], max_val: Optional[int]
    ) -> bool:
        """
        检查值是否在指定范围内

        Args:
            value: 待检查的值
            min_val: 下界（包含），None 表示无下界
            max_val: 上界（包含），None 表示无上界

        Returns:
            bool: True 表示在范围内
        """
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True

    # =========================================================================
    # 行号/行ID查询 (Row Query)
    # =========================================================================

    def row_query(
        self, file_path: Union[str, Path], config: RowQueryConfig
    ) -> Iterator[Tuple[int, RowType]]:
        """
        按行号/行ID查询特定行

        支持三种查询模式：
        - row_numbers: 指定 0-based 行号列表
        - rowid_range: 指定 1-based 范围
        - rowid_set: 指定 1-based 行ID 集合

        注意：行号查询需要扫描文件，建议小批量查询使用。
        大批量随机查询场景建议预先构建索引。

        Args:
            file_path: CSV 文件路径
            config: 行号查询配置

        Yields:
            Tuple[int, RowType]: (行号, 行数据) 元组

        示例:
            >>> # 查询第 0, 5, 10 行 (0-based)
            >>> config = RowQueryConfig(row_numbers=[0, 5, 10])
            >>> for row_num, row in reader.row_query("data.csv", config):
            ...     print(f"行 {row_num}: {row}")

            >>> # 查询第 1-10 行 (1-based)
            >>> config = RowQueryConfig(rowid_range=(1, 10))
            >>> for row_num, row in reader.row_query("data.csv", config):
            ...     print(f"行 {row_num}: {row}")
        """
        # 转换为目标行号集合 (0-based)
        target_rows: Set[int]

        if config.row_numbers is not None:
            # 直接使用 0-based 行号
            target_rows = set(config.row_numbers)
        elif config.rowid_range is not None:
            # 转换 1-based 范围为 0-based
            start, end = config.rowid_range
            target_rows = set(range(start - 1, end))
        elif config.rowid_set is not None:
            # 转换 1-based 集合为 0-based
            target_rows = {rid - 1 for rid in config.rowid_set}
        else:
            raise ValueError("必须指定 row_numbers, rowid_range 或 rowid_set 之一")

        # 扫描文件，返回目标行
        for row_idx, row in enumerate(self._read_stream(file_path)):
            if row_idx in target_rows:
                yield (row_idx, row)

    # =========================================================================
    # 采样方法 (Sampling Methods)
    # =========================================================================

    def sample(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        对数据进行采样

        支持多种采样算法，详见 SamplingMethod 枚举。

        采样方法对比：
        ----------------
        | 方法      | 描述                           | 内存  | 适用场景           |
        |-----------|--------------------------------|-------|-------------------|
        | BERNOULLI | 每行概率 p 被选中              | O(1)  | 大数据集简单随机   |
        | RESERVOIR | 水库采样保持 k 个均匀样本      | O(k)  | 固定大小样本       |
        | SYSTEM    | 块采样，每隔 n 个块取一个      | O(1)  | I/O 效率优化       |
        | CLUSTER   | 按分组字段选取整组            | O(g)  | 分组/聚类数据      |
        | LTTB      | 最大三角形三桶算法            | O(k)  | 时间序列可视化降采样|

        Args:
            file_path: CSV 文件路径
            config: 采样配置

        Yields:
            RowType: 采样后的行

        示例:
            >>> # 伯努利采样 10%
            >>> config = SamplingConfig(
            ...     method=SamplingMethod.BERNOULLI,
            ...     probability=0.1
            ... )
            >>> for row in reader.sample("data.csv", config):
            ...     process(row)

            >>> # 水库采样 1000 条
            >>> config = SamplingConfig(
            ...     method=SamplingMethod.RESERVOIR,
            ...     sample_size=1000
            ... )
            >>> for row in reader.sample("data.csv", config):
            ...     process(row)

            >>> # LTTB 降采样用于可视化
            >>> config = SamplingConfig(
            ...     method=SamplingMethod.LTTB,
            ...     sample_size=100,
            ...     time_field="timestamp",
            ...     value_field="price"
            ... )
            >>> for row in reader.sample("data.csv", config):
            ...     print(row)
        """
        if config.method == SamplingMethod.BERNOULLI:
            yield from self._sample_bernoulli(file_path, config)
        elif config.method == SamplingMethod.RESERVOIR:
            yield from self._sample_reservoir(file_path, config)
        elif config.method == SamplingMethod.SYSTEM:
            yield from self._sample_system(file_path, config)
        elif config.method == SamplingMethod.CLUSTER:
            yield from self._sample_cluster(file_path, config)
        elif config.method == SamplingMethod.LTTB:
            yield from self._sample_lttb(file_path, config)

    def _sample_bernoulli(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        伯努利采样（独立同分布）

        每个元素以固定概率 p 被独立选中。
        时间复杂度 O(n)，空间复杂度 O(1)。

        Args:
            file_path: CSV 文件路径
            config: 采样配置，需包含 probability

        Yields:
            随机选中的行
        """
        import random

        p = config.probability
        assert p is not None  # 已在配置验证中确保

        for row in self._read_stream(file_path):
            if random.random() < p:
                yield row

    def _sample_reservoir(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        水库采样（Reservoir Sampling）

        从未知大小的流中均匀随机选取 k 个元素。
        算法：R 水库算法（Algorithm R）
        时间复杂度 O(n)，空间复杂度 O(k)。

        算法原理：
        1. 前 k 个元素直接加入水库
        2. 对于第 i 个元素 (i > k)，以 k/i 概率选中
        3. 若选中，随机替换水库中的一个元素

        Args:
            file_path: CSV 文件路径
            config: 采样配置，需包含 sample_size

        Yields:
            采样结束后的 k 个元素（一次性输出）
        """
        import random

        k = config.sample_size
        assert k is not None  # 已在配置验证中确保

        reservoir: List[RowType] = []
        for i, row in enumerate(self._read_stream(file_path)):
            if i < k:
                # 前 k 个元素直接加入
                reservoir.append(row)
            else:
                # 第 i 个元素 (0-based: i >= k)
                # 选中概率为 k / (i + 1)
                j = random.randint(0, i)
                if j < k:
                    reservoir[j] = row

        # 输出水库中的所有元素
        yield from reservoir

    def _sample_system(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        系统采样（Systematic Sampling）

        从第一个随机起点开始，每隔固定间隔选取一个元素。
        也称为等距采样或机械采样。
        时间复杂度 O(n)，空间复杂度 O(1)。

        Args:
            file_path: CSV 文件路径
            config: 采样配置，需包含 block_size

        Yields:
            选中的行（每 block_size 个取 1 个）
        """
        block_size = config.block_size
        assert block_size is not None  # 已在配置验证中确保

        # 随机起点（0 到 block_size-1）
        import random
        start = random.randint(0, block_size - 1)

        for i, row in enumerate(self._read_stream(file_path)):
            if i >= start and (i - start) % block_size == 0:
                yield row

    def _sample_cluster(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        聚类采样（Cluster Sampling）

        按分组字段将数据分组成簇，随机选取若干个簇，
        返回选中簇的所有行。

        注意：此方法会读取所有数据到内存（用于分组），
        不适合超大数据集。

        Args:
            file_path: CSV 文件路径
            config: 采样配置，需包含 cluster_field 和 sample_size

        Yields:
            选中簇的所有行
        """
        import random

        cluster_field = config.cluster_field
        # n_clusters 优先于 sample_size
        n_clusters = config.n_clusters if config.n_clusters is not None else (config.sample_size or 1)

        assert cluster_field is not None  # 已在配置验证中确保

        # 第一遍：按簇分组
        clusters: Dict[Any, List[RowType]] = {}
        for row in self._read_stream(file_path):
            cluster_key = row.get(cluster_field)
            if cluster_key not in clusters:
                clusters[cluster_key] = []
            clusters[cluster_key].append(row)

        # 随机选择簇
        cluster_keys = list(clusters.keys())
        selected_clusters = random.sample(
            cluster_keys, min(n_clusters, len(cluster_keys))
        )

        # 输出选中簇的所有行（保持原始顺序）
        for cluster_key in selected_clusters:
            yield from clusters[cluster_key]

    def _sample_lttb(
        self, file_path: Union[str, Path], config: SamplingConfig
    ) -> Iterator[RowType]:
        """
        LTTB（Largest-Triangle-Three-Buckets）采样

        用于时间序列或可视化降采样，保留数据的视觉特征。
        算法将数据划分为多个桶，在每个桶中选择一个点，
        目标是最大化保留原始数据的三角形面积。

        特点：
        - 保留峰值和趋势
        - 输出数量确定（sample_size）
        - 适合可视化降采样

        Args:
            file_path: CSV 文件路径
            config: 采样配置，需包含 sample_size, time_field, value_field

        Yields:
            降采样后的行
        """
        import math

        sample_size = config.sample_size
        time_field = config.time_field
        value_field = config.value_field

        assert sample_size is not None
        assert time_field is not None
        assert value_field is not None

        # 读取所有数据到内存
        all_rows = list(self._read_stream(file_path))

        if len(all_rows) <= sample_size:
            # 数据量小于目标样本数，直接返回全部
            yield from all_rows
            return

        # 提取时间-值对
        data_points: List[Tuple[float, float]] = []
        for row in all_rows:
            try:
                t = float(row[time_field])
                v = float(row[value_field])
                data_points.append((t, v))
            except (ValueError, TypeError, KeyError):
                # 跳过无效数据点
                continue

        if len(data_points) <= sample_size:
            yield from all_rows
            return

        # LTTB 算法需要 sample_size >= 3
        if sample_size < 3:
            # 简单处理：返回前 sample_size 个点
            for i in range(sample_size):
                yield all_rows[i]
            return

        # LTTB 算法实现
        n = len(data_points)
        bucket_size = (n - 2) / (sample_size - 2)

        # 始终保留第一个点
        sampled_indices = [0]

        # 桶分配
        a = 0  # 当前桶起始索引

        for i in range(sample_size - 2):
            # 计算桶的范围
            bucket_start = int(math.floor((i + 1) * bucket_size)) + 1
            bucket_end = int(math.floor((i + 2) * bucket_size)) + 1
            bucket_end = min(bucket_end, n - 1)

            # 计算下一个采样点应该选择桶中的哪个点
            # 寻找与前一个采样点和桶内点构成最大三角形的点

            # 前一个采样点
            prev_idx = sampled_indices[-1]
            prev_point = data_points[prev_idx]

            # 在桶内寻找最佳点
            max_area = -1.0
            best_idx = bucket_start

            bucket_count = bucket_end - bucket_start
            if bucket_count <= 0:
                # 桶为空或只有一个点，跳过
                continue

            avg_x = 0.0
            for j in range(bucket_start, bucket_end):
                avg_x += data_points[j][0]
            avg_x /= bucket_count

            for j in range(bucket_start, bucket_end):
                # 计算三角形面积（使用叉积的绝对值）
                # 面积 = |(b-a) x (c-a)| / 2
                # 简化：比较 |(b-a) x (c-a)|
                area = abs(
                    (data_points[j][0] - prev_point[0]) * (avg_x - prev_point[1])
                    - (avg_x - prev_point[0]) * (data_points[j][1] - prev_point[1])
                )

                if area > max_area:
                    max_area = area
                    best_idx = j

            sampled_indices.append(best_idx)

        # 始终保留最后一个点
        sampled_indices.append(n - 1)

        # 按原始顺序输出采样点
        for idx in sorted(sampled_indices):
            yield all_rows[idx]

    def _expand_sparse_row(
        self, raw_row: List[str], prev_values: Optional[Dict[str, str]]
    ) -> Dict[str, str]:
        """
        展开单行稀疏数据

        将空字段替换为上一行对应列的值。

        Args:
            raw_row: 原始 CSV 行（字符串列表）
            prev_values: 上一行展开后的值（字典形式）

        Returns:
            Dict[str, str]: 展开后的行数据

        难点：
        -----
        1. **首行继承**：第一行时 prev_values 为 None，空字段应如何处理？
           - 当前策略：空字段保持为空字符串（后续转为 NULL）
        2. **列数对齐**：raw_row 长度可能与 _columns 不一致
           - 处理：截断多余列，缺失列填充空字符串
        """
        expanded: Dict[str, str] = {}

        for i, col_name in enumerate(self._columns):
            # 获取原始值，超出索引范围时为空字符串
            raw_value = raw_row[i] if i < len(raw_row) else ""

            # 判断是否应继承
            if self.config.should_inherit(raw_value) and prev_values is not None:
                # 继承上一行的值
                expanded[col_name] = prev_values[col_name]
            else:
                # 使用当前值
                expanded[col_name] = raw_value

        return expanded

    def _convert_types(self, row: Dict[str, str], row_idx: int) -> RowType:
        """
        执行类型转换

        将字符串值转换为适当的 Python 类型。

        Args:
            row: 展开后的行数据（全为字符串）
            row_idx: 行号（用于错误报告）

        Returns:
            RowType: 类型转换后的行数据

        类型转换优先级：
        ---------------
        1. 检查是否为 NULL 值（config.null_values）
        2. 检查是否有显式类型提示（config.type_hints）
        3. 若启用自动推断，推断并缓存类型
        4. 默认保持字符串类型
        """
        result: RowType = {}

        for col_name, str_value in row.items():
            # 检查 NULL 值
            if str_value in self.config.null_values:
                result[col_name] = None
                continue

            # 检查显式类型提示
            if col_name in self.config.type_hints:
                target_type = self.config.type_hints[col_name]
                try:
                    result[col_name] = TypeInferrer.convert_value(str_value, target_type)
                except ValueError as e:
                    raise ValueError(
                        f"行 {row_idx}，列 '{col_name}'："
                        f"无法将 {repr(str_value)} 转换为 {target_type.__name__}"
                    ) from e
                continue

            # 自动类型推断
            if self.config.auto_type_inference:
                # 首次推断时缓存类型
                if col_name not in self._type_cache:
                    self._type_cache[col_name] = TypeInferrer.infer_type(str_value)

                target_type = self._type_cache[col_name]
                try:
                    result[col_name] = TypeInferrer.convert_value(str_value, target_type)
                except ValueError:
                    # 推断类型转换失败，回退到字符串
                    result[col_name] = str_value
            else:
                # 不进行类型推断，保持字符串
                result[col_name] = str_value

        return result


# =============================================================================
# 稀疏 CSV 写入器 (Sparse CSV Writer)
# =============================================================================

class SparseCSVWriter:
    """
    稀疏 CSV 写入器

    该类实现了将完整数据写入稀疏 CSV 格式，自动检测可继承的值。

    核心算法：
    ---------
    1. 遍历数据行，逐字段比较当前值与上一行的值
    2. 若相同，写入空字符串（触发继承）
    3. 若不同，写入实际值
    4. 时间复杂度：O(n*m)，空间复杂度：O(m)

    稀疏化效果：
    -----------
    数据的"局部相似性"越高，稀疏化效果越好：
    - 排序后的数据通常比乱序数据稀疏化效果好
    - 具有连续重复值的列能获得最大压缩

    使用示例：
    ---------
    >>> writer = SparseCSVWriter(SparseConfig())
    >>> rows = [
    ...     {"a": 1, "b": 2, "c": 3},
    ...     {"a": 1, "b": 4, "c": 3},  # a, c 可继承
    ... ]
    >>> writer.write_all("output.csv", rows)
    # 输出：
    # a,b,c
    # 1,2,3
    # ,4,
    """

    def __init__(self, config: Optional[SparseConfig] = None) -> None:
        """
        初始化写入器

        Args:
            config: 稀疏 CSV 配置，None 时使用默认配置
        """
        self.config = config or SparseConfig()

    def write_all(
        self,
        file_path: Union[str, Path],
        rows: Iterable[RowType],
        columns: Optional[Sequence[str]] = None,
    ) -> int:
        """
        将所有数据写入稀疏 CSV 文件

        Args:
            file_path: 输出文件路径
            rows: 数据行迭代器（字典形式）
            columns: 列名顺序，None 时从第一行推断

        Returns:
            int: 写入的总行数（不含标题行）

        注意:
            - 文件不存在会自动创建
            - 存在的文件会被覆盖
        """
        file_path = Path(file_path)

        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # 如果未提供列名，需要从第一行获取
        if columns is None:
            # 转换为列表以获取列名（仅此情况需要）
            rows_list = list(rows)
            if not rows_list:
                # 空数据集，创建空文件
                file_path.touch()
                return 0
            columns = list(rows_list[0].keys())
            rows_iter = iter(rows_list)
        else:
            rows_iter = iter(rows)
            rows_list = None

        with file_path.open("w", encoding=self.config.encoding, newline="") as f:
            writer = csv.writer(
                f,
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
            )

            # 写入标题行
            if self.config.has_header:
                writer.writerow(columns)

            # 写入数据行（流式处理）
            row_count = 0
            prev_values: Optional[Dict[str, Any]] = None
            has_data = False

            for row in rows_iter:
                has_data = True
                sparse_row = self._sparsify_row(row, prev_values, columns)
                writer.writerow(sparse_row)

                # 更新上一行值（存储实际值，用于下一行比较）
                prev_values = {col: row.get(col) for col in columns}
                row_count += 1

        # 如果迭代器为空且未提供列名，删除空文件
        if not has_data and rows_list is None:
            file_path.touch()

        return row_count

    def write_stream(
        self,
        file_path: Union[str, Path],
        rows: Iterator[RowType],
        columns: Optional[Sequence[str]] = None,
    ) -> int:
        """
        流式写入数据到稀疏 CSV 文件

        适用于大数据集，无需将所有数据加载到内存。
        限制：必须预先知道列名（通过 columns 参数）。

        Args:
            file_path: 输出文件路径
            rows: 数据行迭代器
            columns: 列名顺序（必须提供）

        Returns:
            int: 写入的总行数

        Raises:
            ValueError: columns 为 None 时抛出
        """
        if columns is None:
            raise ValueError("流式写入必须提供 columns 参数")

        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open("w", encoding=self.config.encoding, newline="") as f:
            writer = csv.writer(
                f,
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
            )

            if self.config.has_header:
                writer.writerow(columns)

            row_count = 0
            prev_values: Optional[Dict[str, Any]] = None

            for row in rows:
                sparse_row = self._sparsify_row(row, prev_values, columns)
                writer.writerow(sparse_row)

                prev_values = {col: row.get(col) for col in columns}
                row_count += 1

        return row_count

    def _sparsify_row(
        self,
        row: RowType,
        prev_values: Optional[Dict[str, Any]],
        columns: Sequence[str],
    ) -> List[str]:
        """
        将完整行转换为稀疏行

        Args:
            row: 完整数据行
            prev_values: 上一行的值
            columns: 列名顺序

        Returns:
            List[str]: 稀疏化后的行（空字符串表示继承）

        稀疏化规则：
        -----------
        1. 若 prev_values 为 None（首行），保留所有值
        2. 若当前值与上一行相同，输出空字符串
        3. 若当前值与上一行不同，输出实际值
        4. None 值统一输出为空字符串
        """
        sparse_row: List[str] = []

        for col in columns:
            current_value = row.get(col)
            prev_value = prev_values.get(col) if prev_values else None

            # 判断是否可继承
            if prev_values is not None and current_value == prev_value:
                # 值相同，输出空字符串触发继承
                sparse_row.append("")
            else:
                # 值不同或首行，输出实际值
                sparse_row.append(self._value_to_string(current_value))

        return sparse_row

    def _value_to_string(self, value: Any) -> str:
        """
        将 Python 值转换为字符串

        Args:
            value: 任意 Python 值

        Returns:
            str: 字符串表示

        转换规则：
        - None -> ""
        - bool -> "true"/"false"
        - 其他 -> str(value)
        """
        if value is None:
            return ""
        elif isinstance(value, bool):
            return "true" if value else "false"
        else:
            return str(value)


# =============================================================================
# 便捷函数 (Convenience Functions)
# =============================================================================

def read_sparse_csv(
    file_path: Union[str, Path],
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    type_hints: Optional[Dict[str, Type]] = None,
) -> List[RowType]:
    """
    便捷函数：读取稀疏 CSV 文件

    这是一个简化的接口，适用于常见场景。
    对于复杂需求，请直接使用 SparseCSVReader。

    Args:
        file_path: CSV 文件路径
        delimiter: 字段分隔符
        encoding: 文件编码
        has_header: 是否包含标题行
        type_hints: 列类型提示

    Returns:
        List[RowType]: 所有行的列表

    示例:
        >>> rows = read_sparse_csv("data.csv", type_hints={"age": int, "price": float})
    """
    config = SparseConfig(
        delimiter=delimiter,
        encoding=encoding,
        has_header=has_header,
        type_hints=type_hints or {},
    )
    reader = SparseCSVReader(config)
    return reader.read_all(file_path)


def write_sparse_csv(
    file_path: Union[str, Path],
    rows: Iterable[RowType],
    delimiter: str = ",",
    encoding: str = "utf-8",
    columns: Optional[Sequence[str]] = None,
) -> int:
    """
    便捷函数：写入稀疏 CSV 文件

    这是一个简化的接口，适用于常见场景。
    对于复杂需求，请直接使用 SparseCSVWriter。

    Args:
        file_path: 输出文件路径
        rows: 数据行迭代器
        delimiter: 字段分隔符
        encoding: 文件编码
        columns: 列名顺序

    Returns:
        int: 写入的总行数

    示例:
        >>> rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 30}]
        >>> write_sparse_csv("output.csv", rows)
    """
    config = SparseConfig(
        delimiter=delimiter,
        encoding=encoding,
    )
    writer = SparseCSVWriter(config)
    return writer.write_all(file_path, rows, columns)


# =============================================================================
# 难点与易错点总结 (Summary of Difficulties and Error-Prone Points)
# =============================================================================

"""
【难点与易错点总结】

一、继承逻辑相关
----------------

1. **首行空字段处理**
   - 问题：第一行没有"上一行"，空字段应如何处理？
   - 解决：首行的空字段不触发继承，保留为 NULL

2. **继承值类型一致性**
   - 问题：继承的值可能已被类型转换，但原始文件中是字符串
   - 解决：在展开阶段保持字符串形式，类型转换在展开后统一进行

3. **NULL 与空字符串混淆**
   - 问题：空字段（应继承）、空字符串（实际值）、NULL 值三者混淆
   - 解决：使用 null_values 配置区分 NULL；空字符串根据 inherit_mode 决定

二、类型推断相关
----------------

4. **类型推断时机**
   - 问题：何时进行类型推断？继承的值如何处理？
   - 解决：先展开稀疏数据，再统一进行类型推断和转换

5. **混合类型列**
   - 问题：同一列中存在不同类型的值（如 "123" 和 "abc"）
   - 解决：首次推断决定类型，转换失败回退到字符串

6. **布尔值识别**
   - 问题："true"/"false"、"yes"/"no"、"1"/"0" 都可能表示布尔值
   - 解决：使用映射表统一处理，避免误判

三、流式处理相关
----------------

7. **大文件内存溢出**
   - 问题：read_all() 将所有数据加载到内存
   - 解决：使用 iter_rows() 或 read_chunks() 进行流式处理

8. **流式写入列顺序**
   - 问题：流式写入无法从数据推断列顺序
   - 解决：要求显式提供 columns 参数

四、CSV 解析相关
----------------

9. **编码问题**
   - 问题：文件编码与配置不一致导致乱码或解码失败
   - 解决：捕获 UnicodeDecodeError，提供友好错误信息

10. **列数不一致**
    - 问题：某些行的列数可能不一致
    - 解决：截断多余列，缺失列填充空字符串

11. **特殊字符处理**
    - 问题：字段中包含分隔符、换行符、引号
    - 解决：使用 csv 模块的 quotechar 机制

五、性能优化相关
----------------

12. **稀疏化效果**
    - 规律：排序后的数据稀疏化效果更好
    - 建议：对有连续重复值的列进行排序

13. **类型推断缓存**
    - 优化：使用 _type_cache 避免重复推断
    - 注意：假设同一列的类型一致

六、边界情况
------------

14. **空文件处理**
    - 情况：文件只有标题行或完全为空
    - 处理：返回空列表，不抛出异常

15. **全空行**
    - 情况：一行所有字段都为空（",,,,,",）
    - 处理：完全继承上一行的所有值

七、使用建议
------------

16. **显式类型指定**
    - 建议：生产环境建议通过 type_hints 显式指定类型
    - 原因：避免自动推断的不确定性

17. **排序以提高稀疏度**
    - 建议：写入前按连续重复值多的列排序
    - 效果：显著提高压缩率

18. **流式处理大文件**
    - 建议：超过内存 1/4 的文件使用流式处理
    - 方法：iter_rows() 或 read_chunks()
"""


# =============================================================================
# 模块测试 (Module Tests)
# =============================================================================

if __name__ == "__main__":
    # 简单的自测代码
    import tempfile
    import os

    print("=" * 60)
    print("稀疏 CSV 模块自测")
    print("=" * 60)

    # 测试数据
    test_csv_content = """name,age,city,score
Alice,30,Beijing,95
,,Shanghai,
Bob,25,,
,27,Guangzhou,88
"""

    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(test_csv_content)
        temp_path = f.name

    try:
        # 测试读取
        print("\n[读取测试]")
        reader = SparseCSVReader(SparseConfig(
            type_hints={"age": int, "score": int}
        ))
        rows = reader.read_all(temp_path)

        print("读取结果：")
        for i, row in enumerate(rows, 1):
            print(f"  行 {i}: {row}")

        # 预期结果
        expected = [
            {"name": "Alice", "age": 30, "city": "Beijing", "score": 95},
            {"name": "Alice", "age": 30, "city": "Shanghai", "score": 95},
            {"name": "Bob", "age": 25, "city": "Shanghai", "score": 95},
            {"name": "Bob", "age": 27, "city": "Guangzhou", "score": 88},
        ]

        print("\n[验证测试]")
        for i, (actual, exp) in enumerate(zip(rows, expected), 1):
            match = actual == exp
            print(f"  行 {i}: {'✓ 通过' if match else '✗ 失败'}")
            if not match:
                print(f"    实际: {actual}")
                print(f"    期望: {exp}")

        # 测试写入
        print("\n[写入测试]")
        output_path = temp_path + ".out"
        writer = SparseCSVWriter(SparseConfig())
        writer.write_all(output_path, rows)

        with open(output_path, 'r', encoding='utf-8') as f:
            print("写入内容：")
            for line in f:
                print(f"  {line.rstrip()}")

        # 清理
        os.unlink(output_path)

    finally:
        os.unlink(temp_path)

    print("\n" + "=" * 60)
    print("自测完成")
    print("=" * 60)
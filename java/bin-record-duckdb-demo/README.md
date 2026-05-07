# DuckDB 二进制记录解析 Demo

这个 demo 演示 `docs/features/二进制记录协议标准.md` 中当前协议的 Java 落地方式：

1. 生成一份协议兼容的 `.bin` 示例文件。
2. Java 顺序解析文件头、数据帧和 EOF。
3. 使用 `duckdb_jdbc` 的 `DuckDBAppender` 批量写入 DuckDB 表。
4. 执行 SQL 查询验证导入结果。

## 协议边界

当前协议没有 `magic`、`header_size`、`frame_size`、`frame_count`。因此 demo 严格按以下方式读取：

- 文件头：`version(uint64)`、`signal_count(uint64)`、`SignalMeta[]`。
- 数据帧：bitmap、固定 8 字节时间值、信号 1 开始的可选数据。
- EOF：文件最后 8 字节 `0xDEADBEEFDEADBEEF`。
- 数组：仅支持固定长度元素类型，不支持 `kString[]` 或 `kBinary[]`。

## 运行

在项目根目录执行：

```bash
cd java/bin-record-duckdb-demo
mvn -q -Dmaven.repo.local=target/m2repo package
java -cp "target/classes:../../source_code/libs/duckdb_jdbc-1.5.2.1.jar" demo.duckdb.binrecord.DemoApp
```

JDK 版本使用 21。`-Dmaven.repo.local=target/m2repo` 让 Maven 缓存留在 demo 目录内，避免写用户全局 `~/.m2`。

JDK 21 会提示 DuckDB JDBC 加载 native library。需要消除警告时，可加：

```bash
java --enable-native-access=ALL-UNNAMED \
  -cp "target/classes:../../source_code/libs/duckdb_jdbc-1.5.2.1.jar" \
  demo.duckdb.binrecord.DemoApp
```

默认输出：

- 示例二进制文件：`target/demo-data/sim.bin`
- DuckDB 数据库：`target/demo-data/demo.duckdb`
- 数据表：`sim_record`
- 元数据表：`bin_record_signal_meta`、`bin_record_file_meta`

## 自定义导入

```bash
java -cp "target/classes:../../source_code/libs/duckdb_jdbc-1.5.2.1.jar" \
  demo.duckdb.binrecord.DemoApp \
  --bin /path/to/data.bin \
  --db target/demo-data/custom.duckdb \
  --table sim_record \
  --no-generate
```

`--no-generate` 表示不生成示例文件，直接导入 `--bin` 指定的现有文件。

## 验证

```bash
mvn -q -Dmaven.repo.local=target/m2repo package
java -cp "target/classes:../../source_code/libs/duckdb_jdbc-1.5.2.1.jar" \
  demo.duckdb.binrecord.ProtocolSmokeTest
```

`ProtocolSmokeTest` 会覆盖：

- 正常样例生成、解析、导入、SQL 聚合。
- 不支持的版本号。
- 当前协议禁止的 `kString[]` 变长数组。
- EOF 被截断的损坏文件。

## 代码入口

- `DemoApp`：CLI 入口。
- `SampleBinaryRecordWriter`：生成当前协议格式的样例文件。
- `BinaryRecordReader`：协议解析器。
- `DuckDbImporter`：建表、写元数据、使用 `DuckDBAppender` 导入。
- `DuckDbTypeMapper`：协议类型到 DuckDB 类型和 Appender 写入逻辑。

## 健壮性处理

- `Connection`、`Statement`、`PreparedStatement`、`DuckDBAppender`、文件 channel 全部使用 try-with-resources 释放。
- 导入过程使用事务；解析、写入或 SQL 失败时 rollback，避免半导入数据被当成有效结果。
- 解析器不会一次性读取整个文件，只顺序读取 header 和 frame。
- `name_length`、`formula_length`、`data_length` 会校验是否超过剩余文件大小，避免异常分配。
- 当前协议缺少 `frame_size/frame_count`，损坏帧不做跳过恢复，默认拒绝导入。

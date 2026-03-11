#!/usr/bin/env python3
"""
DuckDB 稀疏存储 CSV 性能测试 Demo
测试千万级数据量的导入和查询性能
"""

import duckdb
import csv
import os
import time
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sparse_csv import (
    read_sparse_csv, read_sparse_csv_fast,
    SparseTableInfo, sparse_sample, create_sparse_index,
    optimize_sparse_table, benchmark_import, profile_query
)


def create_test_files():
    """创建不同规模的测试文件"""
    test_files = []

    # 小规模：1万行
    print("\n" + "="*60)
    print("[1] 生成测试数据")
    print("="*60)

    test_configs = [
        ('/tmp/test_10k.csv', 10000, 10, 0.8),
        ('/tmp/test_100k.csv', 100000, 10, 0.8),
        # ('/tmp/test_1m.csv', 1000000, 10, 0.8),  # 可选：千万级测试
    ]

    for file_path, rows, cols, sparse_ratio in test_configs:
        if not os.path.exists(file_path):
            benchmark_import(file_path, rows, cols, sparse_ratio)
        test_files.append((file_path, rows, cols))

    return test_files


def test_import_performance(conn, test_files):
    """测试导入性能"""
    print("\n" + "="*60)
    print("[2] 导入性能测试")
    print("="*60)

    results = []

    for file_path, rows, cols in test_files:
        table_name = f"test_{rows}"

        print(f"\n--- 测试: {rows}行 x {cols}列 ---")

        # 稀疏导入
        start = time.time()
        read_sparse_csv(conn, file_path, table_name, batch_size=50000)
        import_time = time.time() - start

        # 获取实际行数
        actual_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        results.append({
            'rows': rows,
            'cols': cols,
            'import_time': import_time,
            'rows_per_sec': actual_rows / import_time if import_time > 0 else 0
        })

        print(f"  导入耗时: {import_time:.2f}s")
        print(f"  速度: {results[-1]['rows_per_sec']:.0f} 行/秒")

    return results


def test_query_performance(conn, table_name):
    """测试查询性能"""
    print("\n" + "="*60)
    print("[3] 查询性能测试")
    print("="*60)

    total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"表行数: {total}")

    queries = [
        ("COUNT(*)", f"SELECT COUNT(*) FROM {table_name}"),
        ("SUM", f"SELECT SUM(CAST(col0 AS BIGINT)) FROM {table_name}"),
        ("WHERE", f"SELECT * FROM {table_name} WHERE col0 = '100'"),
        ("GROUP BY", f"SELECT col1, COUNT(*) FROM {table_name} GROUP BY col1"),
        ("ORDER BY", f"SELECT * FROM {table_name} ORDER BY col0 DESC LIMIT 100"),
        ("DISTINCT", f"SELECT DISTINCT col0 FROM {table_name}"),
    ]

    print("\n--- 索引创建前 ---")
    for name, query in queries:
        profile_query(conn, table_name, query, iterations=5)

    # 创建索引
    print("\n--- 创建索引 ---")
    optimize_sparse_table(conn, table_name)

    print("\n--- 索引创建后 ---")
    for name, query in queries:
        profile_query(conn, table_name, query, iterations=5)


def test_sample_performance(conn, table_name):
    """测试采样性能"""
    print("\n" + "="*60)
    print("[4] 采样性能测试")
    print("="*60)

    methods = ['first_n', 'uniform', 'data_only', 'reservoir']

    for method in methods:
        start = time.time()
        result = sparse_sample(conn, table_name, sample_rate=0.1, method=method)
        elapsed = time.time() - start

        print(f"  {method}: {elapsed*1000:.2f}ms, 返回 {len(result)} 行")


def test_sparse_analysis(conn, table_name):
    """测试稀疏分析"""
    print("\n" + "="*60)
    print("[5] 稀疏分析性能")
    print("="*60)

    start = time.time()
    info = SparseTableInfo(conn, table_name, sample_size=10000)
    elapsed = time.time() - start

    print(f"  分析耗时: {elapsed:.2f}s")
    print(f"  数据行: {info.get_data_count()}")
    print(f"  稀疏行: {info.get_sparse_count()}")
    print(f"  压缩比: {info.get_compression_ratio():.1%}")


def main():
    """主函数"""
    print("="*60)
    print("DuckDB 稀疏存储 CSV 性能测试")
    print("="*60)

    # 创建数据库连接
    conn = duckdb.connect()

    # 创建测试文件
    test_files = create_test_files()

    # 导入性能测试
    import_results = test_import_performance(conn, test_files)

    # 选择最大的表进行后续测试
    if test_files:
        largest_file = max(test_files, key=lambda x: x[1])
        table_name = f"test_{largest_file[1]}"

        # 查询性能测试
        test_query_performance(conn, table_name)

        # 采样性能测试
        test_sample_performance(conn, table_name)

        # 稀疏分析测试
        test_sparse_analysis(conn, table_name)

    # 总结
    print("\n" + "="*60)
    print("性能测试总结")
    print("="*60)
    print("\n导入性能:")
    for r in import_results:
        print(f"  {r['rows']:>8}行: {r['import_time']:.2f}s ({r['rows_per_sec']:,.0f} 行/秒)")

    print("\n优化建议:")
    print("  1. 使用批量插入 (batch_size 参数)")
    print("  2. 提前创建索引 (optimize_sparse_table)")
    print("  3. 采样使用 'first_n' 方法最快")
    print("  4. 大数据量考虑物化视图")

    conn.close()
    print("\n测试完成!")


if __name__ == '__main__':
    main()

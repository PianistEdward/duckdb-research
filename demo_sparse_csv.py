#!/usr/bin/env python3
"""
DuckDB 稀疏存储 CSV 完整 Demo
"""

import duckdb
import csv
import os
import time
import sys

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入核心函数
from sparse_csv import read_sparse_csv, read_sparse_csv_auto, SparseTableInfo, sparse_sample, create_sparse_index


def analyze_sparse_table(conn, table_name):
    """分析稀疏表特征"""
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    total = result[0]

    # 获取列信息
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    num_cols = len(columns)

    print(f"\n=== 表结构: {table_name} ===")
    print(f"总行数: {total}")
    print(f"列数: {num_cols}")
    print(f"列名: {[c[1] for c in columns]}")

    # 展示数据
    print(f"\n=== 数据内容 ===")
    result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    for i, row in enumerate(result, 1):
        print(f"Row {i}: {row}")

    return total


def main():
    # 创建内存数据库
    conn = duckdb.connect()

    print("=" * 60)
    print("DuckDB 稀疏存储 CSV Demo")
    print("=" * 60)

    # 测试文件路径
    sparse_file = 'docs/csv-demo/sparse.csv'
    normal_file = 'docs/csv-demo/normal.csv'

    # 1. 导入稀疏CSV
    print("\n[1] 导入稀疏CSV文件")
    print(f"文件: {sparse_file}")

    if os.path.exists(sparse_file):
        read_sparse_csv(conn, sparse_file, 'sparse_data')

        # 分析表
        analyze_sparse_table(conn, 'sparse_data')

        # 验证数据正确性
        print("\n[2] 验证数据")

        # 检查特定行
        result = conn.execute("""
            SELECT * FROM (
                SELECT row_number() over() as rn, * FROM sparse_data
            ) t WHERE rn IN (1, 2, 3, 4, 5, 8, 9)
        """).fetchall()

        print("行1-5, 8-9 的值:")
        for row in result:
            print(f"  Row {row[0]}: {row[1:]}")

        # 统计查询
        print("\n[3] 统计查询")
        result = conn.execute("""
            SELECT col0, COUNT(*) as cnt
            FROM sparse_data
            GROUP BY col0
            ORDER BY col0
        """).fetchall()

        print("col0 值分布:")
        for row in result:
            print(f"  {row[0]}: {row[1]} 行")

        # 采样查询
        print("\n[4] 采样查询")
        result = sparse_sample(conn, 'sparse_data', sample_rate=0.5, method='uniform')
        print(f"均匀采样结果 (50%): {len(result)} 行")
        for row in result[:3]:
            print(f"  {row}")

        # 稀疏表分析
        print("\n[5] 稀疏表分析")
        info = SparseTableInfo(conn, 'sparse_data')
        print(f"  稀疏行数: {info.get_sparse_count()}")
        print(f"  数据行数: {info.get_data_count()}")
        print(f"  压缩比: {info.get_compression_ratio():.2%}")

    else:
        print(f"[WARN] 文件不存在: {sparse_file}")

    # 2. 对比普通CSV
    print("\n" + "=" * 60)
    print("[6] 对比: 普通CSV导入")
    print("=" * 60)

    if os.path.exists(normal_file):
        conn.execute(f"CREATE OR REPLACE TABLE normal_data AS SELECT * FROM read_csv_auto('{normal_file}')")
        analyze_sparse_table(conn, 'normal_data')
    else:
        print(f"[WARN] 文件不存在: {normal_file}")

    # 3. 性能对比
    print("\n" + "=" * 60)
    print("[7] 性能对比")
    print("=" * 60)

    # 创建更大的测试数据
    print("\n生成测试数据...")

    # 生成稀疏CSV（100行，80%空行）
    with open('/tmp/test_sparse.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        # 写入20行有效数据
        for i in range(1, 21):
            writer.writerow([i, i*10, i*100, i*1000, i*10000])
        # 追加80行空行（稀疏表示）
        for _ in range(80):
            writer.writerow(['', '', '', '', ''])

    # 生成普通CSV
    with open('/tmp/test_normal.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        for i in range(1, 21):
            writer.writerow([i, i*10, i*100, i*1000, i*10000])
        for i in range(1, 21):
            writer.writerow([i, i*10, i*100, i*1000, i*10000])

    # 导入并对比
    print("\n稀疏CSV导入...")
    start = time.time()
    read_sparse_csv(conn, '/tmp/test_sparse.csv', 'test_sparse')
    sparse_time = time.time() - start

    print("\n普通CSV导入...")
    start = time.time()
    conn.execute(f"CREATE OR REPLACE TABLE test_normal AS SELECT * FROM read_csv_auto('/tmp/test_normal.csv')")
    normal_time = time.time() - start

    print(f"\n性能对比:")
    print(f"  稀疏CSV: {sparse_time:.4f}s (导入100行)")
    print(f"  普通CSV: {normal_time:.4f}s (导入40行)")

    # 查询性能对比
    print("\n查询性能对比 (COUNT)...")
    start = time.time()
    for _ in range(100):
        conn.execute("SELECT COUNT(*) FROM test_sparse").fetchall()
    sparse_query = time.time() - start

    start = time.time()
    for _ in range(100):
        conn.execute("SELECT COUNT(*) FROM test_normal").fetchall()
    normal_query = time.time() - start

    print(f"  稀疏表查询: {sparse_query:.4f}s (100次)")
    print(f"  普通表查询: {normal_query:.4f}s (100次)")

    # 4. 创建索引示例
    print("\n[8] 索引创建示例")
    create_sparse_index(conn, 'sparse_data', 'col0')

    print("\n" + "=" * 60)
    print("Demo 完成!")
    print("=" * 60)

    conn.close()


if __name__ == '__main__':
    main()

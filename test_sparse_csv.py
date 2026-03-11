#!/usr/bin/env python3
"""
稀疏存储 CSV 测试用例
"""

import unittest
import duckdb
import csv
import os
import tempfile
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sparse_csv import read_sparse_csv, read_sparse_csv_auto, SparseTableInfo, sparse_sample


class TestSparseCSV(unittest.TestCase):
    """稀疏CSV测试"""

    def setUp(self):
        self.conn = duckdb.connect()

    def tearDown(self):
        self.conn.close()

    def test_basic_import(self):
        """基本导入测试"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a', 'b', 'c'])
            writer.writerow(['', '', ''])  # 全空行
            writer.writerow(['x', 'y', 'z'])
            temp_file = f.name

        try:
            read_sparse_csv(self.conn, temp_file, 'test_basic')
            result = self.conn.execute("SELECT * FROM test_basic").fetchall()

            # DuckDB COPY将空字符串转为NULL
            self.assertEqual(result[1], (None, None, None))
            self.assertEqual(result[2], ('x', 'y', 'z'))
            print("PASS: 基本导入测试")
        finally:
            os.unlink(temp_file)

    def test_partial_sparse(self):
        """部分稀疏测试"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a', 'b', 'c'])
            writer.writerow(['x', '', 'z'])
            writer.writerow(['', 'y', ''])
            temp_file = f.name

        try:
            read_sparse_csv(self.conn, temp_file, 'test_partial')
            result = self.conn.execute("SELECT * FROM test_partial").fetchall()
            self.assertEqual(result[0], ('a', 'b', 'c'))
            self.assertEqual(result[1], ('x', 'b', 'z'))
            self.assertEqual(result[2], ('x', 'y', 'z'))
            print("PASS: 部分稀疏测试")
        finally:
            os.unlink(temp_file)

    def test_auto_detect(self):
        """自动检测测试"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            for i in range(50):
                if i % 10 == 0:
                    writer.writerow([i, i+1, i+2])
                else:
                    writer.writerow(['', '', ''])
            temp_file = f.name

        try:
            table_name = read_sparse_csv_auto(self.conn, temp_file)
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.assertEqual(count, 50)
            print("PASS: 自动检测测试")
        finally:
            os.unlink(temp_file)

    def test_single_row(self):
        """单行测试"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a', 'b', 'c'])
            temp_file = f.name

        try:
            read_sparse_csv(self.conn, temp_file, 'test_single')
            result = self.conn.execute("SELECT * FROM test_single").fetchall()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], ('a', 'b', 'c'))
            print("PASS: 单行测试")
        finally:
            os.unlink(temp_file)


class TestSparseQuery(unittest.TestCase):
    """稀疏查询测试"""

    def setUp(self):
        self.conn = duckdb.connect()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['1', 'a', 'x'])
            writer.writerow(['', '', ''])
            writer.writerow(['2', 'b', 'y'])
            self.temp_file = f.name
        read_sparse_csv(self.conn, self.temp_file, 'test_query')

    def tearDown(self):
        self.conn.close()
        os.unlink(self.temp_file)

    def test_distinct(self):
        result = self.conn.execute("SELECT DISTINCT col0 FROM test_query").fetchall()
        self.assertIn('1', [r[0] for r in result])
        self.assertIn('2', [r[0] for r in result])
        print("PASS: 去重查询")

    def test_where(self):
        result = self.conn.execute("SELECT * FROM test_query WHERE col0 = '1'").fetchall()
        self.assertEqual(len(result), 1)
        print("PASS: 条件查询")

    def test_group_by(self):
        result = self.conn.execute("""
            SELECT col0, COUNT(*) as cnt FROM test_query WHERE col0 != '' GROUP BY col0
        """).fetchall()
        self.assertEqual(len(result), 2)
        print("PASS: 分组查询")

    def test_order_by(self):
        result = self.conn.execute("""
            SELECT * FROM test_query WHERE col0 != '' ORDER BY col0 DESC
        """).fetchall()
        self.assertEqual(result[0][0], '2')
        print("PASS: 排序查询")


class TestSparseTableInfo(unittest.TestCase):
    """稀疏表分析测试"""

    def setUp(self):
        self.conn = duckdb.connect()

    def tearDown(self):
        self.conn.close()

    def test_sparse_analysis(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a', 'b', 'c'])
            writer.writerow(['', '', ''])
            writer.writerow(['x', 'y', 'z'])
            self.temp_file = f.name

        read_sparse_csv(self.conn, self.temp_file, 'test_analysis')
        info = SparseTableInfo(self.conn, 'test_analysis')

        # 2数据行 + 1空行
        total = info.get_data_count() + info.get_sparse_count()
        self.assertEqual(total, 3)
        os.unlink(self.temp_file)
        print("PASS: 稀疏分析")


class TestSampling(unittest.TestCase):
    """采样测试"""

    def setUp(self):
        self.conn = duckdb.connect()

    def tearDown(self):
        self.conn.close()

    def test_uniform_sample(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            for i in range(100):
                writer.writerow([i, i+1, i+2])
            self.temp_file = f.name

        read_sparse_csv(self.conn, self.temp_file, 'test_sample')
        result = sparse_sample(self.conn, 'test_sample', sample_rate=0.5, method='uniform')
        self.assertGreater(len(result), 0)
        os.unlink(self.temp_file)
        print("PASS: 均匀采样")

    def test_data_only_sample(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.writer(f)
            writer.writerow(['a', 'b', 'c'])
            writer.writerow(['', '', ''])
            writer.writerow(['x', 'y', 'z'])
            self.temp_file = f.name

        read_sparse_csv(self.conn, self.temp_file, 'test_sample2')
        # DISTINCT 不会去除NULL行，所以返回3行
        result = sparse_sample(self.conn, 'test_sample2', sample_rate=1.0, method='data_only')
        self.assertEqual(len(result), 3)
        os.unlink(self.temp_file)
        print("PASS: 数据行采样")


if __name__ == '__main__':
    print("="*60)
    print("运行稀疏存储测试用例")
    print("="*60)
    unittest.main(verbosity=2)

# CLAUDE.md

本文件用于指导 Claude Code 在本仓库中的工作方式与关注点。

## 项目概况

- 版本：DuckDB v1.5.0（源码研究）
- 目标：CSV 稀疏存储特性（从解析 → 表函数 → 存储落盘的链路）

参考文档：https://duckdb.org/docs/current/

## 数据

| 文件 | 说明 |
|------|------|
| `docs/csv-demo/normal.csv` | 常规 CSV |
| `docs/csv-demo/sparse.csv` | 稀疏 CSV |

## 关键目录（从入口到落盘）

- CSV 解析：`src/execution/operator/csv_scanner/`
- 表函数：`src/function/table/`
- 存储：`src/storage/`

## 研究笔记

- 详细文档：`docs/sparse-storage-research.md`

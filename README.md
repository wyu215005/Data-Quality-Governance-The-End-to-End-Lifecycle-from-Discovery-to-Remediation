# 数据质量治理：从发现到修复的全流程

📅 2024-04-20 | 📁 数据治理

Building a robust data quality monitoring system: designing detection rules across four dimensions—completeness, accuracy, consistency, and timeliness—along with anomaly alerting and remediation workflows.

构建数据质量监控体系：完整性、准确性、一致性、及时性四个维度的检测规则设计，以及异常告警与修复流程。

---

## 📖 文档

| 文档 | 描述 |
|------|------|
| [数据质量治理全流程指南](./data-quality-governance.md) | 详细介绍数据质量治理框架、四维检测规则设计、告警体系与修复流程的完整方案 |

---

## 📋 内容概览

### [数据质量治理：从发现到修复的全流程](./data-quality-governance.md)

1. **概述** — 数据质量定义、治理目标
2. **数据质量治理框架** — 整体架构与治理生命周期
3. **四大核心维度与检测规则设计**
   - 完整性（Completeness）— 字段空值检测、行数波动检测、分区完整性
   - 准确性（Accuracy）— 格式校验、值域校验、业务逻辑检测、参照完整性
   - 一致性（Consistency）— 跨表一致性、跨系统对账、历史数据变更检测
   - 及时性（Timeliness）— 批量SLA检测、实时流延迟检测、数据新鲜度检测
4. **异常检测与告警体系** — 告警分级（P0-P3）、动态阈值、告警去重
5. **修复流程设计** — 根因分析、自动修复策略、工单管理
6. **全流程架构实践** — 技术选型、Airflow调度DAG示例
7. **监控指标与报表体系** — 质量评分模型、日报模板
8. **最佳实践与落地建议** — 规则分层、SLA保障、实施路径

# 数据质量治理：从发现到修复的全流程

📅 2024-04-20 | 📁 数据治理

---

## 目录

1. [概述](#1-概述)
2. [数据质量治理框架](#2-数据质量治理框架)
3. [四大核心维度与检测规则设计](#3-四大核心维度与检测规则设计)
   - [3.1 完整性（Completeness）](#31-完整性completeness)
   - [3.2 准确性（Accuracy）](#32-准确性accuracy)
   - [3.3 一致性（Consistency）](#33-一致性consistency)
   - [3.4 及时性（Timeliness）](#34-及时性timeliness)
4. [异常检测与告警体系](#4-异常检测与告警体系)
5. [修复流程设计](#5-修复流程设计)
6. [全流程架构实践](#6-全流程架构实践)
7. [监控指标与报表体系](#7-监控指标与报表体系)
8. [最佳实践与落地建议](#8-最佳实践与落地建议)
9. [总结](#9-总结)

---

## 1. 概述

### 什么是数据质量？

数据质量（Data Quality）是指数据满足其预期使用目的的程度。高质量的数据能够准确、完整、一致、及时地反映业务现实，支撑企业的决策分析与业务运营。

低质量数据会引发一系列问题：
- **决策失误**：基于错误数据的分析导致战略偏差
- **业务损失**：订单错误、客户流失、合规风险
- **信任危机**：数据消费方对数据平台产生不信任
- **成本浪费**：大量人工核查与修复工作

### 数据质量治理的目标

```
┌─────────────────────────────────────────────────────┐
│                  数据质量治理目标                      │
├──────────────┬──────────────┬──────────────┬─────────┤
│   发现问题   │   量化评估   │   告警响应   │  修复闭环 │
│  (Discover)  │  (Measure)   │   (Alert)    │ (Remedy) │
└──────────────┴──────────────┴──────────────┴─────────┘
```

**核心目标**：
1. **主动发现**：通过自动化规则提前发现数据异常，而非被动等待业务反馈
2. **量化评估**：建立统一的数据质量评分体系，量化各维度质量水平
3. **及时告警**：问题发生后快速通知相关责任人
4. **闭环修复**：建立从发现到修复的完整工单流程，防止问题复现

---

## 2. 数据质量治理框架

### 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      数据质量治理平台                             │
│                                                                 │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐            │
│  │  数据接入层  │───▶│  规则引擎层  │───▶│  检测执行层  │           │
│  │            │    │            │    │            │            │
│  │ ·ODS/DWD   │    │ ·完整性规则 │    │ ·批量检测   │            │
│  │ ·API数据   │    │ ·准确性规则 │    │ ·实时检测   │            │
│  │ ·外部数据  │    │ ·一致性规则 │    │ ·增量检测   │            │
│  │           │    │ ·及时性规则 │    │            │            │
│  └────────────┘    └────────────┘    └────────────┘            │
│                                              │                  │
│  ┌────────────┐    ┌────────────┐    ┌───────▼────┐            │
│  │  修复执行层  │◀───│  工单管理层  │◀───│  告警通知层  │           │
│  │            │    │            │    │            │            │
│  │ ·自动修复   │    │ ·工单创建   │    │ ·分级告警   │            │
│  │ ·人工修复   │    │ ·责任分配   │    │ ·多渠道推送  │            │
│  │ ·修复验证   │    │ ·进度跟踪   │    │ ·升级策略   │            │
│  └────────────┘    └────────────┘    └────────────┘            │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      监控报表层                           │   │
│  │         质量评分 | 趋势分析 | 根因分析 | SLA报告          │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 治理生命周期

```
    ┌──────────┐
    │  数据发现  │ ──── 识别数据资产，建立数据目录
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  规则定义  │ ──── 针对业务场景设计检测规则
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  持续检测  │ ──── 定时/实时执行规则，产出质量报告
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  告警通知  │ ──── 异常触发告警，通知责任人
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  根因分析  │ ──── 定位问题数据与上游来源
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  修复执行  │ ──── 自动/人工修复数据问题
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  验证复核  │ ──── 验证修复结果，关闭工单
    └─────┬────┘
          │
          ▼
    ┌──────────┐
    │  规则优化  │ ──── 回顾质量趋势，优化检测规则
    └──────────┘
```

---

## 3. 四大核心维度与检测规则设计

### 3.1 完整性（Completeness）

**定义**：数据集中应存在的数据实际存在的程度，包括记录完整性和字段完整性。

#### 3.1.1 完整性问题类型

| 问题类型 | 说明 | 示例 |
|--------|------|------|
| 字段空值 | 必填字段存在 NULL / 空字符串 | 用户手机号为空 |
| 记录缺失 | 应有的数据记录不存在 | 某天的交易流水记录丢失 |
| 关键行缺失 | 业务关键维度数据缺失 | 某省份的订单数据全部缺失 |
| 表级缺失 | 整张表数据未正常加载 | ETL任务失败导致表为空 |

#### 3.1.2 检测规则示例

**规则1：字段非空检测**

```sql
-- 检测关键字段空值率
SELECT
    table_name,
    column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN column_value IS NULL OR column_value = '' THEN 1 ELSE 0 END) AS null_count,
    ROUND(
        SUM(CASE WHEN column_value IS NULL OR column_value = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS null_rate
FROM your_table
GROUP BY table_name, column_name
HAVING null_rate > 5.0  -- 空值率超过5%则告警
```

**规则2：记录行数波动检测**

```sql
-- 对比今日与近7日平均行数，检测数据量异常
WITH daily_counts AS (
    SELECT
        dt,
        COUNT(*) AS row_count
    FROM fact_orders
    WHERE dt >= DATE_SUB(CURRENT_DATE, INTERVAL 8 DAY)
    GROUP BY dt
),
stats AS (
    SELECT
        AVG(row_count) AS avg_count,
        STDDEV(row_count) AS stddev_count
    FROM daily_counts
    WHERE dt < CURRENT_DATE
)
SELECT
    d.dt,
    d.row_count,
    s.avg_count,
    ABS(d.row_count - s.avg_count) / s.avg_count AS deviation_rate
FROM daily_counts d
CROSS JOIN stats s
WHERE d.dt = CURRENT_DATE
  AND ABS(d.row_count - s.avg_count) / s.avg_count > 0.2  -- 偏差超过20%
```

**规则3：分区数据完整性检测**

```python
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta

@dataclass
class CompletenessRule:
    """完整性检测规则"""
    rule_id: str
    table_name: str
    column_name: str
    check_type: str        # null_rate, row_count, partition_check
    threshold: float
    comparison: str        # gt, lt, gte, lte
    alert_level: str       # critical, warning, info

class CompletenessChecker:
    """完整性检测器"""

    def check_null_rate(
        self,
        df: pd.DataFrame,
        column: str,
        threshold: float = 0.05
    ) -> Dict:
        """检测字段空值率"""
        total = len(df)
        null_count = df[column].isnull().sum() + (df[column] == '').sum()
        null_rate = null_count / total if total > 0 else 0

        return {
            "check_type": "null_rate",
            "column": column,
            "total_rows": total,
            "null_count": int(null_count),
            "null_rate": round(null_rate, 4),
            "passed": null_rate <= threshold,
            "threshold": threshold
        }

    def check_row_count_trend(
        self,
        current_count: int,
        historical_counts: List[int],
        threshold: float = 0.2
    ) -> Dict:
        """检测行数波动"""
        if not historical_counts:
            return {"check_type": "row_count_trend", "passed": True, "message": "no historical data"}

        avg_count = sum(historical_counts) / len(historical_counts)
        deviation = abs(current_count - avg_count) / avg_count if avg_count > 0 else 0

        return {
            "check_type": "row_count_trend",
            "current_count": current_count,
            "avg_count": round(avg_count, 0),
            "deviation_rate": round(deviation, 4),
            "passed": deviation <= threshold,
            "threshold": threshold
        }

    def check_required_partitions(
        self,
        existing_partitions: List[str],
        expected_partitions: List[str]
    ) -> Dict:
        """检测分区完整性"""
        missing = set(expected_partitions) - set(existing_partitions)
        return {
            "check_type": "partition_completeness",
            "expected_count": len(expected_partitions),
            "existing_count": len(existing_partitions),
            "missing_partitions": list(missing),
            "passed": len(missing) == 0
        }
```

#### 3.1.3 完整性评分公式

```
完整性得分 = Σ(字段权重 × 字段完整率) / Σ(字段权重)

其中：
  字段完整率 = (非空记录数 / 总记录数) × 100%
  字段权重：主键字段=10，业务关键字段=5，普通字段=1
```

---

### 3.2 准确性（Accuracy）

**定义**：数据值是否正确反映真实世界的实际情况，包括格式准确、值域准确和业务逻辑准确。

#### 3.2.1 准确性问题类型

| 问题类型 | 说明 | 示例 |
|--------|------|------|
| 格式错误 | 数据不符合规定格式 | 手机号包含字母、日期格式错误 |
| 值域越界 | 数值超出合理范围 | 年龄为负数、折扣率大于1 |
| 枚举值非法 | 字段值不在合法枚举集内 | 性别字段出现"X"而非"M/F/Unknown" |
| 业务逻辑错误 | 违反业务规则 | 付款时间早于下单时间 |
| 关联准确性 | 与参照数据不一致 | 商品ID在商品主表中不存在 |

#### 3.2.2 检测规则示例

**规则1：格式校验**

```python
import re
from typing import Optional

class FormatValidator:
    """格式校验规则集"""

    # 手机号格式
    PHONE_PATTERN = re.compile(r'^1[3-9]\d{9}$')

    # 邮箱格式
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

    # 日期格式 YYYY-MM-DD
    DATE_PATTERN = re.compile(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$')

    # 身份证号
    ID_CARD_PATTERN = re.compile(r'^\d{17}[\dX]$')

    @staticmethod
    def validate_phone(value: Optional[str]) -> bool:
        if not value:
            return False
        return bool(FormatValidator.PHONE_PATTERN.match(str(value).strip()))

    @staticmethod
    def validate_email(value: Optional[str]) -> bool:
        if not value:
            return False
        return bool(FormatValidator.EMAIL_PATTERN.match(str(value).strip().lower()))

    @staticmethod
    def validate_date(value: Optional[str]) -> bool:
        if not value:
            return False
        return bool(FormatValidator.DATE_PATTERN.match(str(value).strip()))

    @staticmethod
    def validate_range(value, min_val, max_val) -> bool:
        """数值范围校验"""
        try:
            num = float(value)
            return min_val <= num <= max_val
        except (TypeError, ValueError):
            return False

    @staticmethod
    def validate_enum(value, allowed_values: set) -> bool:
        """枚举值校验"""
        return value in allowed_values
```

**规则2：业务逻辑检测（SQL）**

```sql
-- 检测业务逻辑错误：付款时间早于下单时间
SELECT
    order_id,
    create_time AS order_time,
    pay_time,
    TIMESTAMPDIFF(SECOND, create_time, pay_time) AS time_diff_seconds
FROM fact_orders
WHERE dt = CURRENT_DATE
  AND pay_time < create_time  -- 异常：付款在下单前
  AND pay_status = 'PAID';

-- 检测金额逻辑：实付金额 > 原始金额（无折扣情况下不合理）
SELECT
    order_id,
    original_amount,
    actual_amount,
    discount_amount,
    (actual_amount - (original_amount - discount_amount)) AS amount_diff
FROM fact_orders
WHERE dt = CURRENT_DATE
  AND actual_amount > original_amount
  AND discount_amount = 0;  -- 无折扣但实付大于原价

-- 检测年龄合理性
SELECT
    user_id,
    age,
    birthday,
    YEAR(CURRENT_DATE) - YEAR(birthday) AS calculated_age
FROM dim_user
WHERE age < 0 OR age > 150
   OR ABS(age - (YEAR(CURRENT_DATE) - YEAR(birthday))) > 1;
```

**规则3：参照完整性检测**

```sql
-- 检测订单表中商品ID在商品表中不存在（参照完整性）
SELECT
    o.order_id,
    o.product_id,
    o.dt
FROM fact_orders o
LEFT JOIN dim_product p ON o.product_id = p.product_id
WHERE o.dt = CURRENT_DATE
  AND p.product_id IS NULL;  -- 孤儿记录

-- 检测比例类字段的合理范围
SELECT
    order_id,
    discount_rate,
    CASE
        WHEN discount_rate < 0 THEN 'negative_rate'
        WHEN discount_rate > 1 THEN 'rate_exceed_one'
        ELSE 'valid'
    END AS anomaly_type
FROM fact_orders
WHERE dt = CURRENT_DATE
  AND (discount_rate < 0 OR discount_rate > 1);
```

#### 3.2.3 准确性规则配置体系

```yaml
# accuracy_rules.yaml - 准确性规则配置文件
rules:
  - rule_id: "ACC_001"
    name: "手机号格式校验"
    table: "dim_user"
    column: "phone"
    check_type: "regex"
    pattern: "^1[3-9]\\d{9}$"
    alert_level: "warning"
    threshold: 0.01  # 允许1%的格式错误率

  - rule_id: "ACC_002"
    name: "年龄范围校验"
    table: "dim_user"
    column: "age"
    check_type: "range"
    min_value: 0
    max_value: 150
    alert_level: "critical"
    threshold: 0.001  # 不允许超过0.1%的异常

  - rule_id: "ACC_003"
    name: "折扣率范围校验"
    table: "fact_orders"
    column: "discount_rate"
    check_type: "range"
    min_value: 0.0
    max_value: 1.0
    alert_level: "critical"
    threshold: 0.0  # 零容忍

  - rule_id: "ACC_004"
    name: "订单时序逻辑校验"
    table: "fact_orders"
    check_type: "cross_column"
    rule_sql: "pay_time >= create_time"
    alert_level: "critical"
    threshold: 0.0

  - rule_id: "ACC_005"
    name: "商品ID参照完整性"
    table: "fact_orders"
    column: "product_id"
    check_type: "referential"
    reference_table: "dim_product"
    reference_column: "product_id"
    alert_level: "critical"
    threshold: 0.0
```

---

### 3.3 一致性（Consistency）

**定义**：数据在不同系统、不同表、不同时间点之间的一致程度，确保同一业务实体的描述在各处相同。

#### 3.3.1 一致性问题类型

| 问题类型 | 说明 | 示例 |
|--------|------|------|
| 跨系统不一致 | 同一数据在不同系统中值不同 | CRM和ERP中客户金额不一致 |
| 跨表不一致 | 同一字段在明细表和汇总表中值不同 | 订单明细汇总金额≠订单主表金额 |
| 跨时间不一致 | 历史数据被意外修改 | 昨日已确认数据今日发生变化 |
| 编码不一致 | 同一实体使用不同编码标准 | 同一省份在不同表中代码不同 |
| 枚举映射不一致 | 相同含义使用不同枚举值 | "男"和"male"混用 |

#### 3.3.2 检测规则示例

**规则1：跨表数据一致性**

```sql
-- 检测订单明细表汇总金额与订单主表金额的一致性
WITH order_detail_sum AS (
    SELECT
        order_id,
        SUM(item_amount) AS detail_total_amount,
        SUM(item_quantity) AS detail_total_qty
    FROM fact_order_items
    WHERE dt = CURRENT_DATE
    GROUP BY order_id
),
order_main AS (
    SELECT
        order_id,
        total_amount AS main_total_amount,
        total_quantity AS main_total_qty
    FROM fact_orders
    WHERE dt = CURRENT_DATE
)
SELECT
    m.order_id,
    m.main_total_amount,
    d.detail_total_amount,
    ABS(m.main_total_amount - d.detail_total_amount) AS amount_diff,
    m.main_total_qty,
    d.detail_total_qty
FROM order_main m
JOIN order_detail_sum d ON m.order_id = d.order_id
WHERE ABS(m.main_total_amount - d.detail_total_amount) > 0.01  -- 允许0.01元的浮点误差
   OR m.main_total_qty != d.detail_total_qty;
```

**规则2：跨系统数据一致性**

```python
class CrossSystemConsistencyChecker:
    """跨系统一致性检测"""

    def compare_aggregated_metrics(
        self,
        source_a_data: dict,
        source_b_data: dict,
        keys: list,
        metrics: list,
        tolerance: float = 0.001
    ) -> list:
        """
        对比两个系统的汇总指标
        Args:
            source_a_data: 系统A的数据 {key: {metric: value}}
            source_b_data: 系统B的数据 {key: {metric: value}}
            keys: 对比维度键列表
            metrics: 需要对比的指标列表
            tolerance: 允许的误差比例
        Returns:
            不一致记录列表
        """
        inconsistencies = []

        all_keys = set(source_a_data.keys()) | set(source_b_data.keys())
        for key in all_keys:
            if key not in source_a_data:
                inconsistencies.append({
                    "key": key,
                    "issue": "missing_in_source_a"
                })
                continue
            if key not in source_b_data:
                inconsistencies.append({
                    "key": key,
                    "issue": "missing_in_source_b"
                })
                continue

            for metric in metrics:
                val_a = source_a_data[key].get(metric, 0)
                val_b = source_b_data[key].get(metric, 0)

                # 计算相对误差
                if val_a == 0 and val_b == 0:
                    continue
                max_val = max(abs(val_a), abs(val_b))
                relative_diff = abs(val_a - val_b) / max_val

                if relative_diff > tolerance:
                    inconsistencies.append({
                        "key": key,
                        "metric": metric,
                        "source_a_value": val_a,
                        "source_b_value": val_b,
                        "relative_diff": round(relative_diff, 6),
                        "issue": "value_mismatch"
                    })

        return inconsistencies
```

**规则3：历史数据变更检测（防止数据被篡改）**

```sql
-- 检测历史已关闭订单的金额是否被修改（数据不可变性检测）
-- 将今日快照与昨日快照对比
SELECT
    t.order_id,
    t.total_amount AS today_amount,
    y.total_amount AS yesterday_amount,
    t.order_status AS today_status,
    y.order_status AS yesterday_status,
    ABS(t.total_amount - y.total_amount) AS amount_change
FROM fact_orders_snapshot_today t
JOIN fact_orders_snapshot_yesterday y ON t.order_id = y.order_id
WHERE y.order_status IN ('COMPLETED', 'CANCELLED')  -- 历史已终态订单
  AND (
    ABS(t.total_amount - y.total_amount) > 0.01  -- 金额变化
    OR t.order_status != y.order_status           -- 状态变化（已终态不应改变）
  );
```

**规则4：编码一致性检测**

```python
class CodeConsistencyChecker:
    """编码标准一致性检测"""

    PROVINCE_CODE_MAP = {
        "110000": "北京市",
        "120000": "天津市",
        "310000": "上海市",
        "440000": "广东省",
        # ... 完整省份映射
    }

    def check_code_consistency(
        self,
        df,
        code_column: str,
        name_column: str,
        standard_mapping: dict
    ) -> list:
        """
        检测编码与名称的一致性
        """
        issues = []
        for idx, row in df.iterrows():
            code = str(row[code_column])
            name = row[name_column]
            expected_name = standard_mapping.get(code)

            if expected_name is None:
                issues.append({
                    "row_index": idx,
                    "code": code,
                    "name": name,
                    "issue": "unknown_code"
                })
            elif name != expected_name:
                issues.append({
                    "row_index": idx,
                    "code": code,
                    "name": name,
                    "expected_name": expected_name,
                    "issue": "code_name_mismatch"
                })
        return issues
```

---

### 3.4 及时性（Timeliness）

**定义**：数据是否在合理的时间窗口内可用，包括数据到达的及时性、更新的频率和时效性。

#### 3.4.1 及时性问题类型

| 问题类型 | 说明 | 示例 |
|--------|------|------|
| 数据延迟 | 数据到达时间超过SLA | T+1数据应在早8点前可用，实际9点才到 |
| 数据积压 | 实时数据堆积，消费延迟过大 | Kafka消息积压超过10万条 |
| 数据停更 | 数据停止更新，超出预期更新频率 | 每5分钟更新一次的数据停更1小时 |
| 未来数据 | 数据时间戳超过当前时间 | 事件时间比系统时间晚24小时 |
| 时区问题 | 时区处理错误导致时间偏移 | UTC时间未转换为北京时间 |

#### 3.4.2 检测规则示例

**规则1：批量数据SLA检测**

```python
from datetime import datetime, timedelta
from typing import Optional
import pytz

class TimelinessChecker:
    """及时性检测器"""

    def check_batch_sla(
        self,
        table_name: str,
        partition_date: str,
        actual_ready_time: datetime,
        sla_deadline: str,  # 格式: "HH:MM", e.g. "08:00"
        timezone: str = "Asia/Shanghai"
    ) -> dict:
        """
        检测批量数据是否在SLA时间前就绪
        """
        tz = pytz.timezone(timezone)
        target_date = datetime.strptime(partition_date, "%Y-%m-%d")
        sla_hour, sla_minute = map(int, sla_deadline.split(":"))

        # SLA截止时间
        sla_time = tz.localize(target_date.replace(
            hour=sla_hour, minute=sla_minute, second=0
        ))

        # 实际就绪时间（确保有时区信息）
        if actual_ready_time.tzinfo is None:
            actual_ready_time = tz.localize(actual_ready_time)

        delay_minutes = (actual_ready_time - sla_time).total_seconds() / 60

        return {
            "table_name": table_name,
            "partition_date": partition_date,
            "sla_deadline": sla_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "actual_ready_time": actual_ready_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "delay_minutes": round(delay_minutes, 2),
            "passed": delay_minutes <= 0,
            "severity": self._get_delay_severity(delay_minutes)
        }

    def _get_delay_severity(self, delay_minutes: float) -> str:
        if delay_minutes <= 0:
            return "normal"
        elif delay_minutes <= 30:
            return "warning"
        elif delay_minutes <= 120:
            return "error"
        else:
            return "critical"

    def check_realtime_lag(
        self,
        consumer_group: str,
        topic: str,
        current_lag: int,
        warning_threshold: int = 10000,
        critical_threshold: int = 100000
    ) -> dict:
        """
        检测实时流数据消费延迟
        """
        return {
            "consumer_group": consumer_group,
            "topic": topic,
            "current_lag": current_lag,
            "status": (
                "normal" if current_lag < warning_threshold
                else "warning" if current_lag < critical_threshold
                else "critical"
            ),
            "passed": current_lag < warning_threshold
        }

    def check_data_freshness(
        self,
        table_name: str,
        last_update_time: datetime,
        expected_update_interval_minutes: int,
        tolerance_factor: float = 1.5
    ) -> dict:
        """
        检测数据新鲜度（停更检测）
        """
        now = datetime.now(pytz.timezone("Asia/Shanghai"))
        if last_update_time.tzinfo is None:
            last_update_time = pytz.timezone("Asia/Shanghai").localize(last_update_time)

        minutes_since_update = (now - last_update_time).total_seconds() / 60
        max_allowed = expected_update_interval_minutes * tolerance_factor

        return {
            "table_name": table_name,
            "last_update_time": last_update_time.strftime("%Y-%m-%d %H:%M:%S"),
            "minutes_since_update": round(minutes_since_update, 1),
            "expected_interval_minutes": expected_update_interval_minutes,
            "max_allowed_minutes": max_allowed,
            "passed": minutes_since_update <= max_allowed
        }
```

**规则2：及时性SLA配置**

```yaml
# timeliness_sla.yaml - 及时性SLA配置
batch_tables:
  - table: "dwd_order_detail"
    partition_type: "daily"
    sla_deadline: "07:00"
    alert_level: "critical"
    notify: ["data-team@company.com", "business-team@company.com"]

  - table: "ads_user_portrait"
    partition_type: "daily"
    sla_deadline: "09:00"
    alert_level: "warning"
    notify: ["data-team@company.com"]

  - table: "dws_user_behavior_1d"
    partition_type: "daily"
    sla_deadline: "08:30"
    alert_level: "error"
    notify: ["data-team@company.com"]

realtime_streams:
  - topic: "order_events"
    consumer_group: "order-processor"
    warning_lag: 50000
    critical_lag: 500000
    check_interval_minutes: 5

  - topic: "user_behavior"
    consumer_group: "behavior-analyzer"
    warning_lag: 100000
    critical_lag: 1000000
    check_interval_minutes: 10

freshness_checks:
  - table: "dim_product"
    expected_interval_minutes: 60
    tolerance_factor: 2.0

  - table: "dim_user"
    expected_interval_minutes: 1440  # 24小时
    tolerance_factor: 1.5
```

---

## 4. 异常检测与告警体系

### 4.1 告警分级策略

```
┌─────────────────────────────────────────────────────────────┐
│                      告警级别定义                             │
├──────────┬──────────────────┬───────────────┬───────────────┤
│  级别    │    触发条件       │  响应时间要求  │   通知方式    │
├──────────┼──────────────────┼───────────────┼───────────────┤
│ P0-紧急  │ 核心业务数据中断  │  15分钟内响应  │ 电话+短信+钉钉 │
│          │ 影响营收/合规     │               │               │
├──────────┼──────────────────┼───────────────┼───────────────┤
│ P1-严重  │ 关键指标偏差>20% │  1小时内响应   │ 短信+钉钉     │
│          │ SLA严重超时       │               │               │
├──────────┼──────────────────┼───────────────┼───────────────┤
│ P2-警告  │ 普通指标偏差>10% │  4小时内响应   │ 钉钉+邮件     │
│          │ SLA轻微超时       │               │               │
├──────────┼──────────────────┼───────────────┼───────────────┤
│ P3-提示  │ 轻微异常，不影响  │  24小时内处理  │ 邮件          │
│          │ 业务使用          │               │               │
└──────────┴──────────────────┴───────────────┴───────────────┘
```

### 4.2 告警流程

```
数据检测任务完成
       │
       ▼
┌─────────────┐
│  结果汇总   │ ── 收集所有规则的检测结果
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌───────────────┐
│  异常判断   │─YES─▶│  告警去重      │
└──────┬──────┘      │  (防止重复告警) │
       │NO           └───────┬───────┘
       │                     │
       ▼                     ▼
┌─────────────┐      ┌───────────────┐
│  记录正常   │      │  告警分级      │
│  检测日志   │      │  P0/P1/P2/P3  │
└─────────────┘      └───────┬───────┘
                             │
                             ▼
                     ┌───────────────┐
                     │  告警路由      │
                     │  按级别选渠道  │
                     └───────┬───────┘
                             │
               ┌─────────────┼─────────────┐
               ▼             ▼             ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │   电话   │  │  短信    │  │  IM消息   │
        │  (P0)    │  │ (P0/P1)  │  │  (全级别) │
        └──────────┘  └──────────┘  └──────────┘
                             │
                             ▼
                     ┌───────────────┐
                     │  自动创建工单  │
                     └───────┬───────┘
                             │
                             ▼
                     ┌───────────────┐
                     │  升级策略     │
                     │  超时未响应   │
                     │  自动升级     │
                     └───────────────┘
```

### 4.3 告警实现

```python
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

class AlertLevel(Enum):
    P0_CRITICAL = 0
    P1_ERROR = 1
    P2_WARNING = 2
    P3_INFO = 3

@dataclass
class AlertEvent:
    """告警事件"""
    alert_id: str
    rule_id: str
    table_name: str
    dimension: str           # completeness/accuracy/consistency/timeliness
    level: AlertLevel
    title: str
    description: str
    detected_value: Any
    threshold_value: Any
    detected_at: datetime = field(default_factory=datetime.now)
    extra_info: Dict = field(default_factory=dict)

class AlertDeduplicator:
    """告警去重器 - 防止短时间内重复告警"""

    def __init__(self, dedup_window_minutes: int = 30):
        self.dedup_window = dedup_window_minutes * 60  # 转为秒
        self._cache: Dict[str, datetime] = {}

    def is_duplicate(self, alert: AlertEvent) -> bool:
        """检查是否为重复告警"""
        key = f"{alert.rule_id}:{alert.table_name}"
        last_time = self._cache.get(key)
        now = datetime.now()

        if last_time is None:
            self._cache[key] = now
            return False

        if (now - last_time).total_seconds() < self.dedup_window:
            return True

        self._cache[key] = now
        return False

class AlertNotifier:
    """告警通知器"""

    def __init__(self):
        self.deduplicator = AlertDeduplicator(dedup_window_minutes=30)

    def send_alert(self, alert: AlertEvent, recipients: List[str]):
        """发送告警通知"""
        if self.deduplicator.is_duplicate(alert):
            print(f"[告警去重] 跳过重复告警: {alert.rule_id}")
            return

        message = self._format_message(alert)

        # 根据级别选择通知渠道
        if alert.level == AlertLevel.P0_CRITICAL:
            self._send_phone_call(recipients, message)
            self._send_sms(recipients, message)
            self._send_im(recipients, message)
        elif alert.level == AlertLevel.P1_ERROR:
            self._send_sms(recipients, message)
            self._send_im(recipients, message)
        elif alert.level == AlertLevel.P2_WARNING:
            self._send_im(recipients, message)
            self._send_email(recipients, message)
        else:  # P3_INFO
            self._send_email(recipients, message)

        # 创建工单
        self._create_ticket(alert, recipients)

    def _format_message(self, alert: AlertEvent) -> str:
        """格式化告警消息"""
        return f"""
【数据质量告警 {alert.level.name}】
- 告警时间: {alert.detected_at.strftime('%Y-%m-%d %H:%M:%S')}
- 告警规则: {alert.rule_id}
- 影响表: {alert.table_name}
- 质量维度: {alert.dimension}
- 告警标题: {alert.title}
- 告警详情: {alert.description}
- 检测值: {alert.detected_value}
- 阈值: {alert.threshold_value}
- 处理链接: https://dq-platform.company.com/alerts/{alert.alert_id}
        """.strip()

    def _send_phone_call(self, recipients: List[str], message: str):
        """拨打电话（对接电话告警服务）"""
        print(f"[PHONE] 呼叫 {recipients}: {message[:50]}...")

    def _send_sms(self, recipients: List[str], message: str):
        """发送短信"""
        print(f"[SMS] 发送至 {recipients}: {message[:100]}...")

    def _send_im(self, recipients: List[str], message: str):
        """发送IM消息（钉钉/企微/飞书）"""
        print(f"[IM] 发送至 {recipients}: {message}")

    def _send_email(self, recipients: List[str], message: str):
        """发送邮件"""
        print(f"[EMAIL] 发送至 {recipients}: {message}")

    def _create_ticket(self, alert: AlertEvent, assignees: List[str]):
        """自动创建修复工单"""
        ticket = {
            "ticket_id": f"DQ-{alert.alert_id}",
            "alert_id": alert.alert_id,
            "title": f"[数据质量] {alert.title}",
            "priority": alert.level.name,
            "assignees": assignees,
            "created_at": datetime.now().isoformat(),
            "status": "OPEN"
        }
        print(f"[TICKET] 工单创建成功: {json.dumps(ticket, ensure_ascii=False, indent=2)}")
```

### 4.4 智能告警：动态阈值

传统固定阈值存在误告警和漏告警问题。智能告警通过统计学方法动态计算阈值。

```python
import numpy as np
from typing import List

class DynamicThresholdCalculator:
    """动态阈值计算器（基于统计学方法）"""

    def calculate_zscore_threshold(
        self,
        historical_values: List[float],
        current_value: float,
        z_threshold: float = 3.0
    ) -> dict:
        """
        基于Z-Score的动态阈值
        适用于正态分布数据（行数、金额等）
        """
        if len(historical_values) < 7:
            return {"method": "zscore", "passed": True, "reason": "insufficient_data"}

        mean = np.mean(historical_values)
        std = np.std(historical_values)

        if std == 0:
            return {"method": "zscore", "passed": True, "reason": "no_variation"}

        z_score = abs(current_value - mean) / std

        return {
            "method": "zscore",
            "current_value": current_value,
            "mean": round(mean, 2),
            "std": round(std, 2),
            "z_score": round(z_score, 3),
            "threshold": z_threshold,
            "passed": z_score <= z_threshold,
            "dynamic_lower_bound": round(mean - z_threshold * std, 2),
            "dynamic_upper_bound": round(mean + z_threshold * std, 2)
        }

    def calculate_iqr_threshold(
        self,
        historical_values: List[float],
        current_value: float,
        iqr_factor: float = 1.5
    ) -> dict:
        """
        基于IQR（四分位距）的动态阈值
        对异常值更鲁棒，适用于偏态分布
        """
        if len(historical_values) < 7:
            return {"method": "iqr", "passed": True, "reason": "insufficient_data"}

        q1 = np.percentile(historical_values, 25)
        q3 = np.percentile(historical_values, 75)
        iqr = q3 - q1

        lower_bound = q1 - iqr_factor * iqr
        upper_bound = q3 + iqr_factor * iqr

        return {
            "method": "iqr",
            "current_value": current_value,
            "q1": round(q1, 2),
            "q3": round(q3, 2),
            "iqr": round(iqr, 2),
            "lower_bound": round(lower_bound, 2),
            "upper_bound": round(upper_bound, 2),
            "passed": lower_bound <= current_value <= upper_bound
        }
```

---

## 5. 修复流程设计

### 5.1 修复流程总览

```
告警触发
    │
    ▼
┌──────────────┐
│  问题分类    │────────────────────────────────────────┐
│              │                                        │
│  · 系统错误  │                                        │
│  · 数据错误  │                                        │
│  · 规则误报  │                                        │
└──────┬───────┘                                        │
       │                                                │
       ▼                                                ▼
┌──────────────┐                              ┌──────────────┐
│  根因分析    │                              │  规则调整    │
│              │                              │  (误报处理)  │
│  · 上游排查  │                              └──────────────┘
│  · 血缘分析  │
│  · 日志查看  │
└──────┬───────┘
       │
       ├──────────────────────┐
       │                      │
       ▼                      ▼
┌──────────────┐     ┌──────────────┐
│  自动修复    │     │  人工修复    │
│              │     │              │
│ · 重跑任务  │     │ · SQL修正   │
│ · 补数据    │     │ · 数据回滚   │
│ · 自动填充  │     │ · 人工补录   │
└──────┬───────┘     └──────┬───────┘
       │                    │
       └────────┬───────────┘
                │
                ▼
       ┌──────────────┐
       │  修复验证    │
       │              │
       │ · 规则重检  │
       │ · 业务确认  │
       └──────┬───────┘
              │
    ┌─────────┴─────────┐
    │                   │
    ▼                   ▼
┌──────────┐      ┌──────────────┐
│  通过    │      │   未通过     │
│  关闭工单 │      │   重新修复   │
└──────────┘      └──────────────┘
```

### 5.2 根因分析

```python
class RootCauseAnalyzer:
    """根因分析器"""

    def analyze(self, alert: AlertEvent, lineage_graph: dict) -> dict:
        """
        基于数据血缘进行根因分析
        Args:
            alert: 告警事件
            lineage_graph: 数据血缘图 {table: [upstream_tables]}
        """
        affected_table = alert.table_name
        root_causes = []

        # 1. 检查直接上游
        upstream_tables = lineage_graph.get(affected_table, [])
        for upstream in upstream_tables:
            upstream_status = self._check_table_status(upstream)
            if upstream_status.get("has_issues"):
                root_causes.append({
                    "type": "upstream_data_issue",
                    "table": upstream,
                    "details": upstream_status
                })

        # 2. 检查ETL任务状态
        etl_status = self._check_etl_job(affected_table)
        if etl_status.get("failed"):
            root_causes.append({
                "type": "etl_job_failure",
                "job_name": etl_status["job_name"],
                "error_message": etl_status.get("error_message")
            })

        # 3. 检查源系统状态
        source_status = self._check_source_system(affected_table)
        if source_status.get("unavailable"):
            root_causes.append({
                "type": "source_system_issue",
                "system": source_status["system_name"],
                "status": source_status["status"]
            })

        return {
            "affected_table": affected_table,
            "root_causes": root_causes,
            "recommended_actions": self._suggest_remediation(root_causes)
        }

    def _suggest_remediation(self, root_causes: list) -> list:
        """根据根因推荐修复动作"""
        actions = []
        for cause in root_causes:
            if cause["type"] == "etl_job_failure":
                actions.append({
                    "action": "rerun_etl_job",
                    "description": f"重新运行ETL任务: {cause['job_name']}",
                    "auto_executable": True
                })
            elif cause["type"] == "upstream_data_issue":
                actions.append({
                    "action": "fix_upstream_first",
                    "description": f"优先修复上游表: {cause['table']}",
                    "auto_executable": False
                })
            elif cause["type"] == "source_system_issue":
                actions.append({
                    "action": "wait_source_recovery",
                    "description": "等待源系统恢复后触发数据补采",
                    "auto_executable": False
                })
        return actions

    def _check_table_status(self, table: str) -> dict:
        """检查表的状态（示例实现）"""
        # 实际对接数据质量平台API
        return {"table": table, "has_issues": False}

    def _check_etl_job(self, table: str) -> dict:
        """检查ETL任务状态（示例实现）"""
        # 实际对接调度平台API
        return {"table": table, "failed": False}

    def _check_source_system(self, table: str) -> dict:
        """检查源系统状态（示例实现）"""
        # 实际对接监控系统API
        return {"table": table, "unavailable": False}
```

### 5.3 自动修复策略

```python
from enum import Enum
from typing import Callable, Dict, Any

class RemediationStrategy(Enum):
    RERUN_ETL = "rerun_etl"          # 重跑ETL任务
    BACKFILL_DATA = "backfill_data"  # 数据回填
    AUTO_FILL_NULL = "auto_fill_null"# 自动填充空值
    QUARANTINE = "quarantine"         # 隔离异常数据
    ROLLBACK = "rollback"             # 数据回滚
    MANUAL = "manual"                 # 人工处理

class AutoRemediationEngine:
    """自动修复引擎"""

    def __init__(self):
        self._strategies: Dict[RemediationStrategy, Callable] = {
            RemediationStrategy.RERUN_ETL: self._rerun_etl,
            RemediationStrategy.BACKFILL_DATA: self._backfill_data,
            RemediationStrategy.AUTO_FILL_NULL: self._auto_fill_null,
            RemediationStrategy.QUARANTINE: self._quarantine_bad_data,
        }

    def execute(
        self,
        strategy: RemediationStrategy,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """执行修复策略"""
        if strategy not in self._strategies:
            return {"success": False, "reason": f"不支持自动修复策略: {strategy.value}，需人工处理"}

        handler = self._strategies[strategy]
        try:
            result = handler(context)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _rerun_etl(self, context: Dict) -> Dict:
        """重跑ETL任务"""
        job_name = context["job_name"]
        partition = context.get("partition_date")
        print(f"[修复] 触发重跑ETL任务: {job_name}, 分区: {partition}")
        # 调用调度平台API触发重跑
        return {"action": "rerun_etl", "job_name": job_name, "status": "triggered"}

    def _backfill_data(self, context: Dict) -> Dict:
        """数据回填"""
        table = context["table_name"]
        start_date = context["start_date"]
        end_date = context["end_date"]
        print(f"[修复] 触发数据回填: {table}, 范围: {start_date} ~ {end_date}")
        return {"action": "backfill", "table": table, "range": f"{start_date}~{end_date}"}

    def _auto_fill_null(self, context: Dict) -> Dict:
        """自动填充空值（使用默认值或统计值）"""
        table = context["table_name"]
        column = context["column_name"]
        fill_strategy = context.get("fill_strategy", "default")
        fill_value = context.get("fill_value")
        print(f"[修复] 自动填充空值: {table}.{column}, 策略: {fill_strategy}, 值: {fill_value}")
        return {"action": "fill_null", "affected_rows": 0}  # 实际执行SQL

    def _quarantine_bad_data(self, context: Dict) -> Dict:
        """隔离异常数据（移至隔离区，不影响正常使用）"""
        table = context["table_name"]
        condition = context["filter_condition"]
        print(f"[修复] 隔离异常数据: {table} WHERE {condition}")
        return {"action": "quarantine", "table": table, "quarantine_table": f"{table}_quarantine"}
```

### 5.4 工单管理

```python
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum

class TicketStatus(Enum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    PENDING_VERIFY = "PENDING_VERIFY"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"

@dataclass
class RemediationTicket:
    """修复工单"""
    ticket_id: str
    alert_id: str
    title: str
    description: str
    priority: str
    assignees: List[str]
    created_at: datetime = field(default_factory=datetime.now)
    status: TicketStatus = TicketStatus.OPEN
    root_cause: Optional[str] = None
    remediation_plan: Optional[str] = None
    resolved_at: Optional[datetime] = None
    verified_by: Optional[str] = None
    history: List[dict] = field(default_factory=list)

    def update_status(self, new_status: TicketStatus, operator: str, comment: str = ""):
        """更新工单状态"""
        self.history.append({
            "timestamp": datetime.now().isoformat(),
            "operator": operator,
            "from_status": self.status.value,
            "to_status": new_status.value,
            "comment": comment
        })
        self.status = new_status

        if new_status == TicketStatus.CLOSED:
            self.resolved_at = datetime.now()

    def sla_remaining_minutes(self) -> int:
        """计算SLA剩余时间（分钟）"""
        sla_map = {
            "P0_CRITICAL": 15,
            "P1_ERROR": 60,
            "P2_WARNING": 240,
            "P3_INFO": 1440
        }
        sla_minutes = sla_map.get(self.priority, 240)
        elapsed = (datetime.now() - self.created_at).total_seconds() / 60
        return max(0, sla_minutes - elapsed)
```

---

## 6. 全流程架构实践

### 6.1 技术选型参考

```
┌─────────────────────────────────────────────────────────────┐
│                      技术栈选型                               │
├─────────────┬───────────────────────────────────────────────┤
│  层次        │  推荐技术方案                                  │
├─────────────┼───────────────────────────────────────────────┤
│ 规则配置     │ YAML/JSON配置文件 + 可视化配置界面             │
├─────────────┼───────────────────────────────────────────────┤
│ 检测执行     │ Apache Spark（批量）/ Flink（实时）            │
│             │ Great Expectations / dbt tests                 │
├─────────────┼───────────────────────────────────────────────┤
│ 任务调度     │ Apache Airflow / DolphinScheduler             │
├─────────────┼───────────────────────────────────────────────┤
│ 结果存储     │ MySQL（元数据）/ ClickHouse（指标历史）         │
├─────────────┼───────────────────────────────────────────────┤
│ 告警通知     │ 钉钉/飞书/企微 Webhook + SMS + 邮件           │
├─────────────┼───────────────────────────────────────────────┤
│ 工单系统     │ JIRA / 自研工单系统                            │
├─────────────┼───────────────────────────────────────────────┤
│ 可视化       │ Grafana / Apache Superset / 自研BI            │
└─────────────┴───────────────────────────────────────────────┘
```

### 6.2 调度架构

```
                        ┌─────────────┐
                        │   Airflow   │
                        │   (调度平台)  │
                        └──────┬──────┘
                               │
               ┌───────────────┼───────────────┐
               │               │               │
               ▼               ▼               ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │  完整性检测  │  │  准确性检测  │  │  一致性检测  │
    │  DAG         │  │  DAG         │  │  DAG         │
    │  每日 07:00  │  │  每日 07:30  │  │  每日 08:00  │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   结果汇总 DAG  │
                    │   每日 08:30    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   告警判断      │
                    │   & 通知        │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   质量报告生成  │
                    │   每日 09:00    │
                    └─────────────────┘
```

### 6.3 完整检测任务示例（Airflow DAG）

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'email': ['data-quality@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_quality_daily_check',
    default_args=default_args,
    description='每日数据质量检测',
    schedule_interval='0 7 * * *',  # 每日7点执行
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'monitoring'],
) as dag:

    def run_completeness_check(**context):
        """完整性检测"""
        from data_quality.checkers import CompletenessChecker
        checker = CompletenessChecker()
        ds = context['ds']
        results = checker.run_all_rules(partition_date=ds)
        context['task_instance'].xcom_push(key='completeness_results', value=results)
        return results

    def run_accuracy_check(**context):
        """准确性检测"""
        from data_quality.checkers import AccuracyChecker
        checker = AccuracyChecker()
        ds = context['ds']
        results = checker.run_all_rules(partition_date=ds)
        context['task_instance'].xcom_push(key='accuracy_results', value=results)
        return results

    def run_consistency_check(**context):
        """一致性检测"""
        from data_quality.checkers import ConsistencyChecker
        checker = ConsistencyChecker()
        ds = context['ds']
        results = checker.run_all_rules(partition_date=ds)
        context['task_instance'].xcom_push(key='consistency_results', value=results)
        return results

    def run_timeliness_check(**context):
        """及时性检测"""
        from data_quality.checkers import TimelinessChecker
        checker = TimelinessChecker()
        ds = context['ds']
        results = checker.run_all_rules(partition_date=ds)
        context['task_instance'].xcom_push(key='timeliness_results', value=results)
        return results

    def aggregate_and_alert(**context):
        """汇总结果并触发告警"""
        from data_quality.alerting import AlertManager
        ti = context['task_instance']

        all_results = {
            'completeness': ti.xcom_pull(key='completeness_results', task_ids='completeness_check'),
            'accuracy': ti.xcom_pull(key='accuracy_results', task_ids='accuracy_check'),
            'consistency': ti.xcom_pull(key='consistency_results', task_ids='consistency_check'),
            'timeliness': ti.xcom_pull(key='timeliness_results', task_ids='timeliness_check'),
        }

        alert_manager = AlertManager()
        alert_manager.process_results(all_results)

    def generate_report(**context):
        """生成质量报告"""
        from data_quality.reporting import ReportGenerator
        ds = context['ds']
        generator = ReportGenerator()
        generator.generate_daily_report(partition_date=ds)

    completeness_check = PythonOperator(
        task_id='completeness_check',
        python_callable=run_completeness_check,
    )

    accuracy_check = PythonOperator(
        task_id='accuracy_check',
        python_callable=run_accuracy_check,
    )

    consistency_check = PythonOperator(
        task_id='consistency_check',
        python_callable=run_consistency_check,
    )

    timeliness_check = PythonOperator(
        task_id='timeliness_check',
        python_callable=run_timeliness_check,
    )

    alert_task = PythonOperator(
        task_id='aggregate_and_alert',
        python_callable=aggregate_and_alert,
    )

    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # 四个检测任务并行，然后汇总告警，最后生成报告
    [completeness_check, accuracy_check, consistency_check, timeliness_check] >> alert_task >> report_task
```

---

## 7. 监控指标与报表体系

### 7.1 核心质量指标

```
┌──────────────────────────────────────────────────────────────┐
│                    数据质量核心指标体系                        │
├──────────────────┬───────────────────────────────────────────┤
│  指标类别        │  具体指标                                  │
├──────────────────┼───────────────────────────────────────────┤
│                  │ · 字段空值率                               │
│  完整性指标      │ · 主键重复率                               │
│                  │ · 数据行数（vs 期望行数）                  │
│                  │ · 分区缺失率                               │
├──────────────────┼───────────────────────────────────────────┤
│                  │ · 格式错误率                               │
│  准确性指标      │ · 值域异常率                               │
│                  │ · 枚举值异常率                             │
│                  │ · 业务规则违反率                           │
├──────────────────┼───────────────────────────────────────────┤
│                  │ · 跨表一致性通过率                         │
│  一致性指标      │ · 跨系统对账差异率                         │
│                  │ · 历史数据变更率                           │
├──────────────────┼───────────────────────────────────────────┤
│                  │ · 数据到达延迟（分钟）                      │
│  及时性指标      │ · SLA达标率                                │
│                  │ · 消息队列积压量                           │
│                  │ · 数据新鲜度                               │
├──────────────────┼───────────────────────────────────────────┤
│                  │ · 综合质量评分（0-100）                    │
│  综合指标        │ · 告警数量趋势                             │
│                  │ · 修复及时率                               │
│                  │ · 规则覆盖率                               │
└──────────────────┴───────────────────────────────────────────┘
```

### 7.2 质量评分计算

```python
class DataQualityScorer:
    """数据质量综合评分器"""

    # 各维度权重（可根据业务调整）
    DIMENSION_WEIGHTS = {
        "completeness": 0.30,
        "accuracy": 0.35,
        "consistency": 0.20,
        "timeliness": 0.15
    }

    def calculate_table_score(self, table_name: str, check_results: dict) -> dict:
        """
        计算单表质量综合评分
        Args:
            table_name: 表名
            check_results: 各维度检测结果
        Returns:
            质量评分详情
        """
        dimension_scores = {}
        for dimension, weight in self.DIMENSION_WEIGHTS.items():
            results = check_results.get(dimension, [])
            if not results:
                continue

            passed = sum(1 for r in results if r.get("passed", True))
            total = len(results)
            # 各维度得分按通过率计算（失败的规则按严重程度扣分）
            base_score = (passed / total * 100) if total > 0 else 100
            # 严重级别惩罚
            penalty = self._calculate_penalty(results)
            dimension_scores[dimension] = max(0, base_score - penalty)

        if not dimension_scores:
            return {"table_name": table_name, "score": 100, "grade": "A"}

        total_score = sum(
            dimension_scores.get(dim, 100) * weight
            for dim, weight in self.DIMENSION_WEIGHTS.items()
        )

        return {
            "table_name": table_name,
            "total_score": round(total_score, 1),
            "grade": self._score_to_grade(total_score),
            "dimension_scores": {
                dim: round(score, 1)
                for dim, score in dimension_scores.items()
            }
        }

    def _calculate_penalty(self, results: list) -> float:
        """根据失败规则的严重程度计算扣分"""
        penalty = 0
        for r in results:
            if not r.get("passed", True):
                level = r.get("alert_level", "warning")
                penalty += {"critical": 20, "error": 10, "warning": 5, "info": 1}.get(level, 5)
        return min(penalty, 50)  # 最多扣50分

    def _score_to_grade(self, score: float) -> str:
        """评分转等级"""
        if score >= 95:
            return "A+"
        elif score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"
```

### 7.3 日报模板

```
╔══════════════════════════════════════════════════════════════╗
║             📊 数据质量日报 - 2024-04-20                      ║
╠══════════════════════════════════════════════════════════════╣
║  综合质量评分: 92.5 / 100    等级: A    趋势: ↑ +1.2        ║
╠══════════════════════════════════════════════════════════════╣
║  维度评分                                                    ║
║  ┌──────────────┬───────┬───────┬───────┬───────┐          ║
║  │  维度        │今日   │昨日   │7日均  │ 趋势  │          ║
║  ├──────────────┼───────┼───────┼───────┼───────┤          ║
║  │ 完整性       │ 95.2  │ 94.8  │ 95.0  │  ↑    │          ║
║  │ 准确性       │ 91.3  │ 91.5  │ 91.8  │  →    │          ║
║  │ 一致性       │ 93.7  │ 92.1  │ 92.5  │  ↑    │          ║
║  │ 及时性       │ 88.9  │ 90.2  │ 89.5  │  ↓    │          ║
║  └──────────────┴───────┴───────┴───────┴───────┘          ║
╠══════════════════════════════════════════════════════════════╣
║  告警统计                                                    ║
║  P0(紧急): 0    P1(严重): 2    P2(警告): 8    P3(提示): 15  ║
╠══════════════════════════════════════════════════════════════╣
║  待处理工单: 3    已关闭工单: 22    SLA达标率: 94.7%         ║
╠══════════════════════════════════════════════════════════════╣
║  Top 3 问题表:                                               ║
║  1. dws_user_behavior_1d  - 及时性: 延迟35分钟              ║
║  2. ads_gmv_by_category   - 准确性: 金额逻辑错误 2条         ║
║  3. dim_product           - 完整性: brand字段空值率 3.2%     ║
╚══════════════════════════════════════════════════════════════╝
```

---

## 8. 最佳实践与落地建议

### 8.1 规则分层管理

```
规则层次
│
├── 平台级规则（所有表通用）
│   ├── 主键唯一性检测
│   ├── 分区数据行数为0检测
│   └── 数据到达时间检测
│
├── 主题域级规则（同一业务域通用）
│   ├── 订单域：金额字段非负校验
│   ├── 用户域：手机号格式校验
│   └── 商品域：价格合理范围校验
│
└── 表级规则（特定表的业务规则）
    ├── fact_orders：付款时间≥下单时间
    ├── dim_user：年龄范围[0,150]
    └── fact_order_items：明细汇总=主表金额
```

### 8.2 数据质量SLA保障

| 数据层级 | SLA要求 | 监控频率 | 告警响应 |
|--------|---------|---------|---------|
| ODS（原始层） | T+1 08:00前就绪 | 每小时检测 | P1级，1小时内响应 |
| DWD（明细层） | T+1 09:00前就绪 | 每日检测 | P1级，1小时内响应 |
| DWS（汇总层） | T+1 10:00前就绪 | 每日检测 | P1级，1小时内响应 |
| ADS（应用层） | T+1 11:00前就绪 | 每日检测 | P2级，4小时内响应 |
| 实时数据 | 消费延迟<5分钟 | 每5分钟检测 | P0级，15分钟内响应 |

### 8.3 组织与协作

```
数据质量治理责任矩阵（RACI）

角色               | 规则定义 | 检测执行 | 告警处理 | 修复闭环 | 报告输出
──────────────────────────────────────────────────────────────
数据平台团队        |    C    |    R    |    I    |    C    |    R
数据开发（上游ETL） |    C    |    I    |    R    |    R    |    I
业务分析师          |    R    |    I    |    C    |    I    |    C
数据资产负责人      |    A    |    A    |    A    |    A    |    A
IT运维              |    I    |    C    |    C    |    C    |    I

R=执行负责  A=最终负责  C=被咨询  I=被告知
```

### 8.4 持续优化建议

1. **从高价值数据开始**：优先覆盖核心业务表（如订单表、用户表），确保ROI
2. **规则要合理**：阈值过严会导致告警泛滥，阈值过松会漏报，需结合历史数据校准
3. **建立基线**：新上线数据表先运行1-2周收集基线数据，再启用动态阈值告警
4. **定期复盘**：每月进行告警回顾，对误告警规则进行调整，提升规则精准度
5. **与数据血缘结合**：建立完善的数据血缘，便于快速定位根因和影响范围
6. **自动化修复优先**：对于可预期的错误类型（如ETL重跑），尽量实现自动修复
7. **质量度量文化**：将数据质量指标纳入团队KPI，提升全员质量意识

---

## 9. 总结

数据质量治理是一项系统工程，需要从技术、流程和组织三个维度共同发力。

### 关键要点回顾

```
┌─────────────────────────────────────────────────────────┐
│                    数据质量治理核心要素                    │
├─────────────┬───────────────────────────────────────────┤
│  四大维度   │ 完整性 → 准确性 → 一致性 → 及时性          │
├─────────────┼───────────────────────────────────────────┤
│  三层防护   │ 规则检测 → 异常告警 → 闭环修复             │
├─────────────┼───────────────────────────────────────────┤
│  两个核心   │ 主动发现（vs 被动响应）                    │
│             │ 闭环治理（vs 一次性修复）                  │
├─────────────┼───────────────────────────────────────────┤
│  一个目标   │ 持续提升数据质量，支撑业务决策可信度        │
└─────────────┴───────────────────────────────────────────┘
```

### 实施路径建议

**第一阶段（1-2个月）**：基础建设
- 建立数据质量平台基础框架
- 覆盖核心表的完整性和及时性检测
- 接通告警通知渠道

**第二阶段（3-4个月）**：规则扩展
- 补充准确性和一致性检测规则
- 建立工单和修复流程
- 上线质量评分和日报

**第三阶段（5-6个月）**：智能升级
- 引入动态阈值和机器学习异常检测
- 建立自动修复机制
- 构建数据血缘与根因分析能力

---

*本文档持续更新，如有问题请联系数据治理团队。*

*最后更新：2024-04-20*

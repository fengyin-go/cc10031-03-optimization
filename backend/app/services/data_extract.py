"""数据提取领域服务 — 消费源端配置 + 字段映射，执行数据采集。

数据工程核心：数据归档存储与快照管理 — 跨模块数据产物

数据流:
    channel("rpa.source_configured") ← page_model
    → data_extract 消费 → 执行采集 → CollectRecord + DirtyDataVault
    → channel("rpa.data_collected") → result_verify
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataExtractService(BaseModuleService):
    MODULE = "data_extract"
    ENTITY = "提取规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "源页面结构", "提取字段", "提取路径", "数据类型", "格式化", "去空策略"]
    REQUIRED_FIELDS = ["规则名称", "源页面结构", "提取字段"]
    STATUS_ORDER = ["草稿", "已测试", "已生效", "已过期"]
    ACTION_RULES = {"测试提取": "草稿", "启用规则": "已测试"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 (field_mapping + data_extractor 角色) ----

    def consume_source_and_extract(self) -> dict[str, Any]:
        """消费 page_model 发布的源端配置，执行数据采集模拟。

        从 channel("rpa.source_configured") 获取源端配置，
        根据提取规则模拟从目标站点采集数据。

        采集结果写入：
        - CollectRecord（通过 store.collect_records.add）
        - 脏数据写入 DirtyDataVault（解析失败/字段缺失）

        Returns:
            {"extracted": int, "dirty": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.source_configured",
        )
        if not msgs:
            return {"extracted": 0, "dirty": 0, "batches": []}

        batches = []
        for msg in msgs:
            source_config = msg.payload
            batch_ref = f"BATC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{msg.msg_id}"

            # 1. 获取提取规则（field_mapping 语义）
            extract_rules = self._get_active_rules_for_site(
                source_config.get("target_site", ""))

            # 2. 模拟采集数据
            extracted, dirty = self._simulate_collection(
                source_config, extract_rules, batch_ref)

            # 3. 写 channel
            result_payload = {
                "batch_ref": batch_ref,
                "model_id": source_config.get("model_id"),
                "site": source_config.get("target_site"),
                "extracted": len(extracted),
                "dirty": len(dirty),
                "extract_rules_applied": len(extract_rules),
                "sample_records": extracted[:3],
            }
            store.channel.publish(
                topic="rpa.data_collected",
                source=self.MODULE,
                payload=result_payload,
            )

            store.channel.confirm(msg.msg_id, self.MODULE)
            batches.append({
                "batch_ref": batch_ref,
                "site": source_config.get("target_site"),
                "extracted": len(extracted),
                "dirty": len(dirty),
            })

        return {
            "extracted": sum(b["extracted"] for b in batches),
            "dirty": sum(b["dirty"] for b in batches),
            "batches": batches,
        }

    def _get_active_rules_for_site(self, site: str) -> list[dict[str, Any]]:
        """获取目标站点关联的已生效提取规则（field_mapping 语义）。"""
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已生效"
        ]
        if site:
            active = [
                r for r in active
                if site[:2] in (r.get("源页面结构") or "")
                or r.get("源页面结构", "") == site
            ]
        # 如果没有关联规则，用默认规则
        if not active and rows:
            active = [r for r in rows if r.get("status") != "已过期"][:3]
        return active

    def _simulate_collection(
        self,
        source_config: dict[str, Any],
        rules: list[dict[str, Any]],
        batch_ref: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """模拟从目标站点采集数据。

        根据页面模型的字段推断和提取规则，生成模拟采集记录。
        部分记录会因字段缺失被标记为脏数据。

        Returns:
            (valid_records, dirty_records)
        """
        fields_to_extract = source_config.get("fields_to_extract",
                                               ["字段1", "字段2", "字段3"])
        # 根据规则补充映射字段
        for rule in rules:
            rule_field = rule.get("提取字段", "")
            if rule_field and rule_field not in fields_to_extract:
                fields_to_extract.append(rule_field)

        # 模拟采集 8-15 条记录
        record_count = random.randint(8, 15)
        valid_records = []
        dirty_records = []

        for i in range(record_count):
            record = self._generate_mock_record(source_config, fields_to_extract, i)

            # 质量检查：模拟约 15% 的脏数据
            if random.random() < 0.15:
                # 随机缺失字段或格式异常
                reasons = ["字段缺失", "格式非法", "解析超时", "编码错误"]
                reason = random.choice(reasons)
                if reason == "字段缺失":
                    del record[random.choice(list(record.keys()))]

                # 按记录键去重写入隔离区：同一批次重跑不会重复隔离；
                # 解析失败的脏数据使用独立键空间，避免与采集记录键碰撞
                store.dirty_vault.quarantine(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    raw_data=dict(record),
                    reason=reason,
                    record_key=f"{batch_ref}#dirty-{record.get('_seq', i + 1)}",
                )
                dirty_records.append(record)
            else:
                # 正常数据：写入采集记录
                store.collect_records.add(
                    batch_ref=batch_ref,
                    source=self.MODULE,
                    fields=record,
                )
                valid_records.append(record)

        return valid_records, dirty_records

    def _generate_mock_record(
        self,
        config: dict[str, Any],
        fields: list[str],
        index: int,
    ) -> dict[str, Any]:
        """生成模拟采集记录（基于真实的数据工程场景）。"""
        site = config.get("target_site", "unknown")
        record: dict[str, Any] = {"_seq": index + 1}

        sample_templates = {
            "序号": lambda i: i + 1,
            "名称": lambda i: f"{site}-产品-{1000 + i}",
            "标题": lambda i: f"关于{site}的第{i + 1}期报告",
            "状态": lambda i: random.choice(["正常", "异常", "待审核"]),
            "金额": lambda i: round(random.uniform(100, 99999), 2),
            "日期": lambda i: f"2026-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "链接": lambda i: f"https://{site}.example.com/item/{i + 1}",
            "摘要": lambda i: f"{site}数据采集摘要 #{i + 1}",
            "时间": lambda i: datetime.now(timezone.utc).isoformat(),
            "字段名": lambda i: f"field_{i + 1}",
            "字段值": lambda i: f"value_{random.randint(1, 100)}",
            "内容": lambda i: f"{site}采集内容行 {i + 1}",
            "标签": lambda i: random.choice(["采集", "RPA", "自动化"]),
            "发布日期": lambda i: f"2026-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "类型": lambda i: random.choice(["文本", "数值", "日期"]),
            "必填": lambda i: random.choice(["是", "否"]),
            "数据项1": lambda i: f"data1_{i}",
            "数据项2": lambda i: f"data2_{i}",
            "数据项3": lambda i: f"data3_{i}",
        }

        for field in fields:
            if field in sample_templates:
                record[field] = sample_templates[field](index)
            else:
                record[field] = f"{field}-{index + 1}"

        return record

    def execute_mapping_and_extract(
        self,
        model_id: int,
        target_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """直接执行字段映射 + 数据提取（不通过 channel 的简化版）。

        用于直接调用场景：已知 page_model 和规则，立即执行采集。

        Args:
            model_id: 页面模型 ID
            target_fields: 额外目标字段（可选）

        Returns:
            采集结果概要
        """
        model_entry = store.find("page_model", model_id)
        if model_entry is None:
            return {"error": f"页面模型 {model_id} 不存在"}

        source_config = {
            "model_id": model_id,
            "target_site": model_entry.get("目标站点", "unknown"),
            "fields_to_extract": target_fields or ["名称", "状态", "金额", "日期"],
        }
        rules = self._get_active_rules_for_site(source_config["target_site"])
        batch_ref = f"DIRECT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        valid, dirty = self._simulate_collection(source_config, rules, batch_ref)

        return {
            "batch_ref": batch_ref,
            "model_id": model_id,
            "extracted": len(valid),
            "dirty": len(dirty),
            "sample": valid[:3],
        }

    def get_collection_stats(self) -> dict[str, Any]:
        """获取采集统计概览。"""
        collect_snapshot = store.collect_records.snapshot()
        dirty_snapshot = store.dirty_vault.snapshot()
        return {
            "collect_records": collect_snapshot,
            "dirty_vault": dirty_snapshot,
            "collection_events": len(
                store.channel.snapshot(topic="rpa.data_collected")),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_collection_stats()
        return {**base, **stats}
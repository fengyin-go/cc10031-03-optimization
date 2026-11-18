"""采集质量谱系与隔离恢复稳定性测试。

覆盖验收标准：
1. 失败记录重跑不重复写入隔离区（状态与隔离回执同步推进）；
2. 恢复后的记录键不再次进入同一隔离批次，不重复累计同一隔离原因；
3. 批量恢复只更新当前状态记录，并发两次提交只算一次；
4. 更正重建 / 追加重算事件两套边界都只保留一条有效隔离原因；
5. 谱系数据集去重且脱敏；质量汇总按恢复状态区分当前问题与历史隔离；
6. 按记录键可查看当前隔离原因、恢复时间和历史重算记录，保留隔离原因明细。
"""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.result_verify import ResultVerifyService
from app.services.report_dashboard import ReportDashboardService

RULE_ID = 99001
_seq = [0]


@pytest.fixture()
def verify_env():
    """隔离校验规则现场：停用既有规则，启用一条确定性规则，用后恢复。"""
    rows = store.rows("result_verify")
    saved = [(r, r.get("status")) for r in rows]
    for r in rows:
        r["status"] = "已关闭"
    rows.append({
        "id": RULE_ID, "status": "已启用", "pending": True, "abnormal": False,
        "规则名称": "名称非空-谱系测试", "校验字段": "名称", "校验条件": "non_empty",
        "期望模式": "non_empty", "异常动作": "quarantine", "抽样比例": "1.0",
        "关联流程": "lineage-test",
    })
    yield
    rows[:] = [r for r in rows if r.get("id") != RULE_ID]
    for r, status in saved:
        r["status"] = status


def _new_batch() -> str:
    _seq[0] += 1
    return f"QL-TEST-{_seq[0]:04d}"


def _seed_batch(batch_ref: str) -> dict[str, int]:
    """一批三条：一条可通过、两条必失败（名称为空）。返回记录键集合。"""
    keys = {}
    for name, fields in (
        ("pass", {"序号": 1, "备注": "无规则字段"}),
        ("fail_a", {"序号": 2, "名称": ""}),
        ("fail_b", {"序号": 3, "名称": ""}),
    ):
        entry = store.collect_records.add(batch_ref, "data_extract", fields)
        keys[name] = entry.record_key
    store.channel.publish("rpa.data_collected", "data_extract",
                          {"batch_ref": batch_ref, "site": "lineage-test"})
    return keys


def _run_verify(batch_ref: str, rounds: int = 1):
    svc = ResultVerifyService()
    result = None
    for _ in range(rounds):
        result = svc.verify_collected_data()
        store.channel.publish("rpa.data_collected", "data_extract",
                              {"batch_ref": batch_ref, "site": "lineage-test"})
    return result


class TestRerunDedup:
    """失败记录重跑不重复隔离，状态与隔离回执同步推进。"""

    def test_failed_records_not_reconsumed_on_rerun(self, verify_env):
        batch = _new_batch()
        _seed_batch(batch)
        svc = ResultVerifyService()

        first = svc.verify_collected_data()
        assert first["failed"] == 2 and first["passed"] == 1
        vault_after_first = store.dirty_vault.count

        # 同一批次重跑：失败项不应再被消费，隔离区不增长
        store.channel.publish("rpa.data_collected", "data_extract",
                              {"batch_ref": batch, "site": "lineage-test"})
        second = svc.verify_collected_data()
        assert second["failed"] == 0
        assert store.dirty_vault.count == vault_after_first

    def test_failure_state_advances_with_receipt(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        ResultVerifyService().verify_collected_data()

        states = {
            e.record_key: e.process_state
            for e in store.collect_records.list_by_batch(batch)
        }
        assert states[keys["pass"]] == "已通过"
        assert states[keys["fail_a"]] == "已隔离"
        assert states[keys["fail_b"]] == "已隔离"
        # 待校验记录清零：下一轮消费拿不到同一失败项
        assert store.collect_records.list_unvalidated(batch) == []

    def test_same_reason_not_accumulated_after_recovery(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()

        before = store.dirty_vault.snapshot()["by_reason"]
        svc.recover_records(record_keys=[keys["fail_a"]])

        # 恢复后重跑同一批次：记录键被拦截，同一隔离原因不重复累计
        store.channel.publish("rpa.data_collected", "data_extract",
                              {"batch_ref": batch, "site": "lineage-test"})
        svc.verify_collected_data()
        after = store.dirty_vault.snapshot()["by_reason"]
        assert after == before

        # 底层同样拦截：已恢复记录键直接进入同一隔离批次被拒绝
        receipt = store.dirty_vault.quarantine(
            "result_verify", batch, {"序号": 2, "名称": ""},
            "字段[名称]为空", record_key=keys["fail_a"])
        assert receipt.status == "blocked_recovered"


class TestRecoveryIdempotency:
    """恢复幂等：并发两次提交只算一次；批量只更新当前状态记录。"""

    def test_double_recover_counts_once(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()

        first = svc.recover_records(record_keys=[keys["fail_a"], keys["fail_b"]])
        assert first["recovered"] == 2
        assert first["collect_synced"] == 2

        # 并发重复提交：全部跳过，不重复累计
        second = svc.recover_records(record_keys=[keys["fail_a"], keys["fail_b"]])
        assert second["recovered"] == 0
        assert len(second["skipped"]) == 2
        # 本批次两个记录键均无当前有效隔离
        effective = [r for key in (keys["fail_a"], keys["fail_b"])
                     for r in store.dirty_vault.find_by_key(key) if r.effective]
        assert effective == []

    def test_batch_recover_only_touches_current(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()
        svc.recover_records(record_keys=[keys["fail_a"]])

        # fail_a 已恢复（历史），fail_b 仍隔离中（当前）：批量只更新当前状态
        result = svc.recover_records(
            record_keys=[keys["fail_a"], keys["fail_b"]])
        assert result["recovered"] == 1
        assert {s["status"] for s in result["skipped"]} == {"already_recovered"}

        states = {
            e.record_key: e.process_state
            for e in store.collect_records.list_by_batch(batch)
        }
        assert states[keys["fail_a"]] == "已恢复"
        assert states[keys["fail_b"]] == "已恢复"


class TestCorrectionBoundaries:
    """更正重建 / 追加重算事件：两套边界都只保留一条有效隔离原因。"""

    def test_rebuild_keeps_single_effective_reason(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()

        # 更正后仍失败：旧记录转历史，只新建一条有效隔离
        result = svc.correct_record(
            keys["fail_a"], {"序号": 2, "名称": ""}, mode="rebuild")
        assert result["receipt"] == "created"
        effective = [r for r in store.dirty_vault.find_by_key(keys["fail_a"])
                     if r.effective]
        assert len(effective) == 1
        assert "字段[名称]为空" in effective[0].reason

        # 再次更正且通过：有效隔离清零，历史保留
        cleared = svc.correct_record(
            keys["fail_a"], {"序号": 2, "名称": "已更正"}, mode="rebuild")
        assert cleared["receipt"] == "cleared"
        effective = [r for r in store.dirty_vault.find_by_key(keys["fail_a"])
                     if r.effective]
        assert effective == []
        history = store.dirty_vault.find_by_key(keys["fail_a"])
        assert len(history) >= 2  # 历史隔离记录保留

    def test_recalc_event_keeps_single_effective_reason(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()

        before = store.dirty_vault.count
        result = svc.correct_record(
            keys["fail_b"], {"序号": 3, "名称": "更正中"}, mode="recalc_event")
        assert result["ok"] is True
        # 不新增隔离记录，有效原因仍只有一条
        assert store.dirty_vault.count == before
        effective = [r for r in store.dirty_vault.find_by_key(keys["fail_b"])
                     if r.effective]
        assert len(effective) == 1
        # 重算事件已登记且延迟发布汇总
        events = store.dirty_vault.recalc_history(keys["fail_b"])
        assert len(events) == 1
        assert events[0]["summary_deferred"] is True


class TestQualityLineage:
    """谱系数据集：去重、脱敏、按记录键可查。"""

    def test_lineage_deduplicated_and_desensitized(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()
        # 重跑一轮后再取谱系：每键仍只有一行
        store.channel.publish("rpa.data_collected", "data_extract",
                              {"batch_ref": batch, "site": "lineage-test"})
        svc.verify_collected_data()

        lineage = svc.quality_lineage(batch_ref=batch)
        lineage_keys = [row["record_key"] for row in lineage]
        assert sorted(lineage_keys) == sorted(keys.values())
        assert len(set(lineage_keys)) == len(lineage_keys)

        # 脱敏：谱系中不出现原始字段值
        blob = json.dumps(lineage, ensure_ascii=False)
        assert "无规则字段" not in blob
        for row in lineage:
            for masked in row["fields_masked"].values():
                assert ":" in masked  # 只保留 类型:长度 指纹

    def test_record_history_view(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()
        svc.correct_record(keys["fail_a"], {"序号": 2, "名称": ""},
                           mode="recalc_event")
        svc.recover_records(record_keys=[keys["fail_a"]])

        view = store.dirty_vault.record_history(keys["fail_a"])
        assert view["record_key"] == keys["fail_a"]
        assert view["current_status"] == "已恢复"
        assert view["current_reason"] is None  # 已恢复无当前隔离原因
        assert view["recovered_at"] is not None  # 恢复时间可查
        assert len(view["recalc_events"]) == 1  # 历史重算记录可查
        assert len(view["history"]) == 1  # 该键只有一条隔离记录（未重复）


class TestQualitySummary:
    """质量汇总：按恢复状态区分当前问题与历史隔离，保留原因明细。"""

    def test_summary_distinguishes_current_and_historical(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()
        svc.recover_records(record_keys=[keys["fail_a"]])

        summary = ReportDashboardService().quality_summary()
        assert summary["reconciled"] is True
        assert summary["current_issues"] >= 1  # fail_b 仍隔离中
        assert summary["historical_quarantine"] >= 1  # fail_a 已恢复转历史
        assert summary["recovered"] >= 1

    def test_reason_details_kept(self, verify_env):
        batch = _new_batch()
        keys = _seed_batch(batch)
        svc = ResultVerifyService()
        svc.verify_collected_data()
        svc.recover_records(record_keys=[keys["fail_a"]])

        details = store.dirty_vault.snapshot()["reason_details"]
        assert details, "隔离原因明细应保留"
        target = next(d for d in details if "字段[名称]为空" in d["reason"])
        # 明细保留当前与历史两个维度；本批次：fail_b 当前、fail_a 历史
        assert target["current"] >= 1
        assert target["historical"] >= 1
        assert keys["fail_b"] in target["record_keys"]
        assert keys["fail_a"] not in target["record_keys"]
        # 本批次口径核对：当前 1 条、历史 1 条
        cur = sum(1 for k in keys.values()
                  for r in store.dirty_vault.find_by_key(k) if r.effective)
        hist = sum(1 for k in keys.values()
                   for r in store.dirty_vault.find_by_key(k) if not r.effective)
        assert (cur, hist) == (1, 1)

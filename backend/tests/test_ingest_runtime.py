"""摄入空区间证明运行时场景测试（标准库 unittest）。

覆盖题述三个值班场景：
1. p5 的 100..199 完整读取无行：证明可查、确认只一次、续读位置正确；
2. p5 的 100..199 读取中途断线：未读历史范围保留、失败读段不能确认；
3. 完整空读取确认前送入偏移 140 的迟到记录：证明失效、回执与落湖去向可查、
   下一次读取回到窗口起点；并验证并发写入幂等与确认隔离。
"""
from __future__ import annotations

import sys
import unittest

sys.path.insert(0, '.')

from app.ingest_runtime import IngestRuntime

SOURCE = "order-events"
PARTITION = "p5"


class IngestScenarioTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.rt = IngestRuntime()

    # ------------------------------------------------------------------
    # 场景一：完整空读取 100..199
    # ------------------------------------------------------------------
    def test_full_empty_read_then_confirm_once(self):
        receipt = self.rt.scan(SOURCE, PARTITION, 100, 200, "empty")
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["read_result"], "empty_ok")
        self.assertEqual(receipt["read_rows"], 0)
        self.assertEqual(
            receipt["covered_partition_data_range"], "p5:[100,200)")
        proof_id = receipt["proof_id"]

        # 值班员回查原扫描与证明：实际覆盖分区、偏移范围、读操作结果齐全。
        scan = self.rt.get_scan(receipt["scan_id"])
        self.assertEqual(scan["read_result"], "empty_ok")
        proof = self.rt.get_proof(proof_id)
        self.assertEqual(proof["covered_start_offset"], 100)
        self.assertEqual(proof["covered_end_offset"], 200)
        self.assertEqual(proof["partition"], "p5")
        self.assertEqual(proof["read_result"], "empty_ok")

        # 确认前当前可续读位置：窗口已扫描但未确认，仍作为检查点待办保留。
        resume_before = self.rt.resume_view(SOURCE, PARTITION)
        self.assertIsNone(resume_before["confirmed_end_offset"])
        self.assertEqual(resume_before["pending_ranges"],
                         [{"start_offset": 100, "end_offset": 200}])
        self.assertEqual(resume_before["next_read_mode"], "retry_pending")
        self.assertEqual(resume_before["next_read_start_offset"], 100)

        # 检查点读取空区间证明并确认，待办随读段确认一并更新。
        confirm = self.rt.confirm_empty(proof_id)
        self.assertTrue(confirm["ok"])
        self.assertEqual(confirm["proof_status"], "已确认")
        # 窗口 [100,200) 已确认，待办清空；因偏移 0 起的前缀未在本运行时登记，
        # 严格连续水位仍为 None，但下一次读取起点按已确认区间末端 200 续读。
        self.assertEqual(confirm["resume"]["pending_ranges"], [])
        self.assertIsNone(confirm["resume"]["confirmed_end_offset"])

        # 同一来源同一区间并发/重复确认只能确认一次。
        again = self.rt.confirm_empty(proof_id)
        self.assertFalse(again["ok"])
        self.assertEqual(again["proof_status"], "已确认")
        self.assertEqual(len(self.rt.list_acks(SOURCE, PARTITION)), 1)

        # 水位确认后，流末端推进到 300，续读从 200 开始。
        self.rt._checkpoint(SOURCE, PARTITION)["stream_end_offset"] = 300
        resume_after = self.rt.resume_view(SOURCE, PARTITION)
        self.assertEqual(resume_after["next_read_mode"], "continuous")
        self.assertEqual(resume_after["next_read_start_offset"], 200)
        self.assertEqual(resume_after["next_read_data_range"], "[200,300)")

    # ------------------------------------------------------------------
    # 场景二：读取中途断线
    # ------------------------------------------------------------------
    def test_disconnect_retains_unread_history_and_blocks_ack(self):
        # 断线发生在读完 100..129（30 行）之后，130..199 未读。
        receipt = self.rt.scan(
            SOURCE, PARTITION, 100, 200, "disconnect",
            last_read_offset=129, partial_offsets=list(range(100, 130)),
        )
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["read_result"], "read_failed")
        # 断线后的优先续读范围是未读尾部 [130,200)。
        self.assertEqual(
            receipt["retained_unread_ranges"],
            [{"start_offset": 130, "end_offset": 200}],
        )
        # 断线前已读的 30 行已落湖，可按偏移回查。
        self.assertEqual(len(receipt["landed_before_failure"]), 30)
        trace_110 = self.rt.trace_offset(SOURCE, PARTITION, 110)
        self.assertTrue(trace_110["found"])
        self.assertIn("offset=110", trace_110["landing"]["lake_location"])

        # 失败读段未确认任何偏移：整窗保留为检查点待办；断线重试游标让下一次
        # 实际读取从未读尾部起点 130 开始，未读历史数据范围不丢失。
        resume = self.rt.resume_view(SOURCE, PARTITION)
        self.assertEqual(resume["pending_ranges"],
                         [{"start_offset": 100, "end_offset": 200}])
        self.assertEqual(resume["next_read_mode"], "retry_pending")
        self.assertEqual(resume["next_read_start_offset"], 130)
        self.assertEqual(resume["retry_cursor"], 130)
        self.assertIn("[130,200)", resume["next_read_data_range"])
        self.assertIsNone(resume["confirmed_end_offset"])

        # 失败读段不允许确认，且失败 ack 键与零行 ack 键物理隔离。
        blocked = self.rt.confirm_segment(receipt["segment_id"])
        self.assertFalse(blocked["ok"])
        self.assertTrue(blocked["ack_key"].startswith("fail:"))
        self.assertEqual(self.rt.list_acks(SOURCE, PARTITION), [])

        # 重试未读区间 [130,200) 且完整读全（无行），生成空区间证明，
        # 其确认键与失败读段的确认键物理隔离。
        retry = self.rt.scan(SOURCE, PARTITION, 130, 200, "empty")
        self.assertTrue(retry["ok"])
        self.assertTrue(retry["ack_key"].startswith("empty:"))
        self.assertNotEqual(retry["ack_key"], blocked["ack_key"])
        confirm_tail = self.rt.confirm_empty(retry["proof_id"])
        self.assertTrue(confirm_tail["ok"])
        # 尾部确认后，断线前未确认的 [100,130) 仍作为历史待办保留。
        self.assertEqual(
            confirm_tail["resume"]["pending_ranges"],
            [{"start_offset": 100, "end_offset": 130}],
        )
        self.assertEqual(confirm_tail["resume"]["next_read_start_offset"], 100)
        self.assertIsNone(confirm_tail["resume"]["confirmed_end_offset"])

        # 补扫断线前区间 [100,130)：其中 30 行已落湖，重扫为 rows 模式，
        # 已存在的落湖记录被幂等抑制；确认后检查点待办清空，整窗确认完毕。
        head = self.rt.scan(
            SOURCE, PARTITION, 100, 130, "rows", partial_offsets=list(range(100, 130)))
        self.assertTrue(all(row["duplicated"] for row in head["landed"]))
        confirm_head = self.rt.confirm_segment(head["segment_id"])
        self.assertTrue(confirm_head["ok"])
        self.assertEqual(confirm_head["resume"]["pending_ranges"], [])

    def test_disconnect_before_any_row_keeps_full_window(self):
        # 完全没读到行：last_read_offset=None，整窗保留。
        receipt = self.rt.scan(
            SOURCE, PARTITION, 100, 200, "disconnect",
            last_read_offset=None, partial_offsets=[],
        )
        self.assertEqual(
            receipt["retained_unread_ranges"],
            [{"start_offset": 100, "end_offset": 200}],
        )
        resume = self.rt.resume_view(SOURCE, PARTITION)
        self.assertEqual(resume["next_read_start_offset"], 100)

    # ------------------------------------------------------------------
    # 场景三：空窗口确认前收到偏移 140 的迟到记录
    # ------------------------------------------------------------------
    def test_late_row_before_confirm_invalidates_proof(self):
        # 同一空窗口先返回零行。
        scan = self.rt.scan(SOURCE, PARTITION, 100, 200, "empty")
        proof_id = scan["proof_id"]
        self.assertEqual(self.rt.get_proof(proof_id)["status"], "待确认")

        # 检查点确认前，迟到行 140 到达，幂等落湖。
        late = self.rt.land_late_row(
            SOURCE, PARTITION, 140, proof_id=proof_id, payload={"v": "late"})
        self.assertTrue(late["ok"])
        self.assertFalse(late["duplicated"])
        self.assertEqual(late["proof_status"], "已失效")

        # 140 的实际去向可查：落湖位置、状态、幂等键。
        trace = self.rt.trace_offset(SOURCE, PARTITION, 140)
        self.assertTrue(trace["found"])
        self.assertEqual(trace["landing"]["landing_status"], "已入湖")
        self.assertEqual(
            trace["landing"]["idempotency_key"], f"{SOURCE}|{PARTITION}|140")
        self.assertEqual(trace["landing"]["lake_location"],
                         f"lake://{SOURCE}/{PARTITION}/offset=140")

        # 原证明已失效，确认被拒绝。
        confirm = self.rt.confirm_empty(proof_id)
        self.assertFalse(confirm["ok"])
        self.assertEqual(confirm["proof_status"], "已失效")

        # 窗口整体回到待办，下一次实际读取从窗口起点 100 重新扫描。
        resume = late["resume"]
        self.assertEqual(resume["pending_ranges"],
                         [{"start_offset": 100, "end_offset": 200}])
        self.assertEqual(resume["next_read_mode"], "retry_pending")
        self.assertEqual(resume["next_read_start_offset"], 100)
        self.assertEqual(resume["next_read_data_range"], "[100,200)")

        # 重扫窗口（rows 模式），已落库的 140 被幂等抑制，不会重复入湖。
        rescan = self.rt.scan(
            SOURCE, PARTITION, 100, 200, "rows", partial_offsets=[140])
        self.assertEqual(len(rescan["landed"]), 1)
        self.assertTrue(rescan["landed"][0]["duplicated"])
        ledger = self.rt.list_landing(SOURCE, PARTITION)
        self.assertEqual(len(ledger), 1)

        # 直接并发再写一次 140，同样只保留一条落湖记录。
        concurrent = self.rt.land_late_row(SOURCE, PARTITION, 140)
        self.assertTrue(concurrent["duplicated"])
        self.assertEqual(len(self.rt.list_landing(SOURCE, PARTITION)), 1)

    def test_late_row_auto_links_pending_proof_without_proof_id(self):
        scan = self.rt.scan(SOURCE, PARTITION, 100, 200, "empty")
        late = self.rt.land_late_row(SOURCE, PARTITION, 150)
        self.assertTrue(late["ok"])
        self.assertEqual(late["proof_id"], scan["proof_id"])
        self.assertEqual(late["proof_status"], "已失效")

    def test_confirmed_proof_keeps_late_row_outside_window(self):
        """已确认空窗口之外的迟到行不影响证明，只正常落湖。"""
        scan = self.rt.scan(SOURCE, PARTITION, 100, 200, "empty")
        self.rt.confirm_empty(scan["proof_id"])
        late = self.rt.land_late_row(SOURCE, PARTITION, 240)
        self.assertTrue(late["ok"])
        self.assertNotIn("proof_status", late)
        self.assertEqual(self.rt.get_proof(scan["proof_id"])["status"], "已确认")

    def test_ack_kinds_are_separate_records(self):
        """零行确认与失败确认在台账中是不同命名空间的键。"""
        empty = self.rt.scan(SOURCE, PARTITION, 100, 200, "empty")
        failed = self.rt.scan(
            SOURCE, PARTITION, 100, 200, "disconnect", last_read_offset=160)
        self.rt.confirm_empty(empty["proof_id"])
        self.rt.confirm_segment(failed["segment_id"])
        acks = self.rt.list_acks(SOURCE, PARTITION)
        self.assertEqual(len(acks), 1)  # 失败读段被拒绝，仅空证明一条确认
        self.assertEqual(acks[0]["ack_kind"], "empty")


if __name__ == "__main__":
    unittest.main()

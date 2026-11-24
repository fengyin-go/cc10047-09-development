"""摄入空区间证明计算引擎测试（标准库 unittest，无需 pytest）。"""
from __future__ import annotations

import sys
import unittest

sys.path.insert(0, '.')

from app.compute import (
    build_checkpoint_resume,
    build_empty_interval_proof,
    build_read_segment,
    plan_next_read,
    read_ack_key,
    retain_unread_ranges_on_failure,
)


class TestReadSegment(unittest.TestCase):
    def test_empty_segment(self):
        seg = build_read_segment("src", "p5", 100, 200, read_rows=0, read_result="empty_ok")
        self.assertEqual(seg["read_result"], "empty_ok")
        self.assertIsNone(seg["last_read_offset"])

    def test_failed_segment_out_of_range_offset_rejected(self):
        # 一行未读到时 last_read_offset 允许为空；落在区间外的偏移必须拒绝。
        seg = build_read_segment(
            "src", "p5", 100, 200, read_rows=0,
            read_result="read_failed", last_read_offset=None,
        )
        self.assertIsNone(seg["last_read_offset"])
        with self.assertRaises(ValueError):
            build_read_segment(
                "src", "p5", 100, 200, read_rows=0,
                read_result="read_failed", last_read_offset=250,
            )

    def test_empty_segment_cannot_carry_rows(self):
        with self.assertRaises(ValueError):
            build_read_segment("src", "p5", 100, 200, read_rows=1, read_result="empty_ok")


class TestAckKeyIsolation(unittest.TestCase):
    def test_empty_and_failure_never_share_ack(self):
        """零行读完与读取失败即使区间完全相同，也不能共用同一条确认。"""
        empty_seg = build_read_segment("src", "p5", 100, 200, read_rows=0, read_result="empty_ok")
        failed_seg = build_read_segment(
            "src", "p5", 100, 200, read_rows=0,
            read_result="read_failed", last_read_offset=150,
        )
        empty_key = read_ack_key(empty_seg)
        failed_key = read_ack_key(failed_seg)
        self.assertTrue(empty_key.startswith("empty:"))
        self.assertTrue(failed_key.startswith("fail:"))
        self.assertNotEqual(empty_key, failed_key)

    def test_same_segment_same_key_for_idempotent_confirm(self):
        seg1 = build_read_segment("src", "p5", 100, 200, read_rows=0, read_result="empty_ok")
        seg2 = build_read_segment("src", "p5", 100, 200, read_rows=0, read_result="empty_ok")
        self.assertEqual(read_ack_key(seg1), read_ack_key(seg2))


class TestEmptyIntervalProof(unittest.TestCase):
    def test_proof_carries_partition_range_and_result(self):
        proof = build_empty_interval_proof(
            "src", "p5", 100, 200, proof_id="proof-1", scan_id="scan-1", read_at="t",
        )
        self.assertEqual(proof["covered_partition_data_range"], "p5:[100,200)")
        self.assertEqual(proof["read_result"], "empty_ok")
        self.assertEqual(proof["read_rows"], 0)
        self.assertEqual(proof["status"], "待确认")


class TestRetainUnreadRanges(unittest.TestCase):
    def test_partial_failure_keeps_history_tail(self):
        """断线在 129，未读历史范围 [130,200) 必须保留。"""
        ranges = retain_unread_ranges_on_failure(100, 200, 129)
        self.assertEqual(ranges, [{"start_offset": 130, "end_offset": 200}])

    def test_failure_before_any_row_keeps_full_range(self):
        ranges = retain_unread_ranges_on_failure(100, 200, None)
        self.assertEqual(ranges, [{"start_offset": 100, "end_offset": 200}])


class TestNextReadPlanner(unittest.TestCase):
    def test_pending_history_reads_first(self):
        plan = plan_next_read(
            confirmed_end_offset=200,
            pending_ranges=[{"start_offset": 130, "end_offset": 200}],
            stream_end_offset=300,
        )
        self.assertEqual(plan["mode"], "retry_pending")
        self.assertEqual(plan["start_offset"], 130)
        self.assertEqual(plan["data_range"], "[130,200)")

    def test_continuous_read_after_watermark(self):
        plan = plan_next_read(
            confirmed_end_offset=200, pending_ranges=[], stream_end_offset=300,
        )
        self.assertEqual(plan["mode"], "continuous")
        self.assertEqual(plan["start_offset"], 200)
        self.assertEqual(plan["end_offset"], 300)

    def test_checkpoint_resume_shape(self):
        view = build_checkpoint_resume(
            source="src", partition="p5",
            confirmed_end_offset=None,
            pending_ranges=[{"start_offset": 100, "end_offset": 200}],
            stream_end_offset=200,
        )
        self.assertEqual(view["next_read_partition"], "p5")
        self.assertEqual(view["next_read_start_offset"], 100)
        self.assertEqual(view["next_read_data_range"], "[100,200)")
        self.assertEqual(view["next_read_mode"], "retry_pending")


if __name__ == "__main__":
    unittest.main()

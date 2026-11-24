"""检查点服务读取摄入空区间证明的单元测试。"""
from __future__ import annotations

import sys
import unittest

sys.path.insert(0, '.')

from app.ingest_runtime import ingest_runtime
from app.services import service_for


class TestCheckpointProofHook(unittest.TestCase):
    def setUp(self) -> None:
        ingest_runtime.reset()

    def test_checkpoint_loads_proofs_and_resume_view(self):
        svc = service_for("stream_checkpoint")
        # 空扫描后确认前：证明可被检查点读取，待办包含整个窗口。
        scan = ingest_runtime.scan("src-a", "p5", 100, 200, "empty")
        view = svc.domain_load_ingest_proofs("src-a", "p5")
        self.assertTrue(view["ok"])
        self.assertEqual(len(view["proofs"]), 1)
        self.assertEqual(view["proofs"][0]["proof_id"], scan["proof_id"])
        self.assertEqual(view["pending_ranges"],
                         [{"start_offset": 100, "end_offset": 200}])
        self.assertEqual(view["next_read_partition"], "p5")
        self.assertEqual(view["next_read_start_offset"], 100)
        self.assertEqual(view["next_read_mode"], "retry_pending")

        # 确认后检查点视图随读段确认更新，待办清空。
        ingest_runtime.confirm_empty(scan["proof_id"])
        view2 = svc.domain_load_ingest_proofs("src-a", "p5")
        self.assertEqual(view2["pending_ranges"], [])
        self.assertEqual(len(view2["acks"]), 1)
        self.assertEqual(view2["next_read_start_offset"], 200)

    def test_checkpoint_empty_partition(self):
        svc = service_for("stream_checkpoint")
        view = svc.domain_load_ingest_proofs("unknown", "p9")
        self.assertTrue(view["ok"])
        self.assertEqual(view["proofs"], [])
        self.assertEqual(view["acks"], [])


if __name__ == "__main__":
    unittest.main()

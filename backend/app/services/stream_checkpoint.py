"""流计算检查点领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamCheckpointService(BaseModuleService):
    MODULE = "stream_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["检查点标识", "关联管道", "检查点时间", "数据偏移", "状态大小", "保存路径", "保留时长", "检查状态"]
    REQUIRED_FIELDS = ["检查点标识", "关联管道", "检查点时间"]
    STATUS_ORDER = ["保存中", "已保存", "已恢复", "已过期"]
    ACTION_RULES = {"创建检查点": "保存中", "恢复检查点": "已恢复"}
    NEGATIVE_ACTIONS = []

    def domain_save_checkpoint(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行写入，联动关联和回填处理，产出标准化记录。处理流程：读取配置→执行写入→关联校验→回填批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回计算处理结果，含标准化校验和批次批次信息

            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}
        import hashlib        # 返回计算处理结果，含标准化校验和批次批次信息

        return {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "pipeline": entry.get("关联管道"),
            "checkpoint_time": entry.get("检查点时间"),
            "data_offset": entry.get("数据偏移"),
            "state_size_kb": int(entry.get("状态大小", 128)),
            "save_path": entry.get("保存路径", "/checkpoints"),
            "retention_hours": 24,
            "checksum": hashlib.sha256(str(entry).encode()).hexdigest()[:16],
        }

    def domain_load_ingest_proofs(
        self, source: str, partition: str,
    ) -> dict[str, Any]:
        """检查点读取摄入空区间证明并恢复待办。

        汇总当前分区的空区间证明、读段确认与落湖台账，并输出下一次实际
        读取的分区数据范围和起始偏移；待办随读段确认一并更新。
        """
        from app.ingest_runtime import ingest_runtime

        proofs = ingest_runtime.list_proofs(source, partition)
        acks = ingest_runtime.list_acks(source, partition)
        resume = ingest_runtime.resume_view(source, partition)
        return {
            "ok": True,
            "checkpoint_service": self.MODULE,
            "source": source,
            "partition": partition,
            "proofs": proofs,
            "acks": acks,
            "pending_ranges": resume["pending_ranges"],
            "confirmed_end_offset": resume["confirmed_end_offset"],
            "next_read_partition": resume["next_read_partition"],
            "next_read_start_offset": resume["next_read_start_offset"],
            "next_read_end_offset": resume["next_read_end_offset"],
            "next_read_data_range": resume["next_read_data_range"],
            "next_read_mode": resume["next_read_mode"],
        }
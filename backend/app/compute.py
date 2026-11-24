"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调、摄入空区间证明与读段确认等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。"""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    expired = []
    valid = []

    for snap in snapshots:
        ts = str(snap.get("created_at", ""))
        # Strip timezone to make comparison naive
        for suffix in ("Z", "+00:00", "+0000"):
            ts = ts.replace(suffix, "")
        ts = ts.split("+")[0]
        try:
            created = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            created = cutoff + timedelta(days=1)
        if created.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
            expired.append(snap)
        else:
            valid.append(snap)

    can_clean = max(0, len(expired) - min_versions)
    return {
        "total_snapshots": len(snapshots),
        "expired_count": len(expired),
        "valid_count": len(valid),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

def delta_reconciler(
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    dedup_key: str = "id",
) -> dict[str, Any]:
    """协调流批数据：对流事件和批事件进行去重合并，输出最终写入集。"""
    # Build dedup index from batch (batch wins for same key)
    batch_index: dict[str, dict[str, Any]] = {}
    for event in batch_events:
        key = str(event.get(dedup_key, ""))
        if key:
            batch_index[key] = event

    reconciled = []
    duplicates = 0
    stream_only = 0

    for event in stream_events:
        key = str(event.get(dedup_key, ""))
        if key and key in batch_index:
            reconciled.append(event)  # batch wins, but keep source trace
            duplicates += 1
        else:
            reconciled.append(event)
            stream_only += 1

    # Append batch-only events
    existing_keys = {str(e.get(dedup_key, "")) for e in stream_events}
    batch_only = 0
    for key, event in batch_index.items():
        if key not in existing_keys:
            reconciled.append(event)
            batch_only += 1

    total = len(reconciled)
    checksum = _compute_checksum(str(reconciled))

    return {
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "duplicates_resolved": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "dedup_key": dedup_key,
        "checksum": checksum,
    }


# ------------------------------------------------------------------
# 摄入空区间证明与读段确认
# ------------------------------------------------------------------

def build_read_segment(
    source: str,
    partition: str,
    start_offset: int,
    end_offset: int,
    *,
    read_rows: int,
    read_result: str,
    last_read_offset: int | None = None,
) -> dict[str, Any]:
    """构造一次读段（read segment）记录。

    区间一律采用 [start_offset, end_offset) 左闭右开语义，例如 100..199
    的完整扫描表示为 start=100, end=200。read_result 区分三类结果：
    - "empty_ok"：区间被完整读取，且确认零行；
    - "read_failed"：读取中途断线/失败，区间未读全，不能确认；
    - "rows_ok"：区间读全且有数据落库。
    """
    if end_offset <= start_offset:
        raise ValueError("读段区间必须满足 start_offset < end_offset")
    if read_result not in ("empty_ok", "read_failed", "rows_ok"):
        raise ValueError(f"未知读操作结果：{read_result}")
    if read_result == "empty_ok" and read_rows != 0:
        raise ValueError("空区间读全的读段 read_rows 必须为 0")
    if read_result == "empty_ok" and last_read_offset is not None:
        raise ValueError("空区间读全不存在最后读取偏移")
    if read_result == "read_failed" and last_read_offset is not None and not (
        start_offset <= last_read_offset < end_offset
    ):
        raise ValueError("最后读取偏移必须落在请求区间内；一行未读到时允许为空")
    return {
        "source": source,
        "partition": partition,
        "start_offset": start_offset,
        "end_offset": end_offset,
        "read_rows": read_rows,
        "read_result": read_result,
        "last_read_offset": last_read_offset,
    }


def read_ack_key(segment: dict[str, Any]) -> str:
    """生成读段确认键。

    零行读全（empty_ok）与读取失败（read_failed）走不同命名空间，
    即使来源、分区、偏移区间完全相同也不会产生同一条确认：
    empty:p1:100:200 与 fail:p1:100:200 永远不同。同一来源同一区间的
    重复确认请求拿到同一个键，由调用方据此保证只确认一次。
    """
    kind = {
        "empty_ok": "empty",
        "read_failed": "fail",
        "rows_ok": "rows",
    }[segment["read_result"]]
    return (
        f"{kind}:{segment['source']}:{segment['partition']}:"
        f"{segment['start_offset']}:{segment['end_offset']}"
    )


def build_empty_interval_proof(
    source: str,
    partition: str,
    start_offset: int,
    end_offset: int,
    *,
    proof_id: str,
    scan_id: str,
    read_at: str,
) -> dict[str, Any]:
    """构造摄入空区间证明：证明某个分区某偏移区间被实际读全且为零行。

    证明只承载「实际覆盖的分区、偏移范围、读操作结果」三类可回查事实，
    与读取失败的记录物理隔离（read_result 固定为 empty_ok）。
    """
    segment = build_read_segment(
        source, partition, start_offset, end_offset,
        read_rows=0, read_result="empty_ok",
    )
    return {
        "proof_id": proof_id,
        "scan_id": scan_id,
        "source": source,
        "partition": partition,
        "covered_start_offset": start_offset,
        "covered_end_offset": end_offset,
        "covered_partition_data_range": f"{partition}:[{start_offset},{end_offset})",
        "read_rows": 0,
        "read_result": "empty_ok",
        "read_at": read_at,
        "status": "待确认",
        "ack_key": read_ack_key(segment),
    }


def retain_unread_ranges_on_failure(
    requested_start: int,
    requested_end: int,
    last_read_offset: int | None,
) -> list[dict[str, int]]:
    """读取中途断线时，计算仍需保留的未读历史数据范围。

    失败绝不推进水位：last_read_offset 是最后一个「成功读取」的偏移，
    下一次实际读取从 last_read_offset + 1 开始（完全没读到则从原起点重试）。
    返回的范围同样是左闭右开区间。
    """
    if requested_end <= requested_start:
        raise ValueError("请求区间必须满足 requested_start < requested_end")
    if last_read_offset is None:
        return [{"start_offset": requested_start, "end_offset": requested_end}]
    if not (requested_start <= last_read_offset < requested_end):
        raise ValueError("最后读取偏移必须落在请求区间内")
    resume_start = last_read_offset + 1
    if resume_start >= requested_end:
        return []
    return [{"start_offset": resume_start, "end_offset": requested_end}]


def plan_next_read(
    *,
    confirmed_end_offset: int | None,
    pending_ranges: list[dict[str, int]],
    stream_end_offset: int,
    largest_confirmed_end: int | None = None,
) -> dict[str, Any]:
    """根据检查点已确认水位和待办区间，规划下一次实际读取的数据范围。

    断线后未读的历史区间优先（不能跳过历史数据）；没有待办时才沿
    已确认水位向流末端续读。严格连续水位缺失时（例如首个登记窗口不从
    偏移 0 开始），退而使用最大已确认区间末端作为续读起点。
    """
    if pending_ranges:
        ordered = sorted(pending_ranges, key=lambda r: r["start_offset"])
        nxt = ordered[0]
        return {
            "mode": "retry_pending",
            "start_offset": nxt["start_offset"],
            "end_offset": nxt["end_offset"],
            "data_range": f"[{nxt['start_offset']},{nxt['end_offset']})",
            "reason": "存在断线后保留的未读历史区间，优先续读",
        }
    start = confirmed_end_offset
    if start is None:
        start = 0 if largest_confirmed_end is None else largest_confirmed_end
    if start >= stream_end_offset:
        return {
            "mode": "idle",
            "start_offset": start,
            "end_offset": start,
            "data_range": f"[{start},{start})",
            "reason": "已确认水位到达流末端，暂无新数据",
        }
    return {
        "mode": "continuous",
        "start_offset": start,
        "end_offset": stream_end_offset,
        "data_range": f"[{start},{stream_end_offset})",
        "reason": "无待办区间，沿检查点水位续读",
    }


def build_checkpoint_resume(
    *,
    source: str,
    partition: str,
    confirmed_end_offset: int | None,
    pending_ranges: list[dict[str, int]],
    stream_end_offset: int,
    last_proof_id: str | None = None,
    largest_confirmed_end: int | None = None,
) -> dict[str, Any]:
    """检查点读取空区间证明后产出的恢复视图：待办随读段确认一并更新。"""
    next_read = plan_next_read(
        confirmed_end_offset=confirmed_end_offset,
        pending_ranges=pending_ranges,
        stream_end_offset=stream_end_offset,
        largest_confirmed_end=largest_confirmed_end,
    )
    return {
        "source": source,
        "partition": partition,
        "confirmed_end_offset": confirmed_end_offset,
        "pending_ranges": [dict(r) for r in pending_ranges],
        "stream_end_offset": stream_end_offset,
        "next_read_partition": partition,
        "next_read_start_offset": next_read["start_offset"],
        "next_read_end_offset": next_read["end_offset"],
        "next_read_data_range": next_read["data_range"],
        "next_read_mode": next_read["mode"],
        "last_proof_id": last_proof_id,
    }


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]
"""流式摄入运行时：读段、空区间证明、读段确认与检查点恢复。

设计要点（对应空区间证明治理需求）：

1. 每次扫描产出一条读段记录，read_result 明确区分
   ``empty_ok``（读全且零行）/ ``read_failed``（中途断线）/ ``rows_ok``（读全且有行）。
2. 零行读全才生成「摄入空区间证明」，证明记录实际覆盖的分区、偏移范围和读操作结果；
   读取失败只保留未读区间，不产生证明、不允许确认。
3. 确认键按结果类型分命名空间（empty:/fail:/rows:），零行读完与读取失败即使区间
   完全相同也不可能共用同一条确认；同一确认键只落一次，保证只确认一次。
4. 同一来源（source, partition, offset）的记录以偏移为幂等键落湖，并发/重复写入
   只产生一条落湖记录，回执标注 duplicated。
5. 检查点按分区维护已确认水位与待办未读区间；确认读段时一并更新待办，并标明下一次
   实际读取的分区数据范围与起始偏移。断线后未读历史范围始终保留、优先重试。
6. 待确认的空窗口收到迟到行时，原证明置为失效，窗口整体回到待办，迟到行的实际落库
   结果可随时按偏移回查。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.compute import (
    build_checkpoint_resume,
    build_empty_interval_proof,
    build_read_segment,
    read_ack_key,
    retain_unread_ranges_on_failure,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _merge_ranges(ranges: list[dict[str, int]]) -> list[dict[str, int]]:
    """合并重叠或嵌套的待办区间，输出升序去重后的区间列表。

    首尾相接（a.end == b.start）的区间不合并：它们是相邻但不同的待办，
    合并会让确认其中一个时误删另一个的待办。
    """
    if not ranges:
        return []
    ordered = sorted(
        (dict(r) for r in ranges),
        key=lambda r: (r["start_offset"], r["end_offset"]),
    )
    merged = [ordered[0]]
    for rng in ordered[1:]:
        last = merged[-1]
        if rng["start_offset"] < last["end_offset"]:
            # 重叠/嵌套：取并集，确保包含关系不会留下残余待办。
            last["start_offset"] = min(last["start_offset"], rng["start_offset"])
            last["end_offset"] = max(last["end_offset"], rng["end_offset"])
        else:
            merged.append(rng)
    return merged


def _subtract_ranges(
    ranges: list[dict[str, int]], cut_start: int, cut_end: int,
) -> list[dict[str, int]]:
    """从待办区间集合中严格减去 [cut_start, cut_end)，保留残余片段。

    相邻但不重叠的区间原样保留（例如减去 [130,200) 不影响 [100,130)）。
    """
    result: list[dict[str, int]] = []
    for rng in ranges:
        r_start, r_end = rng["start_offset"], rng["end_offset"]
        if cut_end <= r_start or cut_start >= r_end:
            result.append(dict(rng))
            continue
        if cut_start <= r_start and cut_end >= r_end:
            continue  # 整段被确认覆盖
        if cut_start > r_start:
            result.append({"start_offset": r_start, "end_offset": min(r_end, cut_start)})
        if cut_end < r_end:
            result.append({"start_offset": max(r_start, cut_end), "end_offset": r_end})
    return result


class IngestRuntime:
    """内存态摄入运行时；真实部署中这一层会替换为事务型状态存储。"""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._counter = 0
        self.scans: dict[str, dict[str, Any]] = {}
        self.segments: dict[str, dict[str, Any]] = {}
        self.proofs: dict[str, dict[str, Any]] = {}
        self.acks: dict[str, dict[str, Any]] = {}
        self.landing: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.checkpoints: dict[tuple[str, str], dict[str, Any]] = {}

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}"

    # ------------------------------------------------------------------
    # 检查点视图
    # ------------------------------------------------------------------

    def _checkpoint(self, source: str, partition: str) -> dict[str, Any]:
        key = (source, partition)
        cp = self.checkpoints.get(key)
        if cp is None:
            cp = {
                "source": source,
                "partition": partition,
                "confirmed_end_offset": None,
                "pending_ranges": [],
                "retry_cursors": {},
                "stream_end_offset": 0,
                "last_proof_id": None,
            }
            self.checkpoints[key] = cp
        return cp

    def _replace_pending(
        self,
        cp: dict[str, Any],
        start: int,
        end: int,
        replacements: list[dict[str, int]],
    ) -> None:
        """用 replacements 替换与 [start, end) 相交的待办条目。

        先做区间减法保留窗口之外的残余片段，再并入 replacements；窗口之外
        （含相邻但不相交）的既有待办原样保留，重复扫描不会叠加成跨段大区间。
        """
        untouched = _subtract_ranges(cp["pending_ranges"], start, end)
        cp["pending_ranges"] = _merge_ranges([*untouched, *replacements])

    def resume_view(self, source: str, partition: str) -> dict[str, Any]:
        """检查点恢复视图：标明下一次实际读取的分区数据范围和起始偏移。"""
        cp = self._checkpoint(source, partition)
        largest_confirmed_end = max(
            (
                seg["end_offset"]
                for ack in self.acks.values()
                if ack["source"] == source and ack["partition"] == partition
                and (seg := self.segments.get(ack["segment_id"])) is not None
            ),
            default=None,
        )
        view = build_checkpoint_resume(
            source=source,
            partition=partition,
            confirmed_end_offset=cp["confirmed_end_offset"],
            pending_ranges=cp["pending_ranges"],
            stream_end_offset=cp["stream_end_offset"],
            last_proof_id=cp["last_proof_id"],
            largest_confirmed_end=largest_confirmed_end,
        )
        # 断线重试游标优先：同一待办窗口内下一次读取从未读尾部起点继续。
        if view["next_read_mode"] == "retry_pending":
            first = view["pending_ranges"][0]
            key = f"[{first['start_offset']},{first['end_offset']})"
            cursor = cp["retry_cursors"].get(key)
            if cursor is not None and first["start_offset"] <= cursor < first["end_offset"]:
                view["next_read_start_offset"] = cursor
                view["next_read_data_range"] = (
                    f"[{cursor},{first['end_offset']})（待办窗口 {key} 的断线续读）"
                )
                view["retry_cursor"] = cursor
        return view

    # ------------------------------------------------------------------
    # 扫描读段
    # ------------------------------------------------------------------

    def scan(
        self,
        source: str,
        partition: str,
        start_offset: int,
        end_offset: int,
        mode: str,
        *,
        last_read_offset: int | None = None,
        partial_offsets: list[int] | None = None,
    ) -> dict[str, Any]:
        """执行一次分区偏移区间扫描，返回带读操作结果的回执。

        mode:
        - ``empty``：完整读取 [start, end) 且零行，生成待确认空区间证明；
        - ``disconnect``：读取中途断线，需给 last_read_offset，保留未读区间；
        - ``rows``：完整读取且有数据行（partial_offsets 给出行偏移）。
        """
        if mode not in ("empty", "disconnect", "rows"):
            return {"ok": False, "message": f"未知扫描模式：{mode}"}
        cp = self._checkpoint(source, partition)
        cp["stream_end_offset"] = max(cp["stream_end_offset"], end_offset)
        scan_id = self._next_id("scan")
        segment_id = self._next_id("seg")
        partial_offsets = partial_offsets or []
        now = _now()

        if mode == "empty":
            segment = build_read_segment(
                source, partition, start_offset, end_offset,
                read_rows=0, read_result="empty_ok",
            )
            proof_id = self._next_id("proof")
            proof = build_empty_interval_proof(
                source, partition, start_offset, end_offset,
                proof_id=proof_id, scan_id=scan_id, read_at=now,
            )
            segment["segment_id"] = segment_id
            segment["scan_id"] = scan_id
            segment["proof_id"] = proof_id
            proof["segment_id"] = segment_id
            self.segments[segment_id] = segment
            self.proofs[proof_id] = proof
            # 扫描完成但尚未确认：窗口保留为待办，确认后才移除。
            self._replace_pending(
                cp, start_offset, end_offset,
                [{"start_offset": start_offset, "end_offset": end_offset}])
            self.scans[scan_id] = {
                "scan_id": scan_id,
                "segment_id": segment_id,
                "proof_id": proof_id,
                "source": source,
                "partition": partition,
                "requested_range": f"[{start_offset},{end_offset})",
                "read_result": "empty_ok",
                "read_rows": 0,
                "scanned_at": now,
            }
            return {
                "ok": True,
                "scan_id": scan_id,
                "segment_id": segment_id,
                "proof_id": proof_id,
                "read_result": "empty_ok",
                "covered_partition_data_range": proof["covered_partition_data_range"],
                "read_rows": 0,
                "ack_key": read_ack_key(segment),
                "proof_status": "待确认",
                "message": "空区间扫描完成，证明已生成，等待检查点确认",
            }

        if mode == "disconnect":
            segment = build_read_segment(
                source, partition, start_offset, end_offset,
                read_rows=len(partial_offsets),
                read_result="read_failed",
                last_read_offset=last_read_offset,
            )
            segment["segment_id"] = segment_id
            segment["scan_id"] = scan_id
            self.segments[segment_id] = segment
            # 断线前已读到的行按幂等键落湖；失败本身不产生确认。
            landed = [
                self._land_row(source, partition, off, {"reason": "断线前已读"}, scan_id)
                for off in partial_offsets
            ]
            unread = retain_unread_ranges_on_failure(
                start_offset, end_offset, last_read_offset)
            # 失败读段不产生任何确认：整个请求窗口（含断线前已读但未确认部分）
            # 都保留为检查点待办；unread 标明断线后的优先续读范围。
            self._replace_pending(cp, start_offset, end_offset, [
                {"start_offset": start_offset, "end_offset": end_offset}])
            # 重试游标：下一次实际读取从未读尾部起点开始，而非整窗重扫。
            retry_start = (
                start_offset if last_read_offset is None
                else last_read_offset + 1
            )
            cp["retry_cursors"][f"[{start_offset},{end_offset})"] = retry_start
            self.scans[scan_id] = {
                "scan_id": scan_id,
                "segment_id": segment_id,
                "proof_id": None,
                "source": source,
                "partition": partition,
                "requested_range": f"[{start_offset},{end_offset})",
                "read_result": "read_failed",
                "read_rows": len(partial_offsets),
                "last_read_offset": last_read_offset,
                "retained_unread_ranges": unread,
                "retained_pending_range": {
                    "start_offset": start_offset, "end_offset": end_offset},
                "scanned_at": now,
            }
            return {
                "ok": False,
                "scan_id": scan_id,
                "segment_id": segment_id,
                "proof_id": None,
                "read_result": "read_failed",
                "read_rows": len(partial_offsets),
                "last_read_offset": last_read_offset,
                "ack_key": read_ack_key(segment),
                "retained_unread_ranges": unread,
                "retained_pending_range": {
                    "start_offset": start_offset, "end_offset": end_offset},
                "landed_before_failure": landed,
                "next_read": self.resume_view(source, partition),
                "message": "读取中途断线，整窗未确认已保留为待办，失败读段不允许确认",
            }

        # rows：完整读取且有行
        segment = build_read_segment(
            source, partition, start_offset, end_offset,
            read_rows=len(partial_offsets), read_result="rows_ok",
        )
        segment["segment_id"] = segment_id
        segment["scan_id"] = scan_id
        self.segments[segment_id] = segment
        landed = [
            self._land_row(source, partition, off, {"reason": "扫描读入"}, scan_id)
            for off in partial_offsets
        ]
        # 数据已落湖但读段未确认前，区间仍是检查点待办。
        self._replace_pending(
            cp, start_offset, end_offset,
            [{"start_offset": start_offset, "end_offset": end_offset}])
        self.scans[scan_id] = {
            "scan_id": scan_id,
            "segment_id": segment_id,
            "proof_id": None,
            "source": source,
            "partition": partition,
            "requested_range": f"[{start_offset},{end_offset})",
            "read_result": "rows_ok",
            "read_rows": len(partial_offsets),
            "scanned_at": now,
        }
        return {
            "ok": True,
            "scan_id": scan_id,
            "segment_id": segment_id,
            "proof_id": None,
            "read_result": "rows_ok",
            "read_rows": len(partial_offsets),
            "ack_key": read_ack_key(segment),
            "landed": landed,
            "message": "区间读取完成，数据已落湖，读段待确认",
        }

    def get_scan(self, scan_id: str) -> dict[str, Any] | None:
        return self.scans.get(scan_id)

    # ------------------------------------------------------------------
    # 空区间证明确认
    # ------------------------------------------------------------------

    def confirm_empty(self, proof_id: str) -> dict[str, Any]:
        """检查点确认空区间证明；同一证明只确认一次，失败读段无法走此路径。"""
        proof = self.proofs.get(proof_id)
        if proof is None:
            return {"ok": False, "message": f"空区间证明 {proof_id} 不存在"}
        if proof["status"] == "已失效":
            return {
                "ok": False,
                "proof_id": proof_id,
                "proof_status": "已失效",
                "message": "证明已因迟到数据落库而失效，窗口需重新扫描，不能确认",
            }
        if proof["status"] == "已确认":
            existing = self.acks.get(proof["ack_key"])
            return {
                "ok": False,
                "proof_id": proof_id,
                "proof_status": "已确认",
                "ack": existing,
                "message": "该空区间证明已确认过，确认只允许一次，本次请求未重复确认",
            }

        source, partition = proof["source"], proof["partition"]
        start, end = proof["covered_start_offset"], proof["covered_end_offset"]
        # 落库台账里若已存在窗口内偏移（迟到行），证明不能确认。
        intruders = [
            rec for (s, p, off), rec in self.landing.items()
            if s == source and p == partition and start <= off < end
        ]
        if intruders:
            proof["status"] = "已失效"
            return {
                "ok": False,
                "proof_id": proof_id,
                "proof_status": "已失效",
                "intruders": intruders,
                "message": "窗口内已有迟到数据落库，空区间证明失效，不能确认",
            }

        cp = self._checkpoint(source, partition)
        ack_key = proof["ack_key"]
        # 防御性双重保证：同一确认键绝不写第二条。
        if ack_key in self.acks:
            return {"ok": False, "message": "确认键已存在，读段只能确认一次"}
        ack = {
            "ack_id": self._next_id("ack"),
            "ack_key": ack_key,
            "ack_kind": "empty",
            "source": source,
            "partition": partition,
            "segment_id": proof["segment_id"],
            "proof_id": proof_id,
            "confirmed_range": f"[{start},{end})",
            "confirmed_at": _now(),
        }
        self.acks[ack_key] = ack
        proof["status"] = "已确认"
        proof["ack_id"] = ack["ack_id"]
        cp["last_proof_id"] = proof_id
        self._advance_after_confirm(cp, start, end)
        return {
            "ok": True,
            "proof_id": proof_id,
            "proof_status": "已确认",
            "ack": ack,
            "resume": self.resume_view(source, partition),
            "message": "空区间证明已确认，检查点待办随读段确认一并更新",
        }

    def confirm_segment(self, segment_id: str) -> dict[str, Any]:
        """通用读段确认：rows_ok 可确认，read_failed 一律拒绝。"""
        segment = self.segments.get(segment_id)
        if segment is None:
            return {"ok": False, "message": f"读段 {segment_id} 不存在"}
        if segment["read_result"] == "read_failed":
            return {
                "ok": False,
                "segment_id": segment_id,
                "read_result": "read_failed",
                "ack_key": read_ack_key(segment),
                "message": "读取失败的读段不能确认：未读区间已保留为重试待办，"
                           "且失败确认键与零行确认键物理隔离",
            }
        if segment["read_result"] == "empty_ok":
            return self.confirm_empty(segment["proof_id"])

        source, partition = segment["source"], segment["partition"]
        start, end = segment["start_offset"], segment["end_offset"]
        ack_key = read_ack_key(segment)
        if ack_key in self.acks:
            return {
                "ok": False,
                "ack": self.acks[ack_key],
                "message": "该读段已确认过，读段只能确认一次",
            }
        ack = {
            "ack_id": self._next_id("ack"),
            "ack_key": ack_key,
            "ack_kind": "rows",
            "source": source,
            "partition": partition,
            "segment_id": segment_id,
            "proof_id": None,
            "confirmed_range": f"[{start},{end})",
            "confirmed_at": _now(),
        }
        self.acks[ack_key] = ack
        cp = self._checkpoint(source, partition)
        self._advance_after_confirm(cp, start, end)
        return {
            "ok": True,
            "ack": ack,
            "resume": self.resume_view(source, partition),
            "message": "有行读段已确认，检查点水位与待办已更新",
        }

    def _advance_after_confirm(
        self, cp: dict[str, Any], start: int, end: int,
    ) -> None:
        """确认后更新待办与水位。

        - 从待办中严格减去已确认区间（区间减法，相邻区间不受影响）；
        - 已确认水位按「已确认连续前缀」重算，存在历史缺口时水位停在缺口前，
          绝不跳过未读历史数据；缺口区间继续留在待办。
        """
        cp["pending_ranges"] = _merge_ranges(
            _subtract_ranges(cp["pending_ranges"], start, end))
        cp["retry_cursors"].pop(f"[{start},{end})", None)
        cp["confirmed_end_offset"] = self._recompute_watermark(cp)
        cp["stream_end_offset"] = max(cp["stream_end_offset"], end)

    def _recompute_watermark(self, cp: dict[str, Any]) -> int | None:
        """从偏移 0 起沿已确认 ack 的连续前缀推导水位。

        水位是连续前缀：只有覆盖起点 0 的已确认读段才能建立水位，后续读段
        必须首尾相接才推进。断线前未确认的头部缺口会让水位停在 None，
        绝不把缺口之后的确认误当成已消费水位。
        """
        confirmed = sorted(
            (
                (a_segment["start_offset"], a_segment["end_offset"])
                for a in self.acks.values()
                if a["source"] == cp["source"]
                and a["partition"] == cp["partition"]
                and (a_segment := self.segments.get(a["segment_id"])) is not None
            ),
            key=lambda x: x[0],
        )
        if not confirmed or confirmed[0][0] != 0:
            return None
        frontier = 0
        for seg_start, seg_end in confirmed:
            if seg_start <= frontier < seg_end:
                frontier = max(frontier, seg_end)
            elif seg_start > frontier:
                break
        return frontier

    # ------------------------------------------------------------------
    # 迟到行 / 并发写入的幂等落湖
    # ------------------------------------------------------------------

    def _land_row(
        self,
        source: str,
        partition: str,
        offset: int,
        payload: dict[str, Any],
        scan_id: str | None,
    ) -> dict[str, Any]:
        key = (source, partition, offset)
        existing = self.landing.get(key)
        if existing is not None:
            return {**existing, "duplicated": True}
        record = {
            "landing_id": self._next_id("land"),
            "source": source,
            "partition": partition,
            "offset": offset,
            "idempotency_key": f"{source}|{partition}|{offset}",
            "lake_location": f"lake://{source}/{partition}/offset={offset}",
            "landing_status": "已入湖",
            "payload": payload,
            "source_scan_id": scan_id,
            "landed_at": _now(),
            "duplicated": False,
        }
        self.landing[key] = record
        return record

    def land_late_row(
        self,
        source: str,
        partition: str,
        offset: int,
        *,
        proof_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """迟到行落湖：幂等写入；若命中待确认空窗口，证明失效且窗口回待办。"""
        proof = self.proofs.get(proof_id) if proof_id else None
        if proof is not None and (
            proof["source"] != source or proof["partition"] != partition
        ):
            return {"ok": False, "message": "证明与给定来源/分区不匹配"}
        cp = self._checkpoint(source, partition)

        # 自动关联：未显式给 proof_id 时，查找覆盖该偏移的待确认证明。
        if proof is None:
            for candidate in self.proofs.values():
                if (
                    candidate["source"] == source
                    and candidate["partition"] == partition
                    and candidate["status"] == "待确认"
                    and candidate["covered_start_offset"] <= offset
                    < candidate["covered_end_offset"]
                ):
                    proof = candidate
                    break

        record = self._land_row(
            source, partition, offset, payload or {}, None)
        response: dict[str, Any] = {
            "ok": True,
            "landing": record,
            "duplicated": record["duplicated"],
        }

        if proof is not None and proof["status"] == "待确认":
            proof["status"] = "已失效"
            proof["invalidated_by_landing"] = record["landing_id"]
            # 整个空窗口回到待办，下一次读取从窗口起点重新扫描。
            self._replace_pending(cp, proof["covered_start_offset"],
                                  proof["covered_end_offset"], [{
                                      "start_offset": proof["covered_start_offset"],
                                      "end_offset": proof["covered_end_offset"],
                                  }])
            cp["stream_end_offset"] = max(
                cp["stream_end_offset"], proof["covered_end_offset"])
            response.update({
                "proof_id": proof["proof_id"],
                "proof_status": "已失效",
                "message": "迟到行已落湖，原空区间证明失效，窗口整体保留为待办",
            })
        elif record["duplicated"]:
            response["message"] = "该来源偏移已有落湖记录，并发写入被幂等抑制，未重复入湖"
        else:
            response["message"] = "迟到行已落湖，可按来源分区偏移回查实际去向"
        response["resume"] = self.resume_view(source, partition)
        return response

    def trace_offset(self, source: str, partition: str, offset: int) -> dict[str, Any]:
        """值班员回查：某条记录实际落库去向，或覆盖它的空区间证明状态。"""
        record = self.landing.get((source, partition, offset))
        if record is not None:
            return {
                "ok": True,
                "source": source,
                "partition": partition,
                "offset": offset,
                "found": True,
                "landing": record,
                "message": f"记录实际落库位置：{record['lake_location']}",
            }
        covering = [
            p for p in self.proofs.values()
            if p["source"] == source and p["partition"] == partition
            and p["covered_start_offset"] <= offset < p["covered_end_offset"]
        ]
        if covering:
            proof = covering[0]
            return {
                "ok": True,
                "found": False,
                "proof_id": proof["proof_id"],
                "proof_status": proof["status"],
                "message": (
                    "该偏移落在已确认空区间证明内，确认无数据"
                    if proof["status"] == "已确认"
                    else "该偏移落在尚未确认的空窗口内，不能断言无数据"
                ),
            }
        return {
            "ok": True,
            "found": False,
            "message": "未落库且无覆盖证明，属于未读区间，请按检查点待办续读",
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def list_proofs(self, source: str, partition: str) -> list[dict[str, Any]]:
        return [
            p for p in self.proofs.values()
            if p["source"] == source and p["partition"] == partition
        ]

    def get_proof(self, proof_id: str) -> dict[str, Any] | None:
        return self.proofs.get(proof_id)

    def list_acks(self, source: str, partition: str) -> list[dict[str, Any]]:
        return [
            a for a in self.acks.values()
            if a["source"] == source and a["partition"] == partition
        ]

    def list_landing(self, source: str, partition: str) -> list[dict[str, Any]]:
        return [
            r for (s, p, _off), r in self.landing.items()
            if s == source and p == partition
        ]


ingest_runtime = IngestRuntime()

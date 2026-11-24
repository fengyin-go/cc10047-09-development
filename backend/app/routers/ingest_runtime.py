"""摄入空区间证明运行时路由：扫描、证明、确认、迟到行落湖与检查点恢复。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.ingest_runtime import ingest_runtime

router = APIRouter(prefix="/api/ingest-runtime", tags=["摄入空区间证明"])


class ScanPayload(BaseModel):
    source: str
    partition: str
    start_offset: int = Field(ge=0)
    end_offset: int = Field(ge=1)
    mode: str = Field(description="empty / disconnect / rows")
    last_read_offset: int | None = None
    partial_offsets: list[int] | None = None


class ConfirmProofPayload(BaseModel):
    proof_id: str


class ConfirmSegmentPayload(BaseModel):
    segment_id: str


class LateRowPayload(BaseModel):
    source: str
    partition: str
    offset: int = Field(ge=0)
    proof_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


@router.post("/scans")
def create_scan(body: ScanPayload) -> dict[str, Any]:
    return ingest_runtime.scan(
        body.source,
        body.partition,
        body.start_offset,
        body.end_offset,
        body.mode,
        last_read_offset=body.last_read_offset,
        partial_offsets=body.partial_offsets,
    )


@router.get("/scans/{scan_id}")
def get_scan(scan_id: str) -> dict[str, Any]:
    scan = ingest_runtime.get_scan(scan_id)
    if scan is None:
        return {"ok": False, "message": f"扫描 {scan_id} 不存在"}
    return {"ok": True, "scan": scan}


@router.get("/proofs")
def list_proofs(source: str, partition: str) -> dict[str, Any]:
    return {"ok": True, "items": ingest_runtime.list_proofs(source, partition)}


@router.get("/proofs/{proof_id}")
def get_proof(proof_id: str) -> dict[str, Any]:
    proof = ingest_runtime.get_proof(proof_id)
    if proof is None:
        return {"ok": False, "message": f"证明 {proof_id} 不存在"}
    return {"ok": True, "proof": proof}


@router.post("/proofs/confirm")
def confirm_proof(body: ConfirmProofPayload) -> dict[str, Any]:
    return ingest_runtime.confirm_empty(body.proof_id)


@router.post("/segments/confirm")
def confirm_segment(body: ConfirmSegmentPayload) -> dict[str, Any]:
    return ingest_runtime.confirm_segment(body.segment_id)


@router.post("/late-rows")
def land_late_row(body: LateRowPayload) -> dict[str, Any]:
    return ingest_runtime.land_late_row(
        body.source,
        body.partition,
        body.offset,
        proof_id=body.proof_id,
        payload=body.payload,
    )


@router.get("/offsets/trace")
def trace_offset(source: str, partition: str, offset: int) -> dict[str, Any]:
    return ingest_runtime.trace_offset(source, partition, offset)


@router.get("/checkpoints/resume")
def resume_checkpoint(source: str, partition: str) -> dict[str, Any]:
    return {"ok": True, "resume": ingest_runtime.resume_view(source, partition)}


@router.get("/acks")
def list_acks(source: str, partition: str) -> dict[str, Any]:
    return {"ok": True, "items": ingest_runtime.list_acks(source, partition)}


@router.get("/landing")
def list_landing(source: str, partition: str) -> dict[str, Any]:
    return {"ok": True, "items": ingest_runtime.list_landing(source, partition)}


@router.post("/reset")
def reset_runtime() -> dict[str, Any]:
    """清空运行时状态，仅供联调与端到端测试使用。"""
    ingest_runtime.reset()
    return {"ok": True, "message": "摄入运行时状态已清空"}

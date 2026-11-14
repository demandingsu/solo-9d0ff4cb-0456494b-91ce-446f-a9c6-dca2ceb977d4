"""拆样 REST 路由（挂载在 collector 进程内，单进程同时服务采集与拆样）。

端点：
* POST /demux/tasks                       提交拆样任务（同参复用 / 异参 409 / 并发只执行一次）
* GET  /demux/tasks                       列出全部拆样任务
* GET  /demux/tasks/{request_id}          查询任务状态与各桶计数
* GET  /demux/tasks/{request_id}/download/{bucket}/R{1|2}
                                          下载某桶的 R1/R2（仅 completed 开放，否则 409）
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .service import DemuxConflict, DemuxRequestError, DemuxService


class SheetRow(BaseModel):
    sample_name: str = Field(..., min_length=1, max_length=128)
    barcode_r1: str = Field(..., min_length=1, max_length=8, description="R1 端 4 位 A/C/G/T 条码")
    barcode_r2: str = Field(..., min_length=1, max_length=8, description="R2 端 4 位 A/C/G/T 条码")


class DemuxCreateRequest(BaseModel):
    request_id: str = Field(
        ..., min_length=1, max_length=128,
        pattern=r"^[A-Za-z0-9_.-]+$", description="请求标识，同标识同参数复用任务",
    )
    run_id: str = Field(
        ..., min_length=1, max_length=128,
        pattern=r"^[A-Za-z0-9_.-]+$", description="已完成采集的运行号",
    )
    sample_sheet: list[SheetRow] = Field(..., min_length=1)
    # 故障注入开关：仅用于本地演示/测试，不参与"同参复用"的参数比对
    fault_after_pairs: int | None = Field(
        default=None, ge=1, description="（演示用）处理到该对数后模拟写盘失败并退出"
    )
    fault_before_status: bool = Field(
        default=False, description="（演示用）模拟文件完成而状态未保存的崩溃窗口"
    )


def build_router(service: DemuxService) -> APIRouter:
    router = APIRouter(prefix="/demux", tags=["demux 后台拆样"])

    @router.post("/tasks", status_code=202)
    def create_task(req: DemuxCreateRequest):
        try:
            snap, created = service.submit(
                request_id=req.request_id,
                run_id=req.run_id,
                sheet_entries=[row.model_dump() for row in req.sample_sheet],
                fault_after_pairs=req.fault_after_pairs,
                fault_before_status=req.fault_before_status,
            )
        except DemuxConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except DemuxRequestError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        snap["reused"] = not created
        return snap

    @router.get("/tasks")
    def list_tasks():
        return {"tasks": service.list_snapshots()}

    @router.get("/tasks/{request_id}")
    def get_task(request_id: str):
        try:
            return service.snapshot(request_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"拆样任务 {request_id} 不存在")

    @router.get("/tasks/{request_id}/download/{bucket}/R{side:int}")
    def download(request_id: str, bucket: str, side: int):
        if side not in (1, 2):
            raise HTTPException(status_code=404, detail="仅支持 R1 / R2")
        try:
            path = service.output_path(request_id, bucket, side)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"拆样任务 {request_id} 不存在")
        except PermissionError as exc:
            if str(exc) == "not_completed":
                raise HTTPException(
                    status_code=409,
                    detail="整组文件与计数齐备（status=completed）后才开放下载",
                )
            raise HTTPException(status_code=400, detail="非法桶名")
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="桶文件不存在，请核对样本名")
        return FileResponse(
            str(path),
            media_type="text/plain; charset=utf-8",
            filename=path.name,
        )

    return router

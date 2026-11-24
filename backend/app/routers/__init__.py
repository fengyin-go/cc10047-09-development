"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import ingest_runtime, resource, chains

# ingest_runtime 的固定路径需先于 /api/{module} 动态资源路由注册。
ROUTERS = [ingest_runtime, resource, chains]

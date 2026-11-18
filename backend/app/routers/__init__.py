"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

quality 固定在 /api/quality 前缀下，必须先于通用 /api/{module} 动态路由注册。
"""
from __future__ import annotations

from app.routers import quality, resource

ROUTERS = [quality, resource]

from fastapi import APIRouter

from app.api.endpoints import engine, health, test_engine

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(engine.router)
api_router.include_router(test_engine.router)

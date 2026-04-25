from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.models.db import create_tables
from app.routers import history, schemas, validate
from app.utils.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info("Starting up — creating database tables if not exist")
    create_tables()
    logger.info("Startup complete")
    yield
    # --- Shutdown ---
    logger.info("Shutting down")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
)

# Routers
app.include_router(validate.router)
app.include_router(schemas.router)
app.include_router(history.router)


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok", "version": settings.app_version}

"""FastAPI main application."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import engine, Base

# Create tables
Base.metadata.create_all(bind=engine)

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
from app.api.routes.data_import import router as import_router
from app.api.routes.dashboard import router as dashboard_router
from app.api.routes.forecast import router as forecast_router
from app.api.routes.recommendations import router as recommendations_router
from app.api.routes.risks import router as risks_router
from app.api.routes.export import router as export_router
from app.api.llm.chat import router as chat_router

app.include_router(import_router, prefix=settings.api_prefix, tags=["import"])
app.include_router(dashboard_router, prefix=settings.api_prefix, tags=["dashboard"])
app.include_router(forecast_router, prefix=settings.api_prefix, tags=["forecast"])
app.include_router(recommendations_router, prefix=settings.api_prefix, tags=["recommendations"])
app.include_router(risks_router, prefix=settings.api_prefix, tags=["risks"])
app.include_router(export_router, prefix=settings.api_prefix, tags=["export"])
app.include_router(chat_router, prefix=settings.api_prefix, tags=["llm"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "AI Financial Analytics API",
        "version": settings.app_version,
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}

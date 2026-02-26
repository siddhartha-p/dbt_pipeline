"""
FastAPI application entry point.
Configures and starts the Employee & Timesheet API.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import get_settings
from api.auth.router import router as auth_router
from api.employees.router import router as employees_router
from api.timesheets.router import router as timesheets_router

settings = get_settings()

# Create FastAPI application
app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router)
app.include_router(employees_router)
app.include_router(timesheets_router)


@app.get("/", tags=["Health"])
def root():
    """Root endpoint - API health check."""
    return {
        "status": "healthy",
        "api": settings.api_title,
        "version": settings.api_version
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

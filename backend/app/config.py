"""Application configuration."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/financial_analytics"
    
    # File storage
    data_dir: str = "./data"
    raw_files_dir: str = "./data/raw"
    processed_files_dir: str = "./data/processed"
    
    # LLM
    openai_api_key: Optional[str] = None
    llm_model: str = "gpt-3.5-turbo"
    llm_temperature: float = 0.3
    
    # Vector DB
    chroma_db_path: str = "./data/chroma_db"
    
    # Security
    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # Application
    app_name: str = "AI Financial Analytics"
    app_version: str = "0.1.0"
    debug: bool = True
    
    # API
    api_prefix: str = "/api"
    cors_origins: list[str] = ["http://localhost:8501"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

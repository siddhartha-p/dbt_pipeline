"""
Configuration settings for the API.
Loads environment variables and provides typed settings.
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database settings (matches .env uppercase with DB_ prefix)
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "postgres"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # JWT settings
    JWT_SECRET_KEY: str = "your-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # API settings
    API_TITLE: str = "Employee & Timesheet API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "RESTful API for employee management and timesheet access"
    
    @property
    def database_url(self) -> str:
        """Construct database URL from components."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # Legacy property names for backwards compatibility
    @property
    def db_host(self) -> str:
        return self.DB_HOST
    
    @property
    def db_port(self) -> str:
        return self.DB_PORT
    
    @property
    def db_name(self) -> str:
        return self.DB_NAME
    
    @property
    def db_user(self) -> str:
        return self.DB_USER
    
    @property
    def db_password(self) -> str:
        return self.DB_PASSWORD
    
    @property
    def jwt_secret_key(self) -> str:
        return self.JWT_SECRET_KEY
    
    @property
    def jwt_algorithm(self) -> str:
        return self.JWT_ALGORITHM
    
    @property
    def jwt_access_token_expire_minutes(self) -> int:
        return self.JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    
    @property
    def api_title(self) -> str:
        return self.API_TITLE
    
    @property
    def api_version(self) -> str:
        return self.API_VERSION
    
    @property
    def api_description(self) -> str:
        return self.API_DESCRIPTION
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

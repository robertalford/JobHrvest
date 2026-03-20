"""Application configuration via environment variables."""

from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    APP_NAME: str = "JobHarvest"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    API_V1_PREFIX: str = "/api/v1"

    # Database
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "jobharvest"
    POSTGRES_USER: str = "jobharvest"
    POSTGRES_PASSWORD: str = "jobharvest"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def DATABASE_URL_SYNC(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    @property
    def REDIS_URL(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def CELERY_BROKER_URL(self) -> str:
        return self.REDIS_URL

    @property
    def CELERY_RESULT_BACKEND(self) -> str:
        return self.REDIS_URL

    # Ollama
    OLLAMA_HOST: str = "ollama"
    OLLAMA_PORT: int = 11434
    OLLAMA_MODEL: str = "llama3.1:8b"
    # Fast LLM for Layer 5 (first attempt). Smaller/faster model that handles most cases.
    # Set to empty string to skip the fast layer and go straight to OLLAMA_MODEL.
    # e.g. "llama3.2:3b" — fits alongside 8b on 8GB RAM via Ollama mmap.
    OLLAMA_FAST_MODEL: Optional[str] = None
    # Vision model for Layer 7 screenshot extraction (e.g. "llava:7b").
    # Leave empty to disable the vision layer.
    OLLAMA_VISION_MODEL: Optional[str] = None

    @property
    def OLLAMA_BASE_URL(self) -> str:
        return f"http://{self.OLLAMA_HOST}:{self.OLLAMA_PORT}"

    # Crawling
    CRAWL_USER_AGENT: str = "JobHarvest/1.0 (job-listing-research; contact@jobharvest.local)"
    CRAWL_RATE_LIMIT_SECONDS: float = 0.5   # 0.5s between requests to same domain (polite but fast)
    CRAWL_MAX_DEPTH: int = 3
    CRAWL_TIMEOUT_SECONDS: int = 20         # reduced from 30s — faster failure on dead sites
    CRAWL_MAX_CONCURRENT: int = 4

    # Storage
    STORAGE_BASE_PATH: str = "/storage"
    RAW_HTML_PATH: str = "/storage/raw_html"
    SCREENSHOTS_PATH: str = "/storage/screenshots"

    # Auth
    SECRET_KEY: str = "change-this-secret-key-in-production"
    ACCESS_TOKEN_EXPIRE_HOURS: int = 168  # 7 days
    APP_USERNAME: str = "admin"
    APP_PASSWORD_HASH: str = ""  # bcrypt hash — set via .env

    # Cors
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:5173", "http://localhost:80", "https://jobharvet.alfords.xyz"]

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()

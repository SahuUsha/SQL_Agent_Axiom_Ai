from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Literal


class Settings(BaseSettings):
    LLM_PROVIDER: Literal["ollama", "openai", "anthropic", "groq", "grok"] = "ollama"
    GROQ_API_KEY: Optional[str] = "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"
    XAI_API_KEY: Optional[str] = None
    GROQ_MODEL: str = "llama-3.1-8b-instant"
    OLLAMA_MODEL: str = ""
    OLLAMA_BASE_URL: str = ""
    
    DB_DIALECT: str = "duckdb"
    DATABASE_URL: Optional[str] = None
    DUCKDB_DATA_PATH: str = "./data"
    
    BIGQUERY_PROJECT_ID: Optional[str] = None
    BIGQUERY_DATASET: Optional[str] = None
    
    USE_DOCKER: bool = False
    USE_EC2: bool = False
    PORT: int = 8002
    
    MAX_RESULT_ROWS: int = 1000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

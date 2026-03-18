from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    crucible_api_url: str = "http://localhost:8000"
    crucible_api_token: str = ""

    transport: Literal["stdio", "sse"] = "stdio"
    sse_host: str = "0.0.0.0"
    sse_port: int = 8001

    # Automatically injected into submitted test plans if missing.
    k6_prometheus_rw_url: str = ""


settings = Settings()

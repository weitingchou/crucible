from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    celery_broker_url: str = "amqp://guest:guest@localhost:5672//"
    celery_result_backend: str = "rpc://"

    s3_bucket: str = "project-crucible-storage"
    aws_region: str = "us-east-1"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/crucible"

    # Injected by the container scheduler so master tasks can advertise their IP.
    runner_ip: str = "127.0.0.1"

    pushgateway_url: str = "http://localhost:9091"

    # Paths to the k6 binary and the generic SQL driver script inside the container.
    k6_binary: str = "/usr/local/bin/k6"
    sql_driver_path: str = "/app/worker/src/worker/drivers/generic_sql_driver.js"


settings = Settings()

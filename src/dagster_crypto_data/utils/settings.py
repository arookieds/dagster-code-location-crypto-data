"""
settings module implement classes, methods, and functions to load the settings
for the pipelines, from:
- .env, if run locally
- environments variables (locally and k8s)

The settings should be held in a Settings class, derived from pydantic_settings.BaseSettings
and use best practices for not exposing secrets, providing unwanted access, etc
"""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Settings is holding the settings to the pipeline
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Logging settings
    log_level: str = Field(default="INFO")
    is_production: bool = Field(default=False)

    # Database settings
    db_host: str = Field(default="localhost")
    db_port: int = Field(default=5432)
    db_name: str = Field(
        default="crypto"
    )  # Do not require validation, as it will be done on the connector side
    db_username: str = Field(default="")
    db_password: SecretStr = Field(default=SecretStr(""))

    # S3/MinIO settings
    s3_url: str = Field(default="http://localhost:9000")  # for minio and s3
    s3_bucket: str = Field(default="crypto-data")
    s3_user: str = Field(default="")
    s3_password: SecretStr = Field(default=SecretStr(""))


@lru_cache
def get_settings() -> Settings:
    """
    get_settings is used to load the settings in to a Settings class

    Returns:
        Settings: Application settings loaded from environment variables.
    """
    return Settings()

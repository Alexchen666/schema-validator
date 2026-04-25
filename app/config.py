from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    database_url: str

    # Schema file storage
    schema_storage_path: str = "./schemas"

    # App metadata
    app_name: str = "Data Validator"
    app_version: str = "1.0.0"
    debug: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

# Single instance imported throughout the app
settings = Settings()

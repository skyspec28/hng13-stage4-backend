from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    RABBITMQ_URL: str = "amqp://admin:admin123@localhost:5672/"
    REDIS_URL: str = "redis://localhost:6379/0"
    FCM_API_KEY: str = ""
    FCM_PROJECT_ID: str = ""
    API_GATEWAY_URL: str = "http://localhost:8000"
    WORKER_PREFETCH_COUNT: int = 10
    MAX_RETRIES: int = 5
    
    class Config:
        env_file = ".env"


settings = Settings()


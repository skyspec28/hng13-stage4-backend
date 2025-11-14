from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    RABBITMQ_URL: str = "amqp://admin:admin123@localhost:5672/"
    REDIS_URL: str = "redis://localhost:6379/0"
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    FROM_EMAIL: str = "noreply@notification.com"
    TEMPLATE_SERVICE_URL: str = "http://localhost:8002"
    API_GATEWAY_URL: str = "http://localhost:8000"
    WORKER_PREFETCH_COUNT: int = 10
    MAX_RETRIES: int = 5
    
    class Config:
        env_file = ".env"


settings = Settings()


import aio_pika
import asyncio
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import httpx
from datetime import datetime

from app.config import settings
from app.services.circuit_breaker import CircuitBreaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EmailWorker:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.connection = None
        self.channel = None
        self.api_gateway_url = settings.API_GATEWAY_URL
    
    async def connect(self):
        self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=settings.WORKER_PREFETCH_COUNT)
        
        # Declare queues if they don't exist
        self.queue = await self.channel.declare_queue(
            "email.queue",
            durable=True,
            arguments={
                "x-max-priority": 10,
                "x-message-ttl": 86400000,
                "x-max-length": 100000
            }
        )
        self.dlq = await self.channel.declare_queue(
            "failed.queue",
            durable=True
        )
    
    async def send_email(self, to_email: str, subject: str, html_content: str):
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = settings.FROM_EMAIL
        message["To"] = to_email
        message.attach(MIMEText(html_content, "html"))
        
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            if settings.SMTP_USER and settings.SMTP_PASSWORD:
                server.starttls()
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.send_message(message)
        
        logger.info(f"Email sent successfully to {to_email}")
    
    async def fetch_and_render_template(self, template_code: str, variables: dict) -> tuple:
        """Fetch template from template service and render with variables"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{settings.TEMPLATE_SERVICE_URL}/api/v1/templates/{template_code}",
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    template_data = response.json()
                    template = template_data.get("data", {})
                    
                    subject = variables.get("subject", template.get("name", "Notification"))
                    html_content = template.get("content", "<p>Notification</p>")
                    
                    for key, value in variables.items():
                        subject = subject.replace(f"{{{{{key}}}}}", str(value))
                        html_content = html_content.replace(f"{{{{{key}}}}}", str(value))
                    
                    return subject, html_content
                else:
                    logger.warning(f"Template not found: {template_code}, using default")
                    return variables.get("subject", "Notification"), f"<html><body><h1>{variables.get('subject', 'Notification')}</h1><p>{variables.get('message', '')}</p></body></html>"
        except Exception as e:
            logger.error(f"Error fetching template: {e}")
            return variables.get("subject", "Notification"), f"<html><body><h1>{variables.get('subject', 'Notification')}</h1><p>{variables.get('message', '')}</p></body></html>"
    
    async def update_status(self, notification_id: str, status: str, error_message: str = None):
        """Update notification status via API Gateway"""
        try:
            async with httpx.AsyncClient() as client:
                payload = {
                    "status": status,
                    "delivered_at": datetime.utcnow().isoformat() if status == "delivered" else None,
                    "error_message": error_message,
                    "metadata": {"worker_type": "email"}
                }
                
                response = await client.post(
                    f"{self.api_gateway_url}/api/v1/email/status/?notification_id={notification_id}",
                    json=payload,
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    logger.info(f"Updated status for {notification_id} to {status}")
                else:
                    logger.warning(f"Failed to update status: {response.status_code}")
        except Exception as e:
            logger.error(f"Error updating status: {e}")
    
    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                body = json.loads(message.body.decode())
                notification_id = body.get("notification_id")
                logger.info(f"Processing notification {notification_id}")
                
                # Update status to processing
                await self.update_status(notification_id, "processing")
                
                if not self.circuit_breaker.can_proceed():
                    logger.warning("Circuit breaker OPEN, requeueing")
                    await message.reject(requeue=True)
                    await asyncio.sleep(5)
                    return
                
                # Fetch and render template
                template_code = body.get("template_code")
                variables = body.get("variables", {})
                
                subject, html_content = await self.fetch_and_render_template(template_code, variables)
                
                await self.send_email(
                    to_email=body.get("user_email"),
                    subject=subject,
                    html_content=html_content
                )
                
                self.circuit_breaker.record_success()
                logger.info(f"Successfully sent email for {notification_id}")
                
                await self.update_status(notification_id, "delivered")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                self.circuit_breaker.record_failure()
                
                retry_count = body.get("retry_count", 0)
                if retry_count >= settings.MAX_RETRIES:
                    logger.error(f"Max retries exceeded for {notification_id}, moving to DLQ")
                    await self.update_status(notification_id, "failed", str(e))
                    await self.move_to_dlq(body)
                else:
                    body["retry_count"] = retry_count + 1
                    await asyncio.sleep(2 ** retry_count)
                    await message.reject(requeue=True)
    
    async def move_to_dlq(self, message_body: dict):
        message = aio_pika.Message(body=json.dumps(message_body).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.channel.default_exchange.publish(message, routing_key="failed.queue")
    
    async def start(self):
        await self.connect()
        logger.info("Email worker started, waiting for messages...")
        await self.queue.consume(self.process_message)
        await asyncio.Future()


async def main():
    worker = EmailWorker()
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutting down worker...")


if __name__ == "__main__":
    asyncio.run(main())


import aio_pika
import asyncio
import json
import logging
import httpx
from datetime import datetime

from app.config import settings
from app.services.circuit_breaker import CircuitBreaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PushWorker:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.connection = None
        self.channel = None
        self.fcm_url = "https://fcm.googleapis.com/fcm/send"
        self.api_gateway_url = settings.API_GATEWAY_URL
    
    async def connect(self):
        self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=settings.WORKER_PREFETCH_COUNT)
        
        # Declare queues if they don't exist
        self.queue = await self.channel.declare_queue(
            "push.queue",
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
    
    async def send_push_notification(self, token: str, title: str, body_text: str, data: dict):
        if not settings.FCM_API_KEY:
            logger.warning("FCM_API_KEY not set, skipping push notification")
            return
        
        payload = {
            "to": token,
            "notification": {"title": title, "body": body_text},
            "data": data
        }
        
        headers = {
            "Authorization": f"key={settings.FCM_API_KEY}",
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(self.fcm_url, json=payload, headers=headers, timeout=10.0)
            response.raise_for_status()
            logger.info(f"FCM Response: {response.json()}")
    
    async def update_status(self, notification_id: str, status: str, error_message: str = None):
        """Update notification status via API Gateway"""
        try:
            async with httpx.AsyncClient() as client:
                payload = {
                    "status": status,
                    "delivered_at": datetime.utcnow().isoformat() if status == "delivered" else None,
                    "error_message": error_message,
                    "metadata": {"worker_type": "push"}
                }
                
                response = await client.post(
                    f"{self.api_gateway_url}/api/v1/push/status/?notification_id={notification_id}",
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
                logger.info(f"Processing push notification {notification_id}")
                
                # Update status to processing
                await self.update_status(notification_id, "processing")
                
                if not self.circuit_breaker.can_proceed():
                    logger.warning("Circuit breaker OPEN, requeueing")
                    await message.reject(requeue=True)
                    await asyncio.sleep(5)
                    return
                
                variables = body.get("variables", {})
                await self.send_push_notification(
                    token=body.get("user_push_token", ""),
                    title=variables.get("title", "Notification"),
                    body_text=variables.get("body", "You have a new notification"),
                    data=variables
                )
                
                self.circuit_breaker.record_success()
                logger.info(f"Push notification sent: {notification_id}")
                
                # Update status to delivered
                await self.update_status(notification_id, "delivered")
                
            except Exception as e:
                logger.error(f"Error processing push: {e}", exc_info=True)
                self.circuit_breaker.record_failure()
                
                retry_count = body.get("retry_count", 0)
                if retry_count >= settings.MAX_RETRIES:
                    logger.error(f"Max retries exceeded, moving to DLQ")
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
        logger.info("Push worker started, waiting for messages...")
        await self.queue.consume(self.process_message)
        await asyncio.Future()


async def main():
    worker = PushWorker()
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutting down worker...")


if __name__ == "__main__":
    asyncio.run(main())


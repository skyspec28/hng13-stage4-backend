import aio_pika
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from uuid import uuid4
import redis.asyncio as redis

from app.config import settings
from app.models.requests import NotificationRequest

logger = logging.getLogger(__name__)


class QueueService:
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.redis_client: Optional[redis.Redis] = None
        
    async def connect(self):
        """Establish connection to RabbitMQ"""
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            self.channel = await self.connection.channel()
            
            # Declare exchange
            self.exchange = await self.channel.declare_exchange(
                "notifications.direct",
                aio_pika.ExchangeType.DIRECT,
                durable=True
            )
            
            # Declare queues with matching arguments as workers
            email_queue = await self.channel.declare_queue(
                "email.queue",
                durable=True,
                arguments={
                    "x-max-priority": 10,
                    "x-message-ttl": 86400000,
                    "x-max-length": 100000
                }
            )
            push_queue = await self.channel.declare_queue(
                "push.queue",
                durable=True,
                arguments={
                    "x-max-priority": 10,
                    "x-message-ttl": 86400000,
                    "x-max-length": 100000
                }
            )
            failed_queue = await self.channel.declare_queue("failed.queue", durable=True)
            
            # Bind queues to exchange
            await email_queue.bind(self.exchange, routing_key="email.queue")
            await push_queue.bind(self.exchange, routing_key="push.queue")
            await failed_queue.bind(self.exchange, routing_key="failed.queue")
            
        return self.channel
    
    async def get_redis(self):
        """Get Redis connection"""
        if not self.redis_client:
            self.redis_client = await redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        return self.redis_client
    
    async def queue_notification(
        self,
        notification_request: NotificationRequest,
        user_data: Dict[str, Any],
        correlation_id: Optional[str] = None
    ) -> str:
        """Queue a notification for processing"""
        await self.connect()
        
        notification_id = f"notif_{uuid4().hex[:12]}"
        
        message_body = {
            "notification_id": notification_id,
            "notification_type": notification_request.notification_type.value,
            "user_id": str(notification_request.user_id),
            "user_email": user_data.get("email"),
            "user_push_token": user_data.get("push_token"),
            "template_code": notification_request.template_code,
            "variables": notification_request.variables,
            "request_id": notification_request.request_id,
            "priority": notification_request.priority,
            "metadata": notification_request.metadata,
            "correlation_id": correlation_id,
            "created_at": datetime.utcnow().isoformat(),
            "retry_count": 0
        }
        
        # Determine routing key based on notification type
        routing_key = f"{notification_request.notification_type.value}.queue"
        
        message = aio_pika.Message(
            body=json.dumps(message_body).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=notification_request.priority,
            correlation_id=correlation_id,
            message_id=notification_id
        )
        
        await self.exchange.publish(
            message,
            routing_key=routing_key
        )
        
        logger.info(f"Queued notification {notification_id} to {routing_key}")
        
        # Store in Redis for idempotency check
        redis_client = await self.get_redis()
        await redis_client.setex(
            f"request_id:{notification_request.request_id}",
            86400,  # 24 hours
            json.dumps({"notification_id": notification_id, "status": "pending"})
        )
        
        # Store notification status
        await redis_client.setex(
            f"notification:{notification_id}",
            settings.STATUS_CACHE_TTL,
            json.dumps({
                "notification_id": notification_id,
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            })
        )
        
        return notification_id
    
    async def check_request_id(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Check if request_id has been processed (idempotency)"""
        redis_client = await self.get_redis()
        cached = await redis_client.get(f"request_id:{request_id}")
        
        if cached:
            return json.loads(cached)
        
        return None
    
    async def get_notification_status(self, notification_id: str) -> Optional[Dict[str, Any]]:
        """Get notification status from Redis"""
        redis_client = await self.get_redis()
        status_data = await redis_client.get(f"notification:{notification_id}")
        
        if status_data:
            return json.loads(status_data)
        
        return None
    
    async def update_notification_status(
        self,
        notification_id: str,
        status: str,
        error_message: Optional[str] = None,
        delivered_at: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Update notification status in Redis"""
        redis_client = await self.get_redis()
        
        # Get existing status
        existing = await self.get_notification_status(notification_id)
        if not existing:
            # If notification doesn't exist, create a new status record
            existing = {
                "notification_id": notification_id,
                "created_at": datetime.utcnow().isoformat()
            }
        
        # Update status fields
        existing["status"] = status
        existing["updated_at"] = datetime.utcnow().isoformat()
        
        if error_message:
            existing["error_message"] = error_message
        
        if delivered_at:
            existing["delivered_at"] = delivered_at
        
        if metadata:
            existing["metadata"] = metadata
        
        # Store updated status
        await redis_client.setex(
            f"notification:{notification_id}",
            settings.STATUS_CACHE_TTL,
            json.dumps(existing)
        )
        
        logger.info(f"Updated notification {notification_id} status to {status}")
        
        return existing
    
    async def close(self):
        """Close connections"""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        if self.redis_client:
            await self.redis_client.close()


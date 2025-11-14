from fastapi import APIRouter, HTTPException, status, Request, Query
from datetime import datetime
import logging
from typing import Optional

from app.models.requests import NotificationRequest, StatusUpdateRequest
from app.models.responses import ApiResponse, NotificationResponse
from app.services.queue_service import QueueService
from app.services.user_service import UserService

logger = logging.getLogger(__name__)
router = APIRouter()

# Dependency injection
queue_service = QueueService()
user_service = UserService()


@router.post(
    "/notifications/",
    response_model=ApiResponse[NotificationResponse],
    status_code=status.HTTP_202_ACCEPTED
)
async def create_notification(
    request: Request,
    notification_request: NotificationRequest
):
    """
    Create and queue a new notification
    
    - **notification_type**: Type of notification (email or push)
    - **user_id**: Target user identifier
    - **template_code**: Template to use for the notification
    - **variables**: Data for template substitution
    - **request_id**: Unique identifier for idempotency
    - **priority**: Priority level (1-10, higher is more urgent)
    """
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    try:
        # Check idempotency
        existing = await queue_service.check_request_id(notification_request.request_id)
        if existing:
            logger.info(f"Duplicate request detected: {notification_request.request_id}")
            return ApiResponse(
                success=True,
                message="Notification already processed",
                data=NotificationResponse(
                    notification_id=existing.get("notification_id"),
                    status=existing.get("status", "pending"),
                    created_at=datetime.utcnow()
                )
            )
        
        # Validate user exists and get preferences
        user = await user_service.get_user(notification_request.user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check user preferences
        preferences = user.get("preferences", {})
        if notification_request.notification_type == "email" and not preferences.get("email", True):
            return ApiResponse(
                success=False,
                message="User has disabled email notifications",
                error="Notification preference disabled"
            )
        
        if notification_request.notification_type == "push" and not preferences.get("push", True):
            return ApiResponse(
                success=False,
                message="User has disabled push notifications",
                error="Notification preference disabled"
            )
        
        # Queue the notification
        notification_id = await queue_service.queue_notification(
            notification_request=notification_request,
            user_data=user,
            correlation_id=correlation_id
        )
        
        response_data = NotificationResponse(
            notification_id=notification_id,
            status="pending",
            created_at=datetime.utcnow()
        )
        
        return ApiResponse(
            success=True,
            message="Notification queued successfully",
            data=response_data
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating notification: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to queue notification"
        )


@router.get(
    "/notifications/{notification_id}",
    response_model=ApiResponse[dict]
)
async def get_notification_status(notification_id: str):
    """Get the status of a notification"""
    status_data = await queue_service.get_notification_status(notification_id)
    
    if not status_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Notification not found"
        )
    
    return ApiResponse(
        success=True,
        message="Notification status retrieved",
        data=status_data
    )


@router.post(
    "/{notification_preference}/status/",
    response_model=ApiResponse[dict],
    status_code=status.HTTP_200_OK
)
async def update_notification_status_by_preference(
    notification_preference: str,
    status_update: StatusUpdateRequest,
    notification_id: Optional[str] = Query(None, description="Notification ID to update")
):
    """
    Update notification status for a specific notification preference (email/push)
    
    This endpoint is typically called by workers after delivery attempts
    
    - **notification_preference**: Type of notification (email or push)
    - **notification_id**: Query parameter with the notification ID to update
    - **status**: New status (pending, processing, delivered, failed)
    - **error_message**: Optional error message if failed
    - **delivered_at**: Optional delivery timestamp
    - **metadata**: Optional additional metadata
    """
    if not notification_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="notification_id query parameter is required"
        )
    
    # Validate notification preference
    if notification_preference not in ["email", "push"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="notification_preference must be 'email' or 'push'"
        )
    
    try:
        updated_status = await queue_service.update_notification_status(
            notification_id=notification_id,
            status=status_update.status.value,
            error_message=status_update.error_message,
            delivered_at=status_update.delivered_at,
            metadata=status_update.metadata
        )
        
        return ApiResponse(
            success=True,
            message=f"Status updated successfully for {notification_preference} notification",
            data=updated_status
        )
    
    except Exception as e:
        logger.error(f"Error updating notification status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update notification status"
        )


@router.post(
    "/notifications/{notification_id}/status",
    response_model=ApiResponse[dict],
    status_code=status.HTTP_200_OK
)
async def update_notification_status_by_id(
    notification_id: str,
    status_update: StatusUpdateRequest
):
    """
    Update notification status by notification ID
    
    This is an alternative endpoint for updating status without specifying preference
    
    - **notification_id**: The notification ID to update
    - **status**: New status (pending, processing, delivered, failed)
    - **error_message**: Optional error message if failed
    - **delivered_at**: Optional delivery timestamp
    - **metadata**: Optional additional metadata
    """
    try:
        updated_status = await queue_service.update_notification_status(
            notification_id=notification_id,
            status=status_update.status.value,
            error_message=status_update.error_message,
            delivered_at=status_update.delivered_at,
            metadata=status_update.metadata
        )
        
        return ApiResponse(
            success=True,
            message="Status updated successfully",
            data=updated_status
        )
    
    except Exception as e:
        logger.error(f"Error updating notification status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update notification status"
        )



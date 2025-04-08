from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status
from app.db.cassandra_client import cassandra_client
from app.models.cassandra_models import MessageModel

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse

class MessageController:
    """
    Controller for handling message operations
    This is a stub that students will implement
    """

    def __init__(self):
        self.message_model = MessageModel() # Placeholder for Cassandra client
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        
        Args:
            message_data: The message data including content, sender_id, and receiver_id
            
        Returns:
            The created message with metadata
        
        Raises:
            HTTPException: If message sending fails
        """
        # This is a stub - students will implement the actual logic

        conversation_id = str(message_data.sender_id) + "_" + str(message_data.receiver_id)
        return await self.message_model.create_message(
            conversation_id=conversation_id,
            sender_id=message_data.sender_id,
            recipient_id=message_data.receiver_id,
            message_text=message_data.content
        )
    
    async def get_conversation_messages(
        self, 
        conversation_id: str, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        
        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        # This is a stub - students will implement the actual logic
        
        return await self.message_model.get_conversation_messages(
            conversation_id=conversation_id,
            page=page,
            limit=limit
        )

    
    async def get_messages_before_timestamp(
        self, 
        conversation_id: int, 
        before_timestamp: datetime,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        # This is a stub - students will implement the actual logic
        conversation_id = conversation_id
        before_timestamp = before_timestamp
        page = page
        limit = limit

        return await self.message_model.get_messages_before_timestamp(
            conversation_id=conversation_id,
            before_timestamp=before_timestamp,
            page=page,
            limit=limit
        )
        
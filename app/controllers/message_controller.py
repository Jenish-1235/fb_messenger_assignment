from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status
from app.db.cassandra_client import cassandra_client
from app.models.cassandra_models import MessageModel
from app.models.cassandra_models import ConversationModel

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse

class MessageController:

    def __init__(self):
        self.message_model = MessageModel() # Placeholder for Cassandra client
        self.conversation_model = ConversationModel()
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        conversation_id = await self.conversation_model.create_or_get_conversation(message_data.sender_id, message_data.receiver_id)
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
        
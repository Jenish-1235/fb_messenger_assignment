from fastapi import HTTPException, status
from app.models.cassandra_models import ConversationModel

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationController:

    def __init__(self):
        self.conversation_model = ConversationModel()
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        return await self.conversation_model.get_user_conversations(user_id, page, limit)
    
    async def get_conversation(self, conversation_id: str) -> ConversationResponse:
        return await self.conversation_model.get_conversation(conversation_id)
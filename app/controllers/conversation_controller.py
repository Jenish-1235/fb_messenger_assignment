from fastapi import HTTPException, status
from app.models.cassandra_models import ConversationModel

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """

    def __init__(self):
        self.conversation_model = ConversationModel()
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        # This is a stub - students will implement the actual logic
        
        return await self.conversation_model.get_user_conversations(user_id, page, limit)
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        # This is a stub - students will implement the actual logic
        self.conversation_model.get_conversation(conversation_id)
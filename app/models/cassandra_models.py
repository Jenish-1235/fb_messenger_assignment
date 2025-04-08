"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def create_message(conversation_id: str, sender_id: int, recipient_id: int, message_text: str):
        """
        Create a new message and return it in the response format.
        """
        message_id = uuid.uuid1()  # time-based UUID (for ordering)
        
        query = """
        INSERT INTO messages (conversation_id, message_id, sender_id, recipient_id, message_text)
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (conversation_id, message_id, sender_id, recipient_id, message_text)
        cassandra_client.execute(query, params)

        return {
            "id": str(message_id),
            "sender_id": sender_id,
            "receiver_id": recipient_id,
            "created_at": str(datetime.now()),
            "conversation_id": conversation_id,
            "content": message_text,
        }
    pass
    
    @staticmethod
    async def get_conversation_messages(conversation_id: str, page: int, limit: int):
        """
        Get messages for a conversation with pagination.
        Cassandra doesn't support OFFSET, so we fetch all and slice in memory.
        Best for small conversations or prototyping.
        """
        query = """
        SELECT * FROM messages WHERE conversation_id = %s
        """
        rows = cassandra_client.execute(query, (conversation_id,))
        
        all_messages = []
        for row in rows:
            print(f"Row: {row}")
            message = {
                "id": str(row.get('message_id')),
                "sender_id": row.get('sender_id'),
                "receiver_id": row.get('recipient_id'),
                "created_at": str(row.get('created_at')),
                "conversation_id": str(row.get('conversation_id')),
                "content": row.get('message_text'),
            }
            all_messages.append(message)

        # Sort messages by time descending (latest first)
        all_messages.sort(key=lambda msg: msg["created_at"], reverse=True)

        # Manual pagination
        total = len(all_messages)
        start = (page - 1) * limit
        end = start + limit
        paginated_messages = all_messages[start:end]

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": paginated_messages
        }
    
    @staticmethod
    async def get_messages_before_timestamp(*args, **kwargs):
        """
        Get messages before a timestamp with pagination.
        
        Students should decide how to implement filtering by timestamp with pagination.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def get_user_conversations(*args, **kwargs):
        """
        Get conversations for a user with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def get_conversation(*args, **kwargs):
        """
        Get a conversation by ID.
        
        Students should decide what parameters are needed and what data to return.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented")
    
    @staticmethod
    async def create_or_get_conversation(*args, **kwargs):
        """
        Get an existing conversation between two users or create a new one.
        
        Students should decide how to handle this operation efficiently.
        """
        # This is a stub - students will implement the actual logic
        raise NotImplementedError("This method needs to be implemented") 
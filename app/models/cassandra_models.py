import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from cassandra.util import uuid_from_time, unix_time_from_uuid1

from app.db.cassandra_client import cassandra_client

class MessageModel:
    
    @staticmethod
    async def create_message(conversation_id: str, sender_id: int, recipient_id: int, message_text: str):
        """
        Create a new message and return it in the response format.
        """

        message_id = uuid.uuid1()
        query = """
        INSERT INTO messages (conversation_id, message_id, sender_id, recipient_id, message_text)
        VALUES (%s, %s, %s, %s, %s)
        """
        cassandra_client.execute(query, (conversation_id, message_id, sender_id, recipient_id, message_text))

        update_user_conversations_query = """
        INSERT INTO user_conversations (user_id, last_message_time, conversation_id, receiver_id, last_message)
        VALUES (%s, %s, %s, %s, %s)
        """
        cassandra_client.execute(update_user_conversations_query, (sender_id, message_id, conversation_id, recipient_id, message_text))
        cassandra_client.execute(update_user_conversations_query, (recipient_id, message_id, conversation_id, sender_id, message_text))
        return {
            "id": str(message_id),
            "sender_id": sender_id,
            "receiver_id": recipient_id,
            "created_at": str(unix_time_from_uuid1(message_id)),
            "conversation_id": conversation_id,
            "content": message_text,
        }
        
    
    @staticmethod
    async def get_conversation_messages(conversation_id: str, page: int, limit: int):

        query = """
        SELECT * FROM messages WHERE conversation_id = %s
        """
        rows = cassandra_client.execute(query, (conversation_id,))
        
        all_messages = []
        for row in rows:
            message = {
                "id": str(row.get('message_id')),
                "sender_id": row.get('sender_id'),
                "receiver_id": row.get('recipient_id'),
                "created_at": str(unix_time_from_uuid1(row.get('message_id'))),
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
    async def get_messages_before_timestamp(conversation_id: str, before_timestamp: datetime, page: int, limit: int):
        """
        Get messages before a timestamp with pagination.
        """

        time_stamp = uuid_from_time(before_timestamp)
        query = """
        SELECT * FROM messages WHERE conversation_id = %s AND message_id < %s
        """
        rows = cassandra_client.execute(query, (conversation_id, time_stamp))
        print(rows)
        all_messages = []
        for row in rows:
            message = {
                "id": str(row.get('message_id')),
                "sender_id": row.get('sender_id'),
                "receiver_id": row.get('recipient_id'),
                "created_at": str(unix_time_from_uuid1(row.get('message_id'))),
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
        


class ConversationModel:
    
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20):
        """
        Fetches all conversations where the given user_id.
        Keeps only the latest conversation per receiver.
        """

        query = """
        SELECT * FROM user_conversations WHERE user_id = %s
        """
        rows = cassandra_client.execute(query, (user_id,))

        conversations_map = {}

        for row in rows:
            receiver_id = row.get("receiver_id")
            last_message_time = row.get("last_message_time")

            # Check if we already have a conversation with this receiver_id
            if receiver_id not in conversations_map or last_message_time > conversations_map[receiver_id]["last_message_time"]:
                conversations_map[receiver_id] = {
                    "id": row.get("conversation_id"),
                    "user1_id": user_id,
                    "user2_id": receiver_id,
                    "last_message_content": row.get("last_message"),
                    "last_message_time": last_message_time,
                    "last_message_at": str(unix_time_from_uuid1(last_message_time)),
                }
         
        sorted_conversations = sorted(conversations_map.values(), key=lambda x: x["last_message_time"], reverse=True)
        start, end = (page - 1) * limit, page * limit
        return {
            "total": len(sorted_conversations),
            "page": page,
            "limit": limit,
            "data": sorted_conversations[start:end]
        }


    @staticmethod
    async def get_conversation(conversation_id:str):
        """
        Get a conversation by ID.
        """

        query = """
        SELECT * FROM messages 
        WHERE conversation_id = %s 
        ORDER BY message_id DESC 
        LIMIT 1
        """
        rows = cassandra_client.execute(query, (conversation_id,))
        for row in rows:
            return {
                "id": row.get("conversation_id"),
                "user1_id": row.get("sender_id"),
                "user2_id": row.get("recipient_id"),
                "last_message_at": str(unix_time_from_uuid1(row.get("message_id"))),
                "conversation_id": row.get("conversation_id"),
                "last_message_content": str(row.get('message_text')),
            }
        return None
    
    @staticmethod
    async def create_or_get_conversation(sender_id: int, receiver_id: int):
        """
        Get an existing conversation between two users or create a new one.
        """

        conversation_ids = [
            f"{sender_id}_{receiver_id}",
            f"{receiver_id}_{sender_id}"
        ]

        for conversation_id in conversation_ids:
            query = """
            SELECT * FROM messages WHERE conversation_id = %s LIMIT 1
            """
            rows = cassandra_client.execute(query, (conversation_id,))
            if rows:
                return conversation_id

        return str(sorted([sender_id, receiver_id])[0]) + "_" + str(sorted([sender_id, receiver_id])[1])
            

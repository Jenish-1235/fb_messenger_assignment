import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import json
from pathlib import Path
from cassandra.util import uuid_from_time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise
    
def export_to_json(filename, data):
    Path("testdata").mkdir(exist_ok=True)
    with open(f"testdata/{filename}", 'w') as f:
        json.dump(data, f, indent=4, default=str)

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # TODO: Students should implement the test data generation logic
    # Hint:
    # 1. Create a set of user IDs
    # 2. Create conversations between random pairs of users
    # 3. For each conversation, generate a random number of messages
    # 4. Update relevant tables to maintain data consistency

    user_ids = list(range(1, NUM_USERS + 1))
    used_pairs = set()

    users = []
    conversations = []
    messages = []

    for user_id in user_ids:
        users.append({"user_id": user_id})
        session.execute(
            "INSERT INTO users (user_id) VALUES (%s)",
            (user_id,)
        )
    logger.info(f"Inserted {NUM_USERS} users into the database")

    conversations_created = 0

    while conversations_created < NUM_CONVERSATIONS:
        sender_id, receiver_id = random.sample(user_ids, 2)
        user_pair = tuple(sorted((sender_id, receiver_id)))
        if user_pair in used_pairs:
            continue
        used_pairs.add(user_pair)
        conversations_created += 1

        conversation_id = f"{user_pair[0]}_{user_pair[1]}"
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        start_time = datetime.now() - timedelta(days=random.randint(0, 30))
        last_message_time = None
        last_msg_txt = ""

        for i in range(num_messages):
            msg_time = start_time + timedelta(minutes=i)
            msg_uuid = uuid_from_time(msg_time)
            from_id, to_id = (sender_id , receiver_id) if i % 2 == 0 else (receiver_id, sender_id)
            msg_text = f"Message {i + 1} from {from_id} to {to_id}"

            msg = {
                "conversation_id": conversation_id,
                "message_id": msg_uuid,
                "sender_id": from_id,
                "receiver_id": to_id,
                "timestamp": msg_time,
                "text": msg_text
            }
            messages.append(msg)

            session.execute(
                "INSERT INTO messages (conversation_id, message_id, sender_id, recipient_id, message_text) "
                "VALUES (%s, %s, %s, %s, %s)",
                (conversation_id, msg_uuid, from_id, to_id, msg_text)
            )

            
            last_message_time = msg_uuid
            last_msg_txt = msg_text

            for user_id, other_user_id in [(sender_id, receiver_id) , (receiver_id, sender_id)]:
                convo = {
                    "user_id": user_id,
                    "last_message_time": str(last_message_time),
                    "conversation_id": conversation_id,
                    "receiver_id": other_user_id,
                    "last_msg_txt": last_msg_txt
                }
                conversations.append(convo) 
                session.execute(
                    "INSERT INTO user_conversations (user_id, last_message_time, conversation_id, receiver_id, last_message) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (user_id, last_message_time, conversation_id, other_user_id, last_msg_txt)
                )

    export_to_json("users.json", users)
    export_to_json("conversations.json", conversations)
    export_to_json("messages.json", messages)               
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 
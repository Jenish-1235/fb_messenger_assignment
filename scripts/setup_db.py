import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    
    for _ in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5) 
    
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")
    query = f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 3 }};
    """
    session.execute(query)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    logger.info("Creating tables...")
    session.set_keyspace(CASSANDRA_KEYSPACE)

    user_table_query = f"""
    CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY
    )
    """
    session.execute(user_table_query)

    conversation_table_query = f"""
    CREATE TABLE IF NOT EXISTS user_conversations (
    user_id int,
    last_message_time timeuuid,
    conversation_id text,
    receiver_id int,
    last_message text,
    PRIMARY KEY (user_id, last_message_time, conversation_id)
    ) WITH CLUSTERING ORDER BY (last_message_time DESC);
    """
    session.execute(conversation_table_query)

    message_table_query = f"""
    CREATE TABLE IF NOT EXISTS messages (
    conversation_id text,
    message_id timeuuid,
    sender_id int,
    recipient_id int,
    message_text text,
    PRIMARY KEY (conversation_id, message_id)
    ) WITH CLUSTERING ORDER BY (message_id DESC);
    """

    session.execute(message_table_query)
    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")
    
    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()
    
    try:
        # Connect to the server
        session = cluster.connect()
        
        # Create keyspace and tables
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)
        
        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main() 
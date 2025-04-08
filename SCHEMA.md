# FB - Messenger Assignment

## Task Requirements

Design a Cassandra schema to support the following features:

- Sending messages between users
- Fetching user conversations ordered by recent activity
- Fetching all messages in a conversation
- Fetching messages before a given timestamp (for pagination)

You are required to:

- Create appropriate tables as per the schema
- Implement the schema using `setup_db.py`

---

### Some Considerations made for Ease as of testing guidance provided :

- Given that user_id will be 1-[Max Users]
- Decided to take convo*id as "con*<Sender*id>*<Receiver_id>"
- msg_id will be time_stamp based uuids.

---

## Cassandra Schema

### Table 1: `users`

This table stores all the User IDs.

#### **Schema Design**

| Column Name | Type | Description          |
| ----------- | ---- | -------------------- |
| user_id     | int  | Primary key (row ID) |

> **Note:** In real-world applications, user data usually includes profile pictures, emails, passwords, etc. However, for this assignment, we’re focusing only on storing the `user_id`.

### Table 2: `user_conversations`

This table stores mapping of user_ids to conversation_ids and metadata like last message preview, timestamp etc.

#### **Schema Design**

| Column Name | Type     | Description                          |
| ----------- | -------- | ------------------------------------ |
| user_id     | int      | Partition key                        |
| last_msg_ts | timeuuid | Clustering key (for sorting DESC)    |
| convo_id    | text     | Clustering key (for uniqueness)      |
| receiver_id | int      | ID of the other user in conversation |
| last_msg    | text     | Preview of the last message          |

#### **Primary Key:**

```cql
PRIMARY KEY ((user_id), last_msg_ts, convo_id)
```

> **Explanation:** Here we are using user_id, convo_id and last_msg_ts as primary key as cassandra do not have primary key just as unique contraint but it uses it for partitioning, sorting etc. too.
> Here we will use user_id as row_id so that partition of table happens on that basis and last_msg_time will be helping to cluster and sort the table in descending order and convo_id will help to resolve any conflicts of messages being sent at exact same time.

### Table 3: `messages`

This table will store all the messages of conversations.

#### **Schema Design**

| Column Name | Type     | Description                 |
| ----------- | -------- | --------------------------- |
| convo_id    | text     | Partition key               |
| msg_id      | timeuuid | Clustering key (Descending) |
| sender_id   | int      | ID of the sender            |
| receiver_id | int      | ID of the receiver          |
| message     | text     | Message content             |

#### **Primary Key:**

```cql
PRIMARY KEY ((convo_id), msg_id)
```

> **Explanation:** Here we will be using convo_id as row_id to partition the table such that messages of one conversation stays on one partition. We will be using msg_ts (message timestamp) as clustering key and order the table in descending order on that basis and will use msg_id to resolve conflicts of messages sent on same time. Also msg_id will provide us ease to extend the system to enable delete message functionality easily.

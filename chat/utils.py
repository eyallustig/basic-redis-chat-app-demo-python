import json
import math
import random
import logging

import bcrypt

from chat import demo_data
from chat.config import get_config

SERVER_ID = random.uniform(0, 322321)
logger = logging.getLogger(__name__)

redis_client = get_config().redis_client


def make_username_key(username):
    return f"username:{username}"


def create_user(username, password):
    username_key = make_username_key(username)
    # Create a user
    hashed_password = bcrypt.hashpw(str(password).encode("utf-8"), bcrypt.gensalt(10))
    next_id = redis_client.incr("total_users")
    user_key = f"user:{next_id}"
    redis_client.set(username_key, user_key)
    redis_client.hmset(user_key, {"username": username, "password": hashed_password})

    redis_client.sadd(f"user:{next_id}:rooms", "0")

    user = {"id": next_id, "username": username}
    logger.info("Created new user id=%s username=%s", next_id, username)
    return user


def get_messages(room_id=0, offset=0, size=50):
    """Check if room with id exists; fetch messages limited by size"""
    room_key = f"room:{room_id}"
    room_exists = redis_client.exists(room_key)
    if not room_exists:
        logger.info(
            "Requested messages for non-existent room_id=%s; returning empty list",
            room_id,
        )
        return []
    else:
        values = redis_client.zrevrange(room_key, offset, offset + size)
        messages = list(map(lambda x: json.loads(x.decode("utf-8")), values))
        logger.info(
            "Fetched %d messages for room_id=%s offset=%s size=%s",
            len(messages),
            room_id,
            offset,
            size,
        )
        return messages


def hmget(key, key2):
    """Wrapper around hmget to unpack bytes from hmget"""
    result = redis_client.hmget(key, key2)
    return list(map(lambda x: x.decode("utf-8"), result))


def get_private_room_id(user1, user2):
    if math.isnan(user1) or math.isnan(user2) or user1 == user2:
        return None
    min_user_id = user2 if user1 > user2 else user1
    max_user_id = user1 if user1 > user2 else user2
    return f"{min_user_id}:{max_user_id}"


def create_private_room(user1, user2):
    """Create a private room and add users to it"""
    room_id = get_private_room_id(user1, user2)
    if not room_id:
        return None, True

    # Add rooms to those users
    redis_client.sadd(f"user:{user1}:rooms", room_id)
    redis_client.sadd(f"user:{user2}:rooms", room_id)

    return (
        {
            "id": room_id,
            "names": [
                hmget(f"user:{user1}", "username"),
                hmget(f"user:{user2}", "username"),
            ],
        },
        False,
    )


def init_redis():
    # We store a counter for the total users and increment it on each register
    total_users_exist = redis_client.exists("total_users")
    if not total_users_exist:
        # This counter is used for the id
        redis_client.set("total_users", 0)
        # Some rooms have pre-defined names. When the clients attempts to fetch a room, an additional lookup
        # is handled to resolve the name.
        # Rooms with private messages don't have a name
        redis_client.set(f"room:0:name", "General")

        logger.info("Initializing Redis with demo data")
        demo_data.create()
    else:
        logger.info("Redis already initialized; skipping demo data creation")

# We use event stream for pub sub. A client connects to the stream endpoint and listens for the messages


def event_stream():
    """Handle message formatting, etc."""
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("MESSAGES")
    logger.info("Subscribed to Redis pubsub channel MESSAGES; server_id=%s", SERVER_ID)
    for message in pubsub.listen():
        message_parsed = json.loads(message["data"])
        if message_parsed["serverId"] == SERVER_ID:
            continue

        data = "data:  %s\n\n" % json.dumps(
            {"type": message_parsed["type"], "data": message_parsed["data"],}
        )
        yield data
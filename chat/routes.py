import json
import os
import logging

import bcrypt
from flask import Response, jsonify, request, session

from chat import utils
from chat.app import app
from chat.auth import auth_middleware


logger = logging.getLogger(__name__)


@app.route("/stream")
def stream():
    logger.info("/stream endpoint subscribed for server-sent events")
    return Response(utils.event_stream(), mimetype="text/event-stream")


# Return our SPA application.
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def catch_all(path):
    return app.send_static_file("index.html")


# This check if the session contains the valid user credentials
@app.route("/me")
def get_me():
    user = session.get("user", None)
    logger.info("/me requested; user=%s", user and user.get("username"))
    return jsonify(user)


@app.route("/links")
def get_links():
    """Returns JSON with available deploy links"""
    # Return github link to the repo
    repo = open(os.path.join(app.root_path, "../repo.json"))
    data = json.load(repo)
    logger.info("/links requested; returning deploy links")
    return jsonify(data)


@app.route("/login", methods=["POST"])
def login():
    """For now, just simulate session behavior"""
    # TODO
    data = request.get_json()
    username = data["username"]
    password = data["password"]

    username_key = utils.make_username_key(username)
    user_exists = utils.redis_client.exists(username_key)
    logger.info("Login attempt for username=%s; user_exists=%s", username, bool(user_exists))
    if not user_exists:
        new_user = utils.create_user(username, password)
        session["user"] = new_user
        logger.info("Login successful for NEW username=%s", username)
        return new_user, 200
    else:
        user_key = utils.redis_client.get(username_key).decode("utf-8")
        data = utils.redis_client.hgetall(user_key)
        if (
            bcrypt.hashpw(password.encode("utf-8"), data[b"password"])
            == data[b"password"]
        ):
            user = {"id": user_key.split(":")[-1], "username": username}
            session["user"] = user
            logger.info("Login successful for username=%s", username)
            return user, 200

    logger.warning("Login failed for username=%s", username)
    return jsonify({"message": "Invalid username or password"}), 404


@app.route("/logout", methods=["POST"])
@auth_middleware
def logout():
    logger.info("Logout requested for user=%s", session.get("user"))
    session["user"] = None
    return jsonify(None), 200


@app.route("/users/online")
@auth_middleware
def get_online_users():
    online_ids = map(
        lambda x: x.decode("utf-8"), utils.redis_client.smembers("online_users")
    )
    users = {}
    for online_id in online_ids:
        user = utils.redis_client.hgetall(f"user:{online_id}")
        users[online_id] = {
            "id": online_id,
            "username": user.get(b"username", "").decode("utf-8"),
            "online": True,
        }
    logger.info("/users/online requested; count=%d", len(users))
    return jsonify(users), 200


@app.route("/rooms/<user_id>")
@auth_middleware
def get_rooms_for_user_id(user_id=0):
    """Get rooms for the selected user."""
    # We got the room ids
    room_ids = list(
        map(
            lambda x: x.decode("utf-8"),
            list(utils.redis_client.smembers(f"user:{user_id}:rooms")),
        )
    )
    rooms = []

    for room_id in room_ids:
        name = utils.redis_client.get(f"room:{room_id}:name")

        # It's a room without a name, likey the one with private messages
        if not name:
            room_exists = utils.redis_client.exists(f"room:{room_id}")
            if not room_exists:
                continue

            user_ids = room_id.split(":")
            if len(user_ids) != 2:
                return jsonify(None), 400

            rooms.append(
                {
                    "id": room_id,
                    "names": [
                        utils.hmget(f"user:{user_ids[0]}", "username"),
                        utils.hmget(f"user:{user_ids[1]}", "username"),
                    ],
                }
            )
        else:
            rooms.append({"id": room_id, "names": [name.decode("utf-8")]})
    logger.info("/rooms/%s requested; rooms_count=%d", user_id, len(rooms))
    return jsonify(rooms), 200


@app.route("/room/<room_id>/messages")
@auth_middleware
def get_messages_for_selected_room(room_id="0"):
    offset = request.args.get("offset")
    size = request.args.get("size")

    try:
        messages = utils.get_messages(room_id, int(offset), int(size))
        logger.info(
            "/room/%s/messages requested; offset=%s size=%s returned=%d",
            room_id,
            offset,
            size,
            len(messages),
        )
        return jsonify(messages)
    except Exception as exc:
        logger.exception("Failed to get messages for room_id=%s", room_id)
        return jsonify(None), 400


@app.route("/users")
def get_user_info_from_ids():
    ids = request.args.getlist("ids[]")
    if ids:
        users = {}
        for id in ids:
            user = utils.redis_client.hgetall(f"user:{id}")
            is_member = utils.redis_client.sismember("online_users", id)
            users[id] = {
                "id": id,
                "username": user[b"username"].decode("utf-8"),
                "online": bool(is_member),
            }
        logger.info("/users requested for ids=%s; returned_count=%d", ids, len(users))
        return jsonify(users)
    logger.warning("/users requested without ids; returning 404")
    return jsonify(None), 404

import hashlib

import secrets
from .swen344_db_utils import *


def hash_password(password):
    hasher = hashlib.new('sha512', b'Salty')
    hasher.update(password.encode('utf-8'))
    hashed_password = hasher.digest()
    return hashed_password


def check_login(user_type, user_name, hashed_password):
    conn = connect()
    cur = conn.cursor()

    if user_type == "d":
        cur.execute("""SELECT password FROM drivers WHERE user_name = (%s);""", (user_name,))
        test = cur.fetchone()
    else:
        cur.execute("SELECT password FROM riders WHERE user_name = (%s);""", (user_name,))
        test = cur.fetchone()

    if test:
        if test[0] == hashed_password.hex():
            result = "Valid Authentication"
        else:
            result = "Invalid Password"
    else:
        result = "Invalid username"
    conn.close()
    return [user_type, result]


def check_session_key(session_key):
    conn = connect()
    cur = conn.cursor()
    valid = False
    cur.execute("SELECT session_key from drivers;")
    tests = cur.fetchall()
    for test in tests:
        if test[0] == session_key:
            valid = True
            break
    cur.execute("SELECT session_key FROM riders;")
    tests = cur.fetchall()
    for test in tests:
        if test[0] == session_key:
            valid = True
            break
    return valid


def create_session_key(user_type, user_name, valid_authentication):
    conn = connect()
    cur = conn.cursor()
    if valid_authentication == "Valid Authentication":
        session_key = secrets.token_hex(16)
        if user_type == "d":
            cur.execute("UPDATE drivers SET session_key = (%s) WHERE user_name = (%s);", (session_key,user_name))
            conn.commit()
        else:
            cur.execute("UPDATE riders SET session_key = (%s) WHERE user_name = (%s);", (session_key, user_name))
            conn.commit()
        conn.close()
        return session_key
    else:
        return valid_authentication


def destroy_session_key(user_type, user_name, valid_authentication):
    conn = connect()
    cur = conn.cursor()
    if valid_authentication == "Valid Authentication":
        session_key = None
        if user_type == "d":
            cur.execute("UPDATE drivers SET session_key = (%s) WHERE user_name =(%s)", (session_key, user_name))
            conn.commit()
        else:
            cur.execute("UPDATE riders SET session_key = (%s) WHERE user_name =(%s) ", (session_key, user_name))
            conn.commit()
        conn.close()
        return session_key
    elif valid_authentication == "Invalid Password":
        return "INVALID PASSWORD"
    else:
        return "INVALID USERNAME"


def login(user_type, username, hashed_password):
    result = check_login(user_type, username, hashed_password)
    return create_session_key(user_type, username, result[1])


def logout(user_type, username, hashed_password):
    result = check_login(user_type, username, hashed_password)
    return destroy_session_key(user_type, username, result[1])

# PostgreSQL database adapter/driver - allows us to connect to the DBMS and perform operations
import psycopg2

# YAML is a data serialization language, like json, it allows for translating a data structure or object state into a
# format that can be stored (for example, in a file like yml or memory data buffer) or transmitted (for example, over a
# computer network) and reconstructed later (possibly in a different computer environment).
import yaml

# provides functions for interacting with our operating system
import os


def connect():
    """
    Will connect you to Postgres via our config. Closing this connection is up to you.
    :return: Connection to swen344 DB
    """
    yml_path = os.path.join(os.path.dirname(__file__), '../../config/db.yml')
    with open(yml_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    # connect method connects to a new DB session and returns a connection object
    return psycopg2.connect(dbname=config['database'],
                            user=config['user'],
                            password=config['password'],
                            host=config['host'],
                            port=config['port'])


def exec_sql_file(path):
    """
    Will open up an SQL file and blindly execute everything in it. Useful for test data and your schema. Having your
    code in a SQL file also gives syntax highlighting! :param path: path of file :return:None
    """
    full_path = os.path.join(os.path.dirname(__file__), f'../../{path}')
    conn = connect()
    cur = conn.cursor()  # cursor allows for us to execute PostgreSQL command in a database session.they are bound to
    # the
    # connection for the entire lifetime and all the commands are executed in the context of the database session
    # wrapped by the connection.
    with open(full_path, 'r') as file:
        cur.execute(file.read())
    conn.commit()
    conn.close()


def exec_get_one(sql, args=None):
    """
     Will run a query and assume that you only want the top result and return that. It does not commit any changes, so
    don’t use it for updates.
    :param sql: SQL command
    :param args: a sequence of values(tuple) that are linked to the SQL VALUES command
    :return: Top result of query
    """
    if args is None:
        args = {}
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    one = cur.fetchone()
    conn.close()
    return one


def exec_get_all(sql, args=None):
    """
    Will run a query and return all results, usually as a list of tuples. It does not commit any changes, so don’t use
    it for updates.
    :param sql: SQL command
    :param args: a sequence of values(tuple) that are linked to the SQL VALUES command
    :return: return all results of query
    """
    if args is None:
        args = {}
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
    list_of_tuples = cur.fetchall()
    conn.close()
    return list_of_tuples


def exec_commit(sql, args=None):
    """
    Will run SQL and then do a commit operation, so use this for updating code. Connection isn't closed.
    :param sql: list of SQL commands
    :param args: a sequence of values(tuple) that are linked to the SQL VALUES command
    :return: The connection to the DB
    """
    if args is None:
        args = {}
    conn = connect()
    cur = conn.cursor()
    if isinstance(sql, list):
        for cmds in sql:
            cur.execute(cmds, args)  # execute returns None everytime - if query is completed you must use the fetch
            # methods if you want to read data from the DB after an execute command
    else:
        cur.execute(sql, args)
    conn.commit()  # Commit any pending transaction to the database
    return conn

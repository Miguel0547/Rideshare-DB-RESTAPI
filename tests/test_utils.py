import requests
from src.db.swen344_db_utils import connect


def assert_sql_count(test, sql, n, msg='Expected row count did not match query'):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql)
    test.assertEqual(n, cur.rowcount, msg)
    conn.close()


def get_rest_call(test, url, params={}, get_header={}, expected_code=200):
    response = requests.get(url, params, headers=get_header)
    test.assertEqual(expected_code, response.status_code,
                     f'Response code to {url} not {expected_code}')
    return response.json()


def post_rest_call(test, url, params={}, post_header={}, expected_code=200):
    '''Implements a REST api using the POST verb'''
    response = requests.post(url, params, headers=post_header)
    test.assertEqual(expected_code, response.status_code,
                     f'Response code to {url} not {expected_code}')
    return response.json()


def put_rest_call(test, url, params={}, put_header={}, expected_code=200):
    '''Implements a REST api using the PUT verb'''
    response = requests.put(url, params, headers=put_header)
    test.assertEqual(expected_code, response.status_code,
                     f'Response code to {url} not {expected_code}')
    return response.json()


def delete_rest_call(test, url, get_header={}, expected_code=200):
    '''Implements a REST api using the DELETE verb'''
    response = requests.delete(url, headers=get_header)
    test.assertEqual(expected_code, response.status_code,
                     f'Response code to {url} not {expected_code}')
    return response.json()

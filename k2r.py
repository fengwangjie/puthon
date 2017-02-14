from collections import OrderedDict

import petl as etl
import pymysql

import config
from common import datetime2
from common.petl import mysqldb


def _connect(conn):
    return pymysql.connect(database=conn['database'],
                           host=conn['host'],
                           user=conn['user'],
                           password=conn['password'],
                           port=int(conn['port']),
                           charset='utf8')


def sync_users(school_id, org_id, src_conn, dst_conn):
    sql = '''
    select * from auth_user
    where school_id = {}
    AND (student_id IS NOT NULL OR teacher_id IS NOT NULL)
    '''.format(school_id)

    src_table = etl.fromdb(src_conn, sql)

    mapping = OrderedDict()
    mapping['org_id'] = lambda x: org_id
    mapping['username'] = 'username'
    mapping['name'] = 'full_name'
    mapping['uid'] = 'uid'
    mapping['phone_no'] = 'phone'
    mapping['status'] = 'status'
    mapping['type'] = lambda x: 'FACULTY' if x['teacher_id'] else 'STUDENT'
    mapping['updated_at'] = lambda x: datetime2.utc()

    dst_table = etl.fieldmap(src_table, mapping)

    dst_conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
    mysqldb.upsertdb(dst_table, dst_conn, 'auth_user', 'uid', commit=True)


def sync_role(conn, org_id):
    src_table = etl.fromdb(conn, "select * from auth_user where org_id = {} and `type` <> 'ORG_ADMIN'".format(org_id))

    mapping = OrderedDict()
    mapping['user_id'] = 'id'
    mapping['role_id'] = lambda x: 2 if x['type'] == 'FACULTY' else 3
    mapping['updated_at'] = lambda x: datetime2.utc()

    dst_table = etl.fieldmap(src_table, mapping)

    conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
    mysqldb.upsertdb(dst_table, conn, 'auth_user_role', ('user_id', 'role_id'), commit=True)


def main():

    src_conn = _connect(config.kedurp['conn'])
    dst_conn = _connect(config.roomis['conn'])

    sync_users(config.kedurp['school_id'], config.roomis['org_id'], src_conn, dst_conn)
    sync_role(dst_conn, config.roomis['org_id'])

if __name__ == '__main__':
    main()

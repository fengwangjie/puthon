import luigi

import petl as etl
from collections import OrderedDict

from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb

import config
from roomis.student import RoomisStudentUserTask
from roomis.teacher import RoomisTeacherUserTask


class RoomisUserRoleTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [RoomisStudentUserTask(self.interval, self.host, self.database, self.user, self.password),
                RoomisTeacherUserTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        src_table = etl.fromdb(conn, "select id, uid, `type` from auth_user where `type` <> 'ORG_ADMIN'")

        mapping = OrderedDict()
        mapping['org_id'] = lambda x: config.roomis['org_id']
        mapping['subject_id'] = 'id'
        mapping['role_id'] = lambda x: 2 if x['type'] == 'FACULTY' else 3
        mapping['subject_type'] = lambda x: 'USER'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, self.table, ('role_id', 'subject_id', 'subject_type'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)

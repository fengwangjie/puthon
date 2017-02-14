import luigi
import petl as etl
from collections import OrderedDict

import config
import staging
from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb


class RoomisTeacherUserTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = staging.teachers()

        mapping = OrderedDict()
        mapping['org_id'] = lambda x: config.roomis['org_id']
        mapping['name'] = 'teacher_name'
        mapping['uid'] = 'teacher_no'
        mapping['username'] = 'teacher_no'
        mapping['type'] = lambda x: "FACULTY"
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "auth_user", 'uid', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class RoomisTeacherCardTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return RoomisTeacherUserTask(self.interval, self.host, self.database, self.user, self.password)

    def run(self):
        conn = self.output().connect()

        student_table = staging.teachers()
        user_table = etl.fromdb(conn, "select id as user_id, uid as teacher_no from auth_user")

        src_table = etl.join(student_table, user_table, key="teacher_no")

        mapping = OrderedDict()
        mapping['org_id'] = lambda x: config.roomis['org_id']
        mapping['user_id'] = 'user_id'
        mapping['no'] = 'card_no'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_card", ("user_id", "no"), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)



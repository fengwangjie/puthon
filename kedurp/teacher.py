import luigi
import petl as etl
from collections import OrderedDict

import config
import staging
from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb


class KedurpTeacherTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = staging.teachers()

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'teacher_name'
        mapping['uid'] = 'teacher_no'
        mapping['phone'] = 'phone'
        mapping['email'] = 'email'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['created_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_teacher", 'uid', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeacherUserTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password)

    def run(self):
        src_table = etl.fromdb(self.input().connect(), "select * from core_teacher")

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['username'] = 'uid'
        mapping['full_name'] = 'name'
        mapping['uid'] = 'uid'
        mapping['phone'] = 'phone'
        mapping['email'] = 'email'
        mapping['teacher_id'] = 'id'
        mapping['status'] = 'status'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "auth_user", 'uid', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table,
                                     self.task_id)

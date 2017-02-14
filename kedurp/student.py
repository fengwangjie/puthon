import luigi
import petl as etl
from collections import OrderedDict

import config
import staging
from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb


class KedurpStudentTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = staging.students()

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'student_name'
        mapping['uid'] = 'student_no'
        mapping['student_no'] = 'student_no'
        mapping['gender'] = lambda x: 'MALE' if x['gender'] else 'FEMALE'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['created_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_student", 'uid', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpStudentUserTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return KedurpStudentTask(self.interval, self.host, self.database, self.user, self.password)

    def run(self):
        src_table = etl.fromdb(self.input().connect(), "select * from core_student")

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['username'] = 'uid'
        mapping['full_name'] = 'name'
        mapping['uid'] = 'uid'
        mapping['student_id'] = 'id'
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


class KedurpGradeTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = etl.distinct(staging.students(), key='grade_name')

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['code'] = 'grade_name'
        mapping['name'] = 'grade_name'
        mapping['status'] = lambda x: 'ACTIVE'
        mapping['created_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_grade", 'name', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)

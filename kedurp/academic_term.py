import luigi
import petl as etl
from collections import OrderedDict

from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb

import staging
import config


class KedurpAcademicTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = staging.academics()

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['academic_year'] = 'academic_year'
        mapping['academic_term'] = 'academic_term'
        mapping['start_date'] = 'start_date'
        mapping['end_date'] = 'end_date'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        # mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_academic_term", ('academic_year', 'academic_term'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)

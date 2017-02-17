import luigi
import petl as etl
from collections import OrderedDict

import config
import staging
from common import datetime2
from common.luigi import pymysqldb
from common.petl import mysqldb
import kedurp
from kedurp.academic_term import KedurpAcademicTask
from kedurp.admin_course import KedurpTimeTableTask
from kedurp.student import KedurpGradeTask, KedurpStudentTask
from kedurp.teacher import KedurpTeacherTask


class KedurpTeachingClassRoomTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = etl.distinct(staging.teaching_courses(), 'class_room')
        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'class_room'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        # mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_room', 'name', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeachingClassTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpAcademicTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             'select id as academic_id, academic_year, academic_term from core_academic_term'),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        teacher_table = etl.fromdb(conn, "select id as teacher_id, uid as teacher_no from core_teacher")

        course_table = etl.join(etl.distinct(staging.teaching_courses(), 'class_name'),
                                etl.fromdb(conn, "select id as room_id, name as room_name from core_room"),
                                lkey="class_room", rkey="room_name")

        grade_name_table = etl.leftjoin(course_table,
                                        staging.students(),
                                        key='student_no')
        grade_id_table = etl.leftjoin(grade_name_table, etl.fromdb(conn,
                                                                   'select DISTINCT id as grade_id,name as grade_name from core_grade WHERE  school_id={}'.format(
                                                                       config.kedurp['school_id'])), key='grade_name')
        src_table = etl.join(grade_id_table, teacher_table, key="teacher_no")

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'class_name'
        mapping['code'] = lambda x: "{}-{}-{}".format(academic.academic_year, academic.academic_term, x['class_name'])

        mapping['head_teacher_id'] = 'teacher_id'
        mapping['academic_term_id'] = lambda x: academic.academic_id
        mapping['klass_type'] = lambda x: 'TEACHING'
        mapping['room_id'] = 'room_id'
        mapping['grade_id'] = 'grade_id'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_class", 'name', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeachingClassStudentTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpTeachingClassTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpStudentTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             'select id as academic_id, academic_year, academic_term from core_academic_term'),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        class_table = etl.fromdb(conn, '''SELECT cc.id as class_id, cc.name as class_name FROM core_class AS cc
                        where cc.klass_type = 'TEACHING'
                        and cc.academic_term_id = {}'''.format(academic.academic_id))

        student_table = etl.join(staging.teaching_courses(),
                                 etl.fromdb(conn, "select id as student_id, student_no from core_student"),
                                 key='student_no')

        src_table = etl.join(student_table, class_table, key='class_name')

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['class_id'] = 'class_id'
        mapping['student_id'] = 'student_id'
        mapping['updated_at'] = lambda x: datetime2.utc()
        # mapping['grade_id'] = 'grade_id'
        dst_table = etl.fieldmap(src_table, mapping)

        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_class_student", ('student_id', 'class_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeachingCourseTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpGradeTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeachingClassRoomTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAcademicTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             '''select id as academic_id, academic_year, academic_term
                                                  from core_academic_term'''),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        class_table = etl.fromdb(conn, '''SELECT cc.id AS class_id, cc.name AS class_name, cc.room_id, cc.school_id as school_id
                                            FROM core_class as cc
                                           WHERE cc.academic_term_id={}'''.format(academic.academic_id))

        course_room_table = etl.join(etl.distinct(staging.teaching_courses(), key=['course', 'course_type']),
                                     etl.fromdb(conn,
                                                'select id AS room_id, name AS class_room, school_id as school_id from core_room'),
                                     key=['class_room'])

        src_table = etl.join(class_table, course_room_table, key=['room_id'])

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'course'
        mapping['course_type'] = lambda x: 'OPTIONAL'
        mapping['class_id'] = 'class_id'
        mapping['periods'] = 'slot'
        mapping['num_consec_periods'] = lambda x: 0
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['version'] = lambda x: '{}{}'.format(academic.academic_year, academic.academic_term)

        mapping['academic_term_id'] = lambda x: academic.academic_id
        mapping['room_id'] = 'room_id'
        # mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_course', 'name', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeachingCourseTeacherTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpTeachingCourseTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpGradeTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             '''select id as academic_id, academic_year, academic_term
                                                  from core_academic_term'''),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        course_table = etl.join(etl.fromdb(conn, '''select cc.id as course_id
                                                            ,cc.name as course
                                                            ,cl.name as class_name
                                                        from core_course cc
                                                        INNER join core_class cl
                                                       where cc.class_id = cl.id
                                                         and cl.klass_type = 'TEACHING'
                                                         and cl.academic_term_id={}'''.format(academic.academic_id)),
                                etl.distinct(staging.teaching_courses(), key=['teacher_no', 'course', 'course_type']),
                                key=['class_name', 'course'])

        teacher_table = etl.fromdb(conn, 'select uid AS teacher_no, id AS teacher_id from core_teacher')

        src_table = etl.join(course_table, teacher_table, key='teacher_no')

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['teacher_id'] = 'teacher_id'
        mapping['course_id'] = 'course_id'
        # mapping['updated_at'] = lambda x: datetime2.utc()
        # mapping['created_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_course_teacher', ('teacher_id', 'course_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTeachingTimeTableItemTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpTimeTableTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeachingCourseTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeachingCourseTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             '''select id as academic_id, academic_year, academic_term
                                                  from core_academic_term'''),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        teaching_course_table = etl.distinct(staging.teaching_courses(),
                                             key=['course', 'course_type', 'class_name', 'day_of_week', 'slot',
                                                  'teacher_no', 'class_room'])

        course_table = etl.fromdb(conn,
                                  '''select cc.id as course_id, cc.class_id as class_id,cc.room_id as room_id,
                                                    cc.name as course, ct.teacher_id as teacher_id,
                                                    tt.id as timetable_id, tt.week as week,
                                                    c.name as class_name
                                       from core_course cc
                                           ,core_course_teacher ct
                                           ,timetable tt
                                           ,core_class c
                                     where cc.id = ct.course_id
                                       and tt.academic_term_id = cc.academic_term_id
                                       and c.id = cc.class_id
                                       and c.klass_type = 'TEACHING'
                                       and cc.academic_term_id={}'''.format(academic.academic_id))

        course_table = etl.join(teaching_course_table, course_table, key=['class_name', 'course', 'week'])
        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['timetable_id'] = 'timetable_id'
        mapping['course_id'] = 'course_id'
        mapping['class_id'] = 'class_id'
        mapping['teacher_id'] = 'teacher_id'
        mapping['room_id'] = 'room_id'
        mapping['day_week'] = 'week'
        mapping['begin_slot'] = 'slot'

        mapping['end_slot'] = 'slot'
        mapping['date'] = lambda x: kedurp.date_in(academic.start_date, academic.end_date, x['week'],
                                                   x['day_of_week'])
        mapping['slot_id'] = lambda x: 1

        # mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(course_table, mapping)

        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, 'timetable_item',
                         ('course_id', 'class_id', 'slot_id', 'begin_slot', 'date', 'timetable_id'),
                         commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)

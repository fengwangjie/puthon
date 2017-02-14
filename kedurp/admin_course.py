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
from kedurp.student import KedurpGradeTask, KedurpStudentTask
from kedurp.teacher import KedurpTeacherTask


#
# class KedurpSubjectTask(luigi.Task):
#     interval = luigi.DateMinuteParameter()
#
#     host = luigi.Parameter()
#     database = luigi.Parameter()
#     user = luigi.Parameter()
#     password = luigi.Parameter()
#     table = luigi.Parameter()
#
#     def run(self):
#         src_table = etl.distinct(staging.admin_courses(), 'class_room')
#         mapping = OrderedDict()
#
#         mapping['school_id'] = lambda x: config.kedurp['school_id']
#         mapping['name'] = 'course'
#         mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
#         mapping['updated_at'] = lambda x: datetime2.utc()
#
#         dst_table = etl.fieldmap(src_table, mapping)
#         conn = self.output().connect()
#         conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
#         mysqldb.upsertdb(dst_table, conn, 'core_subject', 'name', commit=True)
#         self.output().touch()
#
#     def output(self):
#         return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminCourseRoomTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def run(self):
        src_table = etl.distinct(staging.admin_courses(), 'class_room')
        mapping = OrderedDict()

        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'class_room'
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_room', 'name', commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminClassTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpAcademicTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpGradeTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             'select id as academic_id, academic_year, academic_term from core_academic_term'),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        teacher_table = etl.fromdb(conn,
                                   "select id as teacher_id, name as teacher_name, uid as teacher_no from core_teacher")
        grade_table = etl.fromdb(conn, "select id as grade_id, name as grade_name from core_grade")

        student_grade = etl.join(staging.students(), grade_table,
                                 key='grade_name')

        student_grade_room = etl.join(etl.join(student_grade, staging.admin_courses(),
                                               key=['class_name', 'grade_name']),
                                      etl.fromdb(conn, "select id as room_id, name as room_name from core_room"),
                                      lkey="class_room", rkey="room_name")

        src_table = etl.distinct(etl.join(student_grade_room, teacher_table, key='teacher_no'),
                                 key=['class_name', 'grade_name'])

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'class_name'
        mapping['code'] = lambda x: "{}-{}-{}{}".format(academic.academic_year, academic.academic_term,
                                                        x['grade_name'], x['class_name'])
        mapping['grade_id'] = 'grade_id'
        mapping['head_teacher_id'] = 'teacher_id'
        mapping['academic_term_id'] = lambda x: academic.academic_id
        mapping['klass_type'] = lambda x: 'ADMINISTRATION'
        mapping['room_id'] = 'room_id'

        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_class", ('name', 'grade_id', 'academic_term_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminClassStudentTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return KedurpStudentTask(self.interval, self.host, self.database, self.user, self.password), \
               KedurpAdminClassTask(self.interval, self.host, self.database, self.user, self.password)

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             'select id as academic_id, academic_year, academic_term from core_academic_term'),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        class_table = etl.fromdb(conn, '''SELECT cc.id as class_id, cc.name as class_name, cg.name as grade_name FROM core_class AS cc
                        INNER JOIN core_grade AS cg
                        WHERE cc.grade_id = cg.id
                        AND cc.academic_term_id = {}'''.format(academic.academic_id))

        student_table = etl.join(staging.students(),
                                 etl.fromdb(conn, "select id as student_id, student_no from core_student"),
                                 key='student_no')

        src_table = etl.join(student_table, class_table, key=['grade_name', 'class_name'])

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['class_id'] = 'class_id'
        mapping['student_id'] = 'student_id'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)

        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, "core_class_student", ('student_id', 'class_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminCourseTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpGradeTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAdminClassTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAdminCourseRoomTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAcademicTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             '''select id as academic_id, academic_year, academic_term
                                                  from core_academic_term'''),
                                  key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        class_grade = etl.fromdb(conn, '''SELECT cc.id AS class_id, cc.name AS class, cg.id AS grade_id, cg.name AS grade_name, cc.school_id as school_id
                                            FROM core_class as cc INNER JOIN core_grade AS cg ON cc.grade_id = cg.id
                                           WHERE cc.academic_term_id={}'''.format(academic.academic_id))

        course_room = etl.join(
            etl.distinct(staging.admin_courses(), key=['class_name', 'grade_name', 'course', 'course_type']),
            etl.fromdb(conn,
                       'select id AS room_id, name AS class_room, school_id as school_id from core_room '),
            key=['class_room'])

        src_table = etl.join(class_grade, course_room, key=['school_id', 'grade_name'])

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = 'course'
        mapping['course_type'] = lambda x: 'GENERAL'
        mapping['class_id'] = 'class_id'
        mapping['periods'] = 'slot'
        mapping['num_consec_periods'] = lambda x: 0
        mapping['status'] = lambda x: 'INACTIVE' if x['deleted'] else 'ACTIVE'
        mapping['version'] = lambda x: '{}{}'.format(academic.academic_year, academic.academic_term)

        mapping['academic_term_id'] = lambda x: academic.academic_id
        mapping['room_id'] = 'room_id'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_course', ('class_id', 'name'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminCourseTeacherTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpAdminCourseTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpGradeTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAdminClassTask(self.interval, self.host, self.database, self.user, self.password)]

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
                                                            ,cg.name as grade_name
                                                            ,cl.name as class_name
                                                        from core_course cc
                                                            ,core_grade cg
                                                            ,core_class cl
                                                       where cg.id = cl.grade_id
                                                         and cc.class_id = cl.id
                                                         and cc.academic_term_id={}'''.format(academic.academic_id)),
                                staging.admin_courses(),
                                key=['grade_name', 'class_name', 'course'])
        teacher_table = etl.fromdb(conn, 'select uid AS teacher_no, id AS teacher_id from core_teacher')

        src_table = etl.join(course_table, teacher_table, key='teacher_no')

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['teacher_id'] = 'teacher_id'
        mapping['course_id'] = 'course_id'
        mapping['updated_at'] = lambda x: datetime2.utc()
        mapping['created_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn = self.output().connect()
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'core_course_teacher', ('teacher_id', 'course_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpTimeTableTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return KedurpAcademicTask(self.interval, self.host, self.database, self.user, self.password)

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(staging.academics(),
                                  etl.fromdb(conn,
                                             "select id as academic_id, academic_year, academic_term from core_academic_term"),
                                  key=['academic_year', 'academic_term'])
        academic = list(academic_table.namedtuples())[0]
        src_table = staging.timetables()

        mapping = OrderedDict()
        mapping['school_id'] = lambda x: config.kedurp['school_id']
        mapping['name'] = lambda x: "{}-{}{}".format(academic.academic_year, academic.academic_term, '学期课程计划')
        mapping['academic_term_id'] = lambda x: academic.academic_id
        mapping['week'] = 'week'
        mapping['checksum'] = lambda x: 'checksum'
        mapping['updated_at'] = lambda x: datetime2.utc()

        dst_table = etl.fieldmap(src_table, mapping)
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")
        mysqldb.upsertdb(dst_table, conn, 'timetable', key=('week', 'academic_term_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)


class KedurpAdminTimeTableItemTask(luigi.Task):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return [KedurpAdminCourseTeacherTask(self.interval, self.host, self.database, self.user, self.password),
                KedurpAdminCourseTask(self.interval, self.host, self.database, self.user, self.password)]

    def run(self):
        conn = self.output().connect()
        academic_table = etl.join(
            staging.academics(),
            etl.fromdb(conn,
                       '''select id as academic_id, academic_year, academic_term
                                                  from core_academic_term'''),
            key=['academic_year', 'academic_term'])

        academic = list(academic_table.namedtuples())[0]

        print(academic)

        admin_course_table = staging.admin_courses()
        course_table = etl.fromdb(conn,
                                  '''select cc.id as course_id, cc.class_id as class_id, cc.room_id as room_id,
                                                    cc.name as course, ct.teacher_id as teacher_id, tt.id as timetable_id,
                                                    tt.week as week,
                                                    g.name as grade_name,c.name as class_name
                                       from core_course cc
                                           ,core_course_teacher ct
                                           ,timetable tt
                                           ,core_grade g
                                           ,core_class c
                                     where cc.id = ct.course_id
                                       and tt.academic_term_id = cc.academic_term_id
                                       and c.grade_id = g.id
                                       and c.id = cc.class_id
                                       and c.klass_type = 'ADMINISTRATION'
                                       and cc.academic_term_id={}'''.format(academic.academic_id))
        src_table = etl.join(admin_course_table, course_table, key=['grade_name', 'class_name', 'course', 'week'])

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

        mapping['updated_at'] = lambda x: datetime2.utc()
        mapping['created_at'] = lambda x: datetime2.utc()
        dst_table = etl.fieldmap(src_table, mapping)
        conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES")

        mysqldb.upsertdb(dst_table, conn, 'timetable_item',
                         ('course_id', 'class_id', 'slot_id', 'begin_slot', 'date', 'timetable_id'), commit=True)
        self.output().touch()

    def output(self):
        return pymysqldb.MySqlTarget(self.host, self.database, self.user, self.password, self.table, self.task_id)

import petl as etl
import pymysql

db = {
    "version": 1,
    "conn": {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "seiue_staging",
        "user": "root",
        "password": "root",
        "charset": "utf8"
    }
}


def academics():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      "select * from academic_term where version = {}".format(db['version']))


def students():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      "select * from student where version = {}".format(db['version']))


def teachers():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      "select * from teacher where version = {}".format(db['version']))


def admin_courses():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      "select * from admin_course where version = {}".format(db['version']))


def teaching_courses():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      "select * from teaching_course where version = {}".format(db['version']))


def timetables():
    return etl.fromdb(pymysql.connect(**db['conn']),
                      '''SELECT `week` FROM
                         (SELECT ac.week, ac.version FROM admin_course ac
                         UNION SELECT tc.week, tc.version FROM teaching_course tc) AS c
                         WHERE c.version = {}'''.format(db["version"]))

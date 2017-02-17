import json
from peewee import MySQLDatabase

from staging.models import AcademicTerm, Teacher, Student, AdminCourse, TeachingCourse, db_proxy

db_proxy.initialize(MySQLDatabase("staging", host="127.0.0.1", user="root", password="root"))
db_proxy.connect()


def clean():
    AcademicTerm.delete().execute()
    Student.delete().execute()
    Teacher.delete().execute()
    AdminCourse.delete().execute()
    TeachingCourse.delete().execute()


def load():
    with db_proxy.atomic():
        with open("fixtures/academic_terms.json") as f:
            academics = json.loads(f.read())
            AcademicTerm.insert_many(academics).execute()

        with open("fixtures/students.json", encoding="utf-8") as f:
            students = json.loads(f.read())
            Student.insert_many(students).execute()

    with open("fixtures/teachers.json", encoding="utf-8") as f:
        teachers = json.loads(f.read())
        Teacher.insert_many(teachers).execute()

    with open("fixtures/admin_courses.json", encoding="utf-8") as f:
        admin_courses = json.loads(f.read())
        AdminCourse.insert_many(admin_courses).execute()

    with open("fixtures/teaching_courses.json", encoding="utf-8") as f:
        teaching_courses = json.loads(f.read())
        TeachingCourse.insert_many(teaching_courses).execute()


def main():
    clean()
    load()


if __name__ == '__main__':
    main()

from datetime import datetime

import petl as etl
from peewee import *

from common.diff import DictDiffer

db_proxy = Proxy()


class BaseModel(Model):
    id = PrimaryKeyField()
    version = IntegerField()
    updated_at = DateTimeField()
    deleted = BooleanField()

    class Meta:
        database = db_proxy

    @classmethod
    def clean(cls, version, updated_at):
        uq = cls.update(deleted=True, version=version).where(cls.version < version, cls.updated_at < updated_at)
        uq.execute()

    @classmethod
    def upsert(cls, table, keys, version, updated_at):
        for row in etl.dicts(table):
            selection = {}
            for k in keys:
                selection[k] = row[k]
            try:
                found = cls.get(**selection)
            except DoesNotExist:
                record = row
                record['version'] = version
                record['updated_at'] = updated_at
                cls.create(**record)
                continue

            changed = DictDiffer(found._data, row).changed()
            if changed:
                for k in changed:
                    setattr(found, k, row[k])

                setattr(found, 'version', version)

            setattr(found, 'updated_at', updated_at)
            found.save()


class DbVersion(Model):
    id = PrimaryKeyField()
    version = IntegerField()
    updated_at = DateTimeField()

    class Meta:
        database = db_proxy
        db_table = 'db_version'

    @classmethod
    def touch(cls, updated_at=datetime.now()):
        cls.update(version=cls.version+1, updated_at=updated_at).execute()
        return cls.get()


class AcademicTerm(BaseModel):
    academic_year = CharField()
    academic_term = CharField()
    start_date = DateField()
    end_date = DateField()

    class Meta:
        db_table = 'academic_term'


class Student(BaseModel):
    student_no = CharField()
    card_no = CharField()
    student_name = CharField()
    gender = BooleanField()
    grade_name = CharField()
    class_name = CharField()
    head_teacher_no = CharField()
    head_teacher_name = CharField()

    class Meta:
        db_table = 'student'


class Teacher(BaseModel):
    teacher_no = CharField()
    card_no = CharField()
    teacher_name = CharField()
    gender = BooleanField()
    email = CharField()
    phone = CharField()

    class Meta:
        db_table = 'teacher'


class AdminCourse(BaseModel):
    grade_name = CharField()
    class_name = CharField()
    course = CharField()
    course_type = CharField()
    week = IntegerField()
    day_of_week = IntegerField()
    slot = IntegerField()
    start_time = TimeField()
    end_time = TimeField()
    teacher_no = CharField()
    teacher_name = CharField()
    class_room = CharField()

    class Meta:
        db_table = 'admin_course'


class TeachingCourse(BaseModel):
    class_name = CharField()
    student_no = CharField()
    student_name = CharField()
    course_type = CharField()
    course = CharField()
    week = IntegerField()
    day_of_week = IntegerField()
    slot = IntegerField()
    start_time = TimeField()
    end_time = TimeField()
    teacher_no = CharField()
    teacher_name = CharField()
    class_room = CharField()

    class Meta:
        db_table = 'teaching_course'

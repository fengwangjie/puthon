import datetime
import unittest
from collections import OrderedDict

import dateutil.parser
from peewee import MySQLDatabase

import petl as etl
from staging.models import AcademicTerm, DbVersion, db_proxy

db_proxy.initialize(MySQLDatabase("staging", host="127.0.0.1", user="root"))
db_proxy.connect()


class TestDbVersion(unittest.TestCase):

    def setUp(self):
        DbVersion.create(version=0, updated_at=datetime.datetime.now())

    def tearDown(self):
        DbVersion.delete().execute()

    def test_touch(self):
        touched = DbVersion.touch(updated_at=datetime.datetime.now())

        self.assertEquals(touched.version, 1)


class TestAcademicTerm(unittest.TestCase):

    def setUp(self):
        AcademicTerm.create(academic_year='2016', academic_term='2',
                            start_date=datetime.date(2016, 7, 1), end_date=datetime.date(2016, 12, 30),
                            version=1)

    def tearDown(self):
        AcademicTerm.delete().execute()

    def test_update_no_changes(self):
        dicts = [{"academic_year": "2016", "academic_term": '2',
                  'start_date': '2016-07-01', 'end_date': '2016-12-30'}]

        src = etl.fromdicts(
            dicts, header=['academic_year', 'academic_term', 'start_date', 'end_date'])

        mapping = OrderedDict()
        mapping['academic_term'] = 'academic_term'
        mapping['academic_year'] = 'academic_year'
        mapping['start_date'] = lambda x: dateutil.parser.parse(
            x['start_date']).date()
        mapping['end_date'] = lambda x: dateutil.parser.parse(
            x['end_date']).date()

        dst = etl.fieldmap(src, mapping)

        updated_at = datetime.datetime.now()
        AcademicTerm.upsert(
            dst, ('academic_year', 'academic_term'), 2, updated_at)

        at = AcademicTerm.get(AcademicTerm.academic_year == '2016',
                              AcademicTerm.academic_term == '2')

        self.assertEqual(at.version, 1)
        self.assertEqual(at.updated_at, updated_at.replace(microsecond=0))

    def test_insert(self):
        dicts = [{"academic_year": "2016", "academic_term": '1',
                  'start_date': '2016-07-01', 'end_date': '2016-12-31'}]

        src = etl.fromdicts(
            dicts, header=['academic_year', 'academic_term', 'start_date', 'end_date'])

        mapping = OrderedDict()
        mapping['academic_term'] = 'academic_term'
        mapping['academic_year'] = 'academic_year'
        mapping['start_date'] = lambda x: dateutil.parser.parse(
            x['start_date']).date()
        mapping['end_date'] = lambda x: dateutil.parser.parse(
            x['end_date']).date()

        dst = etl.fieldmap(src, mapping)

        updated_at = datetime.datetime.now()
        AcademicTerm.upsert(
            dst, ('academic_year', 'academic_term'), 2, updated_at)

        AcademicTerm.clean(2, updated_at)

        at1 = AcademicTerm.get(AcademicTerm.academic_year == '2016',
                               AcademicTerm.academic_term == '1')

        at2 = AcademicTerm.get(AcademicTerm.academic_year == '2016',
                               AcademicTerm.academic_term == '2')

        self.assertEqual(at2.version, 2)
        self.assertTrue(at2.deleted)

        self.assertEqual(at1.version, 2)
        self.assertFalse(at1.deleted)
        self.assertEqual(at1.updated_at, updated_at.replace(microsecond=0))

    def test_update_with_changes(self):
        dicts = [{"academic_year": "2016", "academic_term": '2',
                  'start_date': '2016-07-01', 'end_date': '2016-12-31'}]

        src = etl.fromdicts(
            dicts, header=['academic_year', 'academic_term', 'start_date', 'end_date'])

        mapping = OrderedDict()
        mapping['academic_term'] = 'academic_term'
        mapping['academic_year'] = 'academic_year'
        mapping['start_date'] = lambda x: dateutil.parser.parse(
            x['start_date']).date()
        mapping['end_date'] = lambda x: dateutil.parser.parse(
            x['end_date']).date()

        dst = etl.fieldmap(src, mapping)

        updated_at = datetime.datetime.now()
        AcademicTerm.upsert(
            dst, ('academic_year', 'academic_term'),  2, updated_at)

        at = AcademicTerm.get(AcademicTerm.academic_year == '2016',
                              AcademicTerm.academic_term == '2')

        self.assertEqual(at.version, 2)
        self.assertEqual(at.updated_at, updated_at.replace(microsecond=0))

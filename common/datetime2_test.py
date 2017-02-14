import unittest
from datetime import date

from common import datetime2


class Datetime2Test(unittest.TestCase):

    def test_dates_in(self):
        start = date(2017, 2, 1)
        end = date(2017, 2, 28)
        days = datetime2.dates_in(start, end, day_of_weeks=[3])

        self.assertEqual(len(days), 4)
        self.assertIn(date(2017, 2, 1), days)
        self.assertIn(date(2017, 2, 8), days)
        self.assertIn(date(2017, 2, 15), days)
        self.assertIn(date(2017, 2, 22), days)

    def test_week_range(self):
        start = date(2017, 2, 1)

        monday, sunday = datetime2.week_range(start, 4)
        self.assertEqual(monday, date(2017, 2, 20))
        self.assertEqual(sunday, date(2017, 2, 26))

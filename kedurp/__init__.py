import petl as etl

import config
from common import datetime2


def date_in(start_date, end_date, week, day_of_week):
    week_start, week_end = datetime2.week_range(start_date, week)

    if week_start > end_date:
        raise ValueError("week is out of date range.")

    days = datetime2.dates_in(week_start, week_end, day_of_weeks=[day_of_week])
    if len(days) == 0:
        raise ValueError("invalid day of week.")
    return days[0]


def map_course(course_table, start_date, end_date):
    def reduce(row):
        day_of_week = row['day_of_week']
        for d in datetime2.dates_in(start_date, end_date, day_of_weeks=[day_of_week]):
            yield [config.kedurp['school_id'], row.timetable_id, row.course_id, row.class_id, row.teacher_id,
                   row.room_id, row.day_of_week, d.strftime('%Y-%m-%d'), row.slot, row.slot, 1, datetime2.utc(),
                   datetime2.utc()]

    mapped = etl.rowmapmany(course_table, reduce,
                            header=['school_id', 'timetable_id', 'course_id', 'class_id', 'teacher_id',
                                    'room_id', 'day_week', 'date', 'begin_slot', 'end_slot', 'slot_id', 'updated_at',
                                    'created_at'])
    return etl.sort(mapped, 'date')

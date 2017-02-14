import datetime
from datetime import timedelta


def utc():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def week_range(star_date, week):
    d = star_date - timedelta(star_date.weekday())
    dlt = timedelta(days=(week - 1) * 7)
    return d + dlt, d + dlt + timedelta(days=6)


def dates_in(start_date, end_date, day_of_weeks=[]):
    days = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    return [d for d in days if d.isoweekday() in day_of_weeks]

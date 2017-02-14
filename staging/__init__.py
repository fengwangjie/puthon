from staging import db as query


def academics():
    return query.academics()


def students():
    return query.students()


def teachers():
    return query.teachers()


def admin_courses():
    return query.admin_courses()


def teaching_courses():
    return query.teaching_courses()


def timetables():
    return query.timetables()

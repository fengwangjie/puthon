import luigi

from kedurp.student import KedurpStudentTask, KedurpStudentUserTask, KedurpGradeTask

from kedurp.teacher import KedurpTeacherUserTask, KedurpTeacherTask
from kedurp.academic_term import KedurpAcademicTask

from kedurp.admin_course import KedurpAdminCourseRoomTask,\
    KedurpAdminClassTask, KedurpAdminClassStudentTask, \
    KedurpAdminCourseTeacherTask, KedurpAdminCourseTask, \
    KedurpTimeTableTask, KedurpAdminTimeTableItemTask

from kedurp.teaching_course import KedurpTeachingClassRoomTask, \
     KedurpTeachingClassTask, KedurpTeachingClassStudentTask, \
     KedurpTeachingCourseTeacherTask, KedurpTeachingCourseTask,\
     KedurpTeachingTimeTableItemTask


class KedurpTask(luigi.WrapperTask):
    interval = luigi.DateMinuteParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    def requires(self):
        params = {
            "interval": self.interval,
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password
        }
        yield KedurpAcademicTask(**params)

        yield KedurpStudentTask(**params)
        yield KedurpStudentUserTask(**params)
        yield KedurpGradeTask(**params)

        yield KedurpTeacherUserTask(**params)
        yield KedurpTeacherTask(**params)

        # yield KedurpSubjectTask(**params)
        yield KedurpAdminCourseRoomTask(**params)

        yield KedurpAdminClassTask(**params)
        yield KedurpAdminClassStudentTask(**params)

        yield KedurpAdminCourseTask(**params)
        yield KedurpAdminCourseTeacherTask(**params)

        yield KedurpTimeTableTask(**params)
        yield KedurpAdminTimeTableItemTask(**params)

        yield KedurpTeachingClassRoomTask(**params)

        yield KedurpTeachingClassTask(**params)
        yield KedurpTeachingClassStudentTask(**params)

        yield KedurpTeachingCourseTask(**params)
        yield KedurpTeachingCourseTeacherTask(**params)

        yield KedurpTeachingTimeTableItemTask(**params)


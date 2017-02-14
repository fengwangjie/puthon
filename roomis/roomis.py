import luigi

from roomis.student import RoomisStudentCardTask, RoomisStudentUserTask
from roomis.teacher import RoomisTeacherUserTask, RoomisTeacherCardTask

from roomis.role import RoomisUserRoleTask


class RoomisTask(luigi.WrapperTask):
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
        yield RoomisStudentCardTask(**params)
        yield RoomisStudentUserTask(**params)
        yield RoomisTeacherUserTask(**params)
        yield RoomisTeacherCardTask(**params)
        yield RoomisUserRoleTask(**params)

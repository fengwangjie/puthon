from datetime import datetime

import luigi
from kedurp import kedurp
from roomis import roomis


class MainTask(luigi.WrapperTask):
    interval = luigi.DateMinuteParameter()

    def requires(self):
        yield kedurp.KedurpTask(self.interval)
        # yield roomis.RoomisTask(self.interval)


def main():
    luigi.run(['MainTask', '--interval', datetime.now().strftime('%Y-%m-%dT%H%M'), '--workers', '1'])

if __name__ == '__main__':
    main()

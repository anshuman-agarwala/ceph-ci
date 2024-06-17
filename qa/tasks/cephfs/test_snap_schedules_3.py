import os
import json
import time
import errno
import logging
import uuid

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from datetime import datetime, timedelta
from tasks.cephfs.test_snap_schedules_helper import TestSnapSchedulesHelper

log = logging.getLogger(__name__)

def extract_schedule_and_retention_spec(spec=[]):
    schedule = set([s[0] for s in spec])
    retention = set([s[1] for s in spec])
    return (schedule, retention)

def seconds_upto_next_schedule(time_from, timo):
    ts = int(time_from)
    return ((int(ts / 60) * 60) + timo) - ts

class TestSnapSchedulesSnapdir(TestSnapSchedulesHelper):
    def test_snap_dir_name(self):
        """Test the correctness of snap directory name"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedulesSnapdir.TEST_DIRECTORY])

        # set a schedule on the dir
        self.fs_snap_schedule_cmd('add', path=TestSnapSchedulesSnapdir.TEST_DIRECTORY, snap_schedule='1m')
        self.fs_snap_schedule_cmd('retention', 'add', path=TestSnapSchedulesSnapdir.TEST_DIRECTORY, retention_spec_or_period='1m')
        exec_time = time.time()

        timo, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')
        sdn = self.get_snap_dir_name()
        log.info(f'expecting snap {TestSnapSchedulesSnapdir.TEST_DIRECTORY}/{sdn}/scheduled-{snap_sfx} in ~{timo}s...')

        # verify snapshot schedule
        self.verify_schedule(TestSnapSchedulesSnapdir.TEST_DIRECTORY, ['1m'], retentions=[{'m':1}])

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path=TestSnapSchedulesSnapdir.TEST_DIRECTORY)

        # remove all scheduled snapshots
        self.remove_snapshots(TestSnapSchedulesSnapdir.TEST_DIRECTORY, sdn)

        self.mount_a.run_shell(['rmdir', TestSnapSchedulesSnapdir.TEST_DIRECTORY])

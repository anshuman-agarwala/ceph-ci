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

class TestSnapSchedulesMandatoryFSArgument(TestSnapSchedulesHelper):
    REQUIRE_BACKUP_FILESYSTEM = True
    TEST_DIRECTORY = 'mandatory_fs_argument_test_dir'

    def test_snap_schedule_without_fs_argument(self):
        """Test command fails without --fs argument in presence of multiple fs"""
        test_path = TestSnapSchedulesMandatoryFSArgument.TEST_DIRECTORY
        self.mount_a.run_shell(['mkdir', '-p', test_path])

        # try setting a schedule on the dir; this should fail now that we are
        # working with mutliple fs; we need the --fs argument if there are more
        # than one fs hosted by the same cluster
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', test_path, snap_schedule='1M')

        self.mount_a.run_shell(['rmdir', test_path])

    def test_snap_schedule_for_non_default_fs(self):
        """Test command succes with --fs argument for non-default fs"""
        test_path = TestSnapSchedulesMandatoryFSArgument.TEST_DIRECTORY
        self.mount_a.run_shell(['mkdir', '-p', test_path])

        # use the backup fs as the second fs; all these commands must pass
        self.fs_snap_schedule_cmd('add', test_path, snap_schedule='1M', fs='backup_fs')
        self.fs_snap_schedule_cmd('activate', test_path, snap_schedule='1M', fs='backup_fs')
        self.fs_snap_schedule_cmd('retention', 'add', test_path, retention_spec_or_period='1M', fs='backup_fs')
        self.fs_snap_schedule_cmd('list', test_path, fs='backup_fs', format='json')
        self.fs_snap_schedule_cmd('status', test_path, fs='backup_fs', format='json')
        self.fs_snap_schedule_cmd('retention', 'remove', test_path, retention_spec_or_period='1M', fs='backup_fs')
        self.fs_snap_schedule_cmd('deactivate', test_path, snap_schedule='1M', fs='backup_fs')
        self.fs_snap_schedule_cmd('remove', test_path, snap_schedule='1M', fs='backup_fs')

        self.mount_a.run_shell(['rmdir', test_path])

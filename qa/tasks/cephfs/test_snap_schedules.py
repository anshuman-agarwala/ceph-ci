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


class TestSnapSchedules(TestSnapSchedulesHelper):
    def remove_snapshots(self, dir_path):
        snap_path = f'{dir_path}/.snap'

        snapshots = self.mount_a.ls(path=snap_path)
        for snapshot in snapshots:
            snapshot_path = os.path.join(snap_path, snapshot)
            log.debug(f'removing snapshot: {snapshot_path}')
            self.mount_a.run_shell(['rmdir', snapshot_path])

    def test_non_existent_snap_schedule_list(self):
        """Test listing snap schedules on a non-existing filesystem path failure"""
        try:
            self.fs_snap_schedule_cmd('list', path=TestSnapSchedules.TEST_DIRECTORY)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError('incorrect errno when listing a non-existing snap schedule')
        else:
            raise RuntimeError('expected "fs snap-schedule list" to fail')

    def test_non_existent_schedule(self):
        """Test listing non-existing snap schedules failure"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        try:
            self.fs_snap_schedule_cmd('list', path=TestSnapSchedules.TEST_DIRECTORY)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError('incorrect errno when listing a non-existing snap schedule')
        else:
            raise RuntimeError('expected "fs snap-schedule list" returned fail')

        self.mount_a.run_shell(['rmdir', TestSnapSchedules.TEST_DIRECTORY])

    def test_snap_schedule_list_post_schedule_remove(self):
        """Test listing snap schedules post removal of a schedule"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        self.fs_snap_schedule_cmd('add', path=TestSnapSchedules.TEST_DIRECTORY, snap_schedule='1h')

        self.fs_snap_schedule_cmd('remove', path=TestSnapSchedules.TEST_DIRECTORY)

        try:
            self.fs_snap_schedule_cmd('list', path=TestSnapSchedules.TEST_DIRECTORY)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError('incorrect errno when listing a non-existing snap schedule')
        else:
            raise RuntimeError('"fs snap-schedule list" returned error')

        self.mount_a.run_shell(['rmdir', TestSnapSchedules.TEST_DIRECTORY])

    def test_snap_schedule(self):
        """Test existence of a scheduled snapshot"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        # set a schedule on the dir
        self.fs_snap_schedule_cmd('add', path=TestSnapSchedules.TEST_DIRECTORY, snap_schedule='1m')
        exec_time = time.time()

        timo, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')
        log.debug(f'expecting snap {TestSnapSchedules.TEST_DIRECTORY}/.snap/scheduled-{snap_sfx} in ~{timo}s...')
        to_wait = timo + 2 # some leeway to avoid false failures...

        # verify snapshot schedule
        self.verify_schedule(TestSnapSchedules.TEST_DIRECTORY, ['1m'])

        def verify_added(snaps_added):
            log.debug(f'snapshots added={snaps_added}')
            self.assertEqual(len(snaps_added), 1)
            snapname = snaps_added[0]
            if snapname.startswith('scheduled-'):
                if snapname[10:26] == snap_sfx[:16]:
                    self.check_scheduled_snapshot(exec_time, timo)
                    return True
            return False
        self.add_snap_create_cbk(verify_added)
        self.verify(TestSnapSchedules.TEST_DIRECTORY, to_wait)
        self.assert_if_not_verified()

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path=TestSnapSchedules.TEST_DIRECTORY)

        # remove all scheduled snapshots
        self.remove_snapshots(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(['rmdir', TestSnapSchedules.TEST_DIRECTORY])

    def test_multi_snap_schedule(self):
        """Test exisitence of multiple scheduled snapshots"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        # set schedules on the dir
        self.fs_snap_schedule_cmd('add', path=TestSnapSchedules.TEST_DIRECTORY, snap_schedule='1m')
        self.fs_snap_schedule_cmd('add', path=TestSnapSchedules.TEST_DIRECTORY, snap_schedule='2m')
        exec_time = time.time()

        timo_1, snap_sfx_1 = self.calc_wait_time_and_snap_name(exec_time, '1m')
        log.debug(f'expecting snap {TestSnapSchedules.TEST_DIRECTORY}/.snap/scheduled-{snap_sfx_1} in ~{timo_1}s...')
        timo_2, snap_sfx_2 = self.calc_wait_time_and_snap_name(exec_time, '2m')
        log.debug(f'expecting snap {TestSnapSchedules.TEST_DIRECTORY}/.snap/scheduled-{snap_sfx_2} in ~{timo_2}s...')
        to_wait = timo_2 + 2 # use max timeout

        # verify snapshot schedule
        self.verify_schedule(TestSnapSchedules.TEST_DIRECTORY, ['1m', '2m'])

        def verify_added_1(snaps_added):
            log.debug(f'snapshots added={snaps_added}')
            self.assertEqual(len(snaps_added), 1)
            snapname = snaps_added[0]
            if snapname.startswith('scheduled-'):
                if snapname[10:26] == snap_sfx_1[:16]:
                    self.check_scheduled_snapshot(exec_time, timo_1)
                    return True
            return False
        def verify_added_2(snaps_added):
            log.debug(f'snapshots added={snaps_added}')
            self.assertEqual(len(snaps_added), 1)
            snapname = snaps_added[0]
            if snapname.startswith('scheduled-'):
                if snapname[10:26] == snap_sfx_2[:16]:
                    self.check_scheduled_snapshot(exec_time, timo_2)
                    return True
            return False
        self.add_snap_create_cbk(verify_added_1)
        self.add_snap_create_cbk(verify_added_2)
        self.verify(TestSnapSchedules.TEST_DIRECTORY, to_wait)
        self.assert_if_not_verified()

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path=TestSnapSchedules.TEST_DIRECTORY)

        # remove all scheduled snapshots
        self.remove_snapshots(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(['rmdir', TestSnapSchedules.TEST_DIRECTORY])

    def test_snap_schedule_with_retention(self):
        """Test scheduled snapshots along with rentention policy"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        # set a schedule on the dir
        self.fs_snap_schedule_cmd('add', path=TestSnapSchedules.TEST_DIRECTORY, snap_schedule='1m')
        self.fs_snap_schedule_cmd('retention', 'add', path=TestSnapSchedules.TEST_DIRECTORY, retention_spec_or_period='1m')
        exec_time = time.time()

        timo_1, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')
        log.debug(f'expecting snap {TestSnapSchedules.TEST_DIRECTORY}/.snap/scheduled-{snap_sfx} in ~{timo_1}s...')
        to_wait = timo_1 + 2 # some leeway to avoid false failures...

        # verify snapshot schedule
        self.verify_schedule(TestSnapSchedules.TEST_DIRECTORY, ['1m'], retentions=[{'m':1}])

        def verify_added(snaps_added):
            log.debug(f'snapshots added={snaps_added}')
            self.assertEqual(len(snaps_added), 1)
            snapname = snaps_added[0]
            if snapname.startswith('scheduled-'):
                if snapname[10:26] == snap_sfx[:16]:
                    self.check_scheduled_snapshot(exec_time, timo_1)
                    return True
            return False
        self.add_snap_create_cbk(verify_added)
        self.verify(TestSnapSchedules.TEST_DIRECTORY, to_wait)
        self.assert_if_not_verified()

        timo_2 = timo_1 + 60 # expected snapshot removal timeout
        def verify_removed(snaps_removed):
            log.debug(f'snapshots removed={snaps_removed}')
            self.assertEqual(len(snaps_removed), 1)
            snapname = snaps_removed[0]
            if snapname.startswith('scheduled-'):
                if snapname[10:26] == snap_sfx[:16]:
                    self.check_scheduled_snapshot(exec_time, timo_2)
                    return True
            return False
        log.debug(f'expecting removal of snap {TestSnapSchedules.TEST_DIRECTORY}/.snap/scheduled-{snap_sfx} in ~{timo_2}s...')
        to_wait = timo_2
        self.add_snap_remove_cbk(verify_removed)
        self.verify(TestSnapSchedules.TEST_DIRECTORY, to_wait+2)
        self.assert_if_not_verified()

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path=TestSnapSchedules.TEST_DIRECTORY)

        # remove all scheduled snapshots
        self.remove_snapshots(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(['rmdir', TestSnapSchedules.TEST_DIRECTORY])

    def get_snap_stats(self, dir_path):
        snap_path = f"{dir_path}/.snap"[1:]
        snapshots = self.mount_a.ls(path=snap_path)
        fs_count = len(snapshots)
        log.debug(f'snapshots: {snapshots}')

        result = self.fs_snap_schedule_cmd('status', path=dir_path,
                                           format='json')
        json_res = json.loads(result)[0]
        db_count = int(json_res['created_count'])
        log.debug(f'json_res: {json_res}')

        snap_stats = dict()
        snap_stats['fs_count'] = fs_count
        snap_stats['db_count'] = db_count

        log.debug(f'fs_count: {fs_count}')
        log.debug(f'db_count: {db_count}')

        return snap_stats

    def verify_snap_stats(self, dir_path):
        snap_stats = self.get_snap_stats(dir_path)
        self.assertTrue(snap_stats['fs_count'] == snap_stats['db_count'])

    def test_concurrent_snap_creates(self):
        """Test concurrent snap creates in same file-system without db issues"""
        """
        Test snap creates at same cadence on same fs to verify correct stats.
        A single SQLite DB Connection handle cannot be used to run concurrent
        transactions and results transaction aborts. This test makes sure that
        proper care has been taken in the code to avoid such situation by
        verifying number of dirs created on the file system with the
        created_count in the schedule_meta table for the specific path.
        """
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])

        testdirs = []
        for d in range(10):
            testdirs.append(os.path.join("/", TestSnapSchedules.TEST_DIRECTORY, "dir" + str(d)))

        for d in testdirs:
            self.mount_a.run_shell(['mkdir', '-p', d[1:]])
            self.fs_snap_schedule_cmd('add', path=d, snap_schedule='1m')

        exec_time = time.time()
        timo_1, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')

        for d in testdirs:
            self.fs_snap_schedule_cmd('activate', path=d, snap_schedule='1m')

        # we wait for 10 snaps to be taken
        wait_time = timo_1 + 10 * 60 + 15
        time.sleep(wait_time)

        for d in testdirs:
            self.fs_snap_schedule_cmd('deactivate', path=d, snap_schedule='1m')

        for d in testdirs:
            self.verify_snap_stats(d)

        for d in testdirs:
            self.fs_snap_schedule_cmd('remove', path=d, snap_schedule='1m')
            self.remove_snapshots(d[1:])
            self.mount_a.run_shell(['rmdir', d[1:]])

    def test_snap_schedule_with_mgr_restart(self):
        """Test that snap schedule is resumed after mgr restart"""
        self.mount_a.run_shell(['mkdir', '-p', TestSnapSchedules.TEST_DIRECTORY])
        testdir = os.path.join("/", TestSnapSchedules.TEST_DIRECTORY, "test_restart")
        self.mount_a.run_shell(['mkdir', '-p', testdir[1:]])
        self.fs_snap_schedule_cmd('add', path=testdir, snap_schedule='1m')

        exec_time = time.time()
        timo_1, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')

        self.fs_snap_schedule_cmd('activate', path=testdir, snap_schedule='1m')

        # we wait for 10 snaps to be taken
        wait_time = timo_1 + 10 * 60 + 15
        time.sleep(wait_time)

        old_stats = self.get_snap_stats(testdir)
        self.assertTrue(old_stats['fs_count'] == old_stats['db_count'])
        self.assertTrue(old_stats['fs_count'] > 9)

        # restart mgr
        active_mgr = self.mgr_cluster.mon_manager.get_mgr_dump()['active_name']
        log.debug(f'restarting active mgr: {active_mgr}')
        self.mgr_cluster.mon_manager.revive_mgr(active_mgr)
        time.sleep(300)  # sleep for 5 minutes
        self.fs_snap_schedule_cmd('deactivate', path=testdir, snap_schedule='1m')

        new_stats = self.get_snap_stats(testdir)
        self.assertTrue(new_stats['fs_count'] == new_stats['db_count'])
        self.assertTrue(new_stats['fs_count'] > old_stats['fs_count'])
        self.assertTrue(new_stats['db_count'] > old_stats['db_count'])

        # cleanup
        self.fs_snap_schedule_cmd('remove', path=testdir, snap_schedule='1m')
        self.remove_snapshots(testdir[1:])
        self.mount_a.run_shell(['rmdir', testdir[1:]])

    def test_schedule_auto_deactivation_for_non_existent_path(self):
        """
        Test that a non-existent path leads to schedule deactivation after a few retries.
        """
        self.fs_snap_schedule_cmd('add', path="/bad-path", snap_schedule='1m')
        start_time = time.time()

        while time.time() - start_time < 60.0:
            s = self.fs_snap_schedule_cmd('status', path="/bad-path", format='json')
            json_status = json.loads(s)[0]

            self.assertTrue(int(json_status['active']) == 1)
            time.sleep(60)

        s = self.fs_snap_schedule_cmd('status', path="/bad-path", format='json')
        json_status = json.loads(s)[0]
        self.assertTrue(int(json_status['active']) == 0)

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path="/bad-path")

    def test_snap_schedule_for_number_of_snaps_retention(self):
        """
        Test that number of snaps retained are as per user spec.
        """
        total_snaps = 55
        test_dir = '/' + TestSnapSchedules.TEST_DIRECTORY

        self.mount_a.run_shell(['mkdir', '-p', test_dir[1:]])

        # set a schedule on the dir
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1m')
        self.fs_snap_schedule_cmd('retention', 'add', path=test_dir,
                                  retention_spec_or_period=f'{total_snaps}n')
        exec_time = time.time()

        timo_1, snap_sfx = self.calc_wait_time_and_snap_name(exec_time, '1m')

        # verify snapshot schedule
        self.verify_schedule(test_dir, ['1m'])

        # we wait for total_snaps snaps to be taken
        wait_time = timo_1 + total_snaps * 60 + 15
        time.sleep(wait_time)

        snap_stats = self.get_snap_stats(test_dir)
        self.assertTrue(snap_stats['fs_count'] == total_snaps)
        self.assertTrue(snap_stats['db_count'] >= total_snaps)

        # remove snapshot schedule
        self.fs_snap_schedule_cmd('remove', path=test_dir)

        # remove all scheduled snapshots
        self.remove_snapshots(test_dir[1:])

        self.mount_a.run_shell(['rmdir', test_dir[1:]])

    def test_snap_schedule_all_periods(self):
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/minutes"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1m')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/hourly"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1h')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/daily"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1d')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/weekly"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1w')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/monthly"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1M')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/yearly"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1y')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/bad_period_spec"
        self.mount_a.run_shell(['mkdir', '-p', test_dir])
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1X')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1MM')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='1')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='M')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='-1m')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', path=test_dir, snap_schedule='')

        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/minutes"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/hourly"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/daily"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/weekly"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/monthly"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/yearly"
        self.mount_a.run_shell(['rmdir', test_dir])
        test_dir = TestSnapSchedulesSnapdir.TEST_DIRECTORY + "/bad_period_spec"
        self.mount_a.run_shell(['rmdir', test_dir])

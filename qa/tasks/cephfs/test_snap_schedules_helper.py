import os
import json
import time
import errno
import logging
import uuid

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

def extract_schedule_and_retention_spec(spec=[]):
    schedule = set([s[0] for s in spec])
    retention = set([s[1] for s in spec])
    return (schedule, retention)

def seconds_upto_next_schedule(time_from, timo):
    ts = int(time_from)
    return ((int(ts / 60) * 60) + timo) - ts

class TestSnapSchedulesHelper(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    TEST_VOLUME_NAME = 'snap_vol'
    TEST_DIRECTORY = 'snap_test_dir1'

    # this should be in sync with snap_schedule format
    SNAPSHOT_TS_FORMAT = '%Y-%m-%d-%H_%M_%S'

    def remove_snapshots(self, dir_path, sdn):
        snap_path = f'{dir_path}/{sdn}'

        snapshots = self.mount_a.ls(path=snap_path)
        for snapshot in snapshots:
            if snapshot.startswith("_scheduled"):
                continue
            snapshot_path = os.path.join(snap_path, snapshot)
            log.debug(f'removing snapshot: {snapshot_path}')
            self.mount_a.run_shell(['sudo', 'rmdir', snapshot_path])

    def get_snap_dir_name(self):
        from .fuse_mount import FuseMount
        from .kernel_mount import KernelMount

        if isinstance(self.mount_a, KernelMount):
            sdn = self.mount_a.client_config.get('snapdirname', '.snap')
        elif isinstance(self.mount_a, FuseMount):
            sdn = self.mount_a.client_config.get('client_snapdir', '.snap')
            self.fs.set_ceph_conf('client', 'client snapdir', sdn)
            self.mount_a.remount()
        return sdn

    def check_scheduled_snapshot(self, exec_time, timo):
        now = time.time()
        delta = now - exec_time
        log.debug(f'exec={exec_time}, now = {now}, timo = {timo}')
        # tolerate snapshot existance in the range [-5,+5]
        self.assertTrue((delta <= timo + 5) and (delta >= timo - 5))

    def _fs_cmd(self, *args):
        return self.get_ceph_cmd_stdout("fs", *args)

    def fs_snap_schedule_cmd(self, *args, **kwargs):
        if 'fs' in kwargs:
            fs = kwargs.pop('fs')
            args += ('--fs', fs)
        if 'format' in kwargs:
            fmt = kwargs.pop('format')
            args += ('--format', fmt)
        for name, val in kwargs.items():
            args += (str(val),)
        res = self._fs_cmd('snap-schedule', *args)
        log.debug(f'res={res}')
        return res

    def _create_or_reuse_test_volume(self):
        result = json.loads(self._fs_cmd("volume", "ls"))
        if len(result) == 0:
            self.vol_created = True
            self.volname = TestSnapSchedulesHelper.TEST_VOLUME_NAME
            self._fs_cmd("volume", "create", self.volname)
        else:
            self.volname = result[0]['name']

    def _enable_snap_schedule(self):
        return self.get_ceph_cmd_stdout("mgr", "module", "enable", "snap_schedule")

    def _disable_snap_schedule(self):
        return self.get_ceph_cmd_stdout("mgr", "module", "disable", "snap_schedule")

    def _allow_minute_granularity_snapshots(self):
        self.config_set('mgr', 'mgr/snap_schedule/allow_m_granularity', True)

    def _dump_on_update(self):
        self.config_set('mgr', 'mgr/snap_schedule/dump_on_update', True)

    def setUp(self):
        super(TestSnapSchedulesHelper, self).setUp()
        self.volname = None
        self.vol_created = False
        self._create_or_reuse_test_volume()
        self.create_cbks = []
        self.remove_cbks = []
        # used to figure out which snapshots are created/deleted
        self.snapshots = set()
        self._enable_snap_schedule()
        self._allow_minute_granularity_snapshots()
        self._dump_on_update()

    def tearDown(self):
        if self.vol_created:
            self._delete_test_volume()
        self._disable_snap_schedule()
        super(TestSnapSchedulesHelper, self).tearDown()

    def _schedule_to_timeout(self, schedule):
        mult = schedule[-1]
        period = int(schedule[0:-1])
        if mult == 'm':
            return period * 60
        elif mult == 'h':
            return period * 60 * 60
        elif mult == 'd':
            return period * 60 * 60 * 24
        elif mult == 'w':
            return period * 60 * 60 * 24 * 7
        elif mult == 'M':
            return period * 60 * 60 * 24 * 30
        elif mult == 'Y':
            return period * 60 * 60 * 24 * 365
        else:
            raise RuntimeError('schedule multiplier not recognized')

    def add_snap_create_cbk(self, cbk):
        self.create_cbks.append(cbk)
    def remove_snap_create_cbk(self, cbk):
        self.create_cbks.remove(cbk)

    def add_snap_remove_cbk(self, cbk):
        self.remove_cbks.append(cbk)
    def remove_snap_remove_cbk(self, cbk):
        self.remove_cbks.remove(cbk)

    def assert_if_not_verified(self):
        self.assertListEqual(self.create_cbks, [])
        self.assertListEqual(self.remove_cbks, [])

    def verify(self, dir_path, max_trials):
        trials = 0
        snap_path = f'{dir_path}/.snap'
        while (len(self.create_cbks) or len(self.remove_cbks)) and trials < max_trials:
            snapshots = set(self.mount_a.ls(path=snap_path))
            log.info(f'snapshots: {snapshots}')
            added = snapshots - self.snapshots
            log.info(f'added: {added}')
            removed = self.snapshots - snapshots
            log.info(f'removed: {removed}')
            if added:
                for cbk in list(self.create_cbks):
                    res = cbk(list(added))
                    if res:
                        self.remove_snap_create_cbk(cbk)
                        break
            if removed:
                for cbk in list(self.remove_cbks):
                    res = cbk(list(removed))
                    if res:
                        self.remove_snap_remove_cbk(cbk)
                        break
            self.snapshots = snapshots
            trials += 1
            time.sleep(1)

    def calc_wait_time_and_snap_name(self, snap_sched_exec_epoch, schedule):
        timo = self._schedule_to_timeout(schedule)
        # calculate wait time upto the next minute
        wait_timo = seconds_upto_next_schedule(snap_sched_exec_epoch, timo)

        # expected "scheduled" snapshot name
        ts_name = (datetime.utcfromtimestamp(snap_sched_exec_epoch)
                   + timedelta(seconds=wait_timo)).strftime(TestSnapSchedulesHelper.SNAPSHOT_TS_FORMAT)
        return (wait_timo, ts_name)

    def verify_schedule(self, dir_path, schedules, retentions=[]):
        log.debug(f'expected_schedule: {schedules}, expected_retention: {retentions}')

        result = self.fs_snap_schedule_cmd('list', path=dir_path, format='json')
        json_res = json.loads(result)
        log.debug(f'json_res: {json_res}')

        for schedule in schedules:
            self.assertTrue(schedule in json_res['schedule'])
        for retention in retentions:
            self.assertTrue(retention in json_res['retention'])

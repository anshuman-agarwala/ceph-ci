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

class TestSnapSchedulesSubvolAndGroupArguments(TestSnapSchedulesHelper):
    def setUp(self):
        super(TestSnapSchedulesSubvolAndGroupArguments, self).setUp()
        self.CREATE_VERSION = int(self.mount_a.ctx['config']['overrides']['subvolume_version'])

    def _create_v1_subvolume(self, subvol_name, subvol_group=None, has_snapshot=False, subvol_type='subvolume', state='complete'):
        group = subvol_group if subvol_group is not None else '_nogroup'
        basepath = os.path.join("volumes", group, subvol_name)
        uuid_str = str(uuid.uuid4())
        createpath = os.path.join(basepath, uuid_str)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)
        self.mount_a.setfattr(createpath, 'ceph.dir.subvolume', '1', sudo=True)

        # create a v1 snapshot, to prevent auto upgrades
        if has_snapshot:
            snappath = os.path.join(createpath, self.get_snap_dir_name(), "fake")
            self.mount_a.run_shell(['sudo', 'mkdir', '-p', snappath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # create a v1 .meta file
        cp = "/" + createpath
        meta_contents = f"[GLOBAL]\nversion = 1\ntype = {subvol_type}\npath = {cp}\nstate = {state}\n"
        meta_contents += "allow_subvolume_upgrade = 0\n"  # boolean
        if state == 'pending':
            # add a fake clone source
            meta_contents = meta_contents + '[source]\nvolume = fake\nsubvolume = fake\nsnapshot = fake\n'
        meta_filepath1 = os.path.join(self.mount_a.mountpoint, basepath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath1, meta_contents, sudo=True)
        return createpath

    def _create_subvolume(self, version, subvol_name, subvol_group=None):
        if version == 1:
            self._create_v1_subvolume(subvol_name, subvol_group)
        elif version >= 2:
            if subvol_group:
                self._fs_cmd('subvolume', 'create', 'cephfs', subvol_name, '--group_name', subvol_group)
            else:
                self._fs_cmd('subvolume', 'create', 'cephfs', subvol_name)
        else:
            self.assertTrue('NoSuchSubvolumeVersion' == None)

    def _get_subvol_snapdir_path(self, version, subvol, group):
        args = ['subvolume', 'getpath', 'cephfs', subvol]
        if group:
            args += ['--group_name', group]

        path = self.get_ceph_cmd_stdout("fs", *args).rstrip()
        if version >= 2:
            path += "/.."
        return path[1:]

    def _verify_snap_schedule(self, version, subvol, group):
        time.sleep(75)
        path = self._get_subvol_snapdir_path(version, subvol, group)
        path += "/" + self.get_snap_dir_name()
        snaps = self.mount_a.ls(path=path)
        log.debug(f"snaps:{snaps}")
        count = 0
        for snapname in snaps:
            if snapname.startswith("scheduled-"):
                count += 1
        # confirm presence of snapshot dir under .snap dir
        self.assertGreater(count, 0)

    def test_snap_schedule_subvol_and_group_arguments_01(self):
        """
        Test subvol schedule creation succeeds for default subvolgroup.
        """
        self._create_subvolume(self.CREATE_VERSION, 'sv01')
        self.fs_snap_schedule_cmd('add', '--subvol', 'sv01', path='.', snap_schedule='1m')

        self._verify_snap_schedule(self.CREATE_VERSION, 'sv01', None)
        path = self._get_subvol_snapdir_path(self.CREATE_VERSION, 'sv01', None)
        self.remove_snapshots(path, self.get_snap_dir_name())

        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv01', path='.', snap_schedule='1m')
        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv01')

    def test_snap_schedule_subvol_and_group_arguments_02(self):
        """
        Test subvol schedule creation fails for non-default subvolgroup.
        """
        self._create_subvolume(self.CREATE_VERSION, 'sv02')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', '--subvol', 'sv02', '--group', 'mygrp02', path='.', snap_schedule='1m')
        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv02')

    def test_snap_schedule_subvol_and_group_arguments_03(self):
        """
        Test subvol schedule creation fails when subvol exists only under default group.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp03')
        self._create_subvolume(self.CREATE_VERSION, 'sv03', 'mygrp03')

        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', '--subvol', 'sv03', path='.', snap_schedule='1m')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv03', '--group_name', 'mygrp03')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp03')

    def test_snap_schedule_subvol_and_group_arguments_04(self):
        """
        Test subvol schedule creation fails without subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp04')
        self._create_subvolume(self.CREATE_VERSION, 'sv04', 'mygrp04')

        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('add', '--group', 'mygrp04', path='.', snap_schedule='1m')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv04', '--group_name', 'mygrp04')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp04')

    def test_snap_schedule_subvol_and_group_arguments_05(self):
        """
        Test subvol schedule creation succeeds for a subvol under a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp05')
        self._create_subvolume(self.CREATE_VERSION, 'sv05', 'mygrp05')
        self.fs_snap_schedule_cmd('add', '--subvol', 'sv05', '--group', 'mygrp05', path='.', snap_schedule='1m', fs='cephfs')

        self._verify_snap_schedule(self.CREATE_VERSION, 'sv05', 'mygrp05')
        path = self._get_subvol_snapdir_path(self.CREATE_VERSION, 'sv05', 'mygrp05')
        self.remove_snapshots(path, self.get_snap_dir_name())

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv05', '--group_name', 'mygrp05')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp05')

    def test_snap_schedule_subvol_and_group_arguments_06(self):
        """
        Test subvol schedule listing fails without a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp06')
        self._create_subvolume(self.CREATE_VERSION, 'sv06', 'mygrp06')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv06', '--group', 'mygrp06', path='.', snap_schedule='1m', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('list', '--subvol', 'sv06', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv06', '--group', 'mygrp06', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv06', '--group_name', 'mygrp06')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp06')

    def test_snap_schedule_subvol_and_group_arguments_07(self):
        """
        Test subvol schedule listing fails without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp07')
        self._create_subvolume(self.CREATE_VERSION, 'sv07', 'mygrp07')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv07', '--group', 'mygrp07', path='.', snap_schedule='1m', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('list', '--group', 'mygrp07', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv07', '--group', 'mygrp07', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv07', '--group_name', 'mygrp07')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp07')

    def test_snap_schedule_subvol_and_group_arguments_08(self):
        """
        Test subvol schedule listing succeeds with a subvol and a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp08')
        self._create_subvolume(self.CREATE_VERSION, 'sv08', 'mygrp08')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv08', '--group', 'mygrp08', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('list', '--subvol', 'sv08', '--group', 'mygrp08', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv08', '--group', 'mygrp08', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv08', '--group_name', 'mygrp08')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp08')

    def test_snap_schedule_subvol_and_group_arguments_09(self):
        """
        Test subvol schedule retention add fails for a subvol without a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp09')
        self._create_subvolume(self.CREATE_VERSION, 'sv09', 'mygrp09')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv09', '--group', 'mygrp09', path='.', snap_schedule='1m', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv09', path='.', retention_spec_or_period='h', retention_count='5')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv09', '--group', 'mygrp09', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv09', '--group_name', 'mygrp09')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp09')

    def test_snap_schedule_subvol_and_group_arguments_10(self):
        """
        Test subvol schedule retention add fails for a subvol without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp10')
        self._create_subvolume(self.CREATE_VERSION, 'sv10', 'mygrp10')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv10', '--group', 'mygrp10', path='.', snap_schedule='1m', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('retention', 'add', '--group', 'mygrp10', path='.', retention_spec_or_period='h', retention_count='5')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv10', '--group', 'mygrp10', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv10', '--group_name', 'mygrp10')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp10')

    def test_snap_schedule_subvol_and_group_arguments_11(self):
        """
        Test subvol schedule retention add succeeds for a subvol within a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp11')
        self._create_subvolume(self.CREATE_VERSION, 'sv11', 'mygrp11')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv11', '--group', 'mygrp11', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv11', '--group', 'mygrp11', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv11', '--group', 'mygrp11', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv11', '--group_name', 'mygrp11')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp11')

    def test_snap_schedule_subvol_and_group_arguments_12(self):
        """
        Test subvol schedule activation fails for a subvol without a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp12')
        self._create_subvolume(self.CREATE_VERSION, 'sv12', 'mygrp12')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv12', '--group', 'mygrp12', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv12', '--group', 'mygrp12', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('activate', '--subvol', 'sv12', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv12', '--group', 'mygrp12', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv12', '--group_name', 'mygrp12')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp12')

    def test_snap_schedule_subvol_and_group_arguments_13(self):
        """
        Test subvol schedule activation fails for a subvol without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp13')
        self._create_subvolume(self.CREATE_VERSION, 'sv13', 'mygrp13')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv13', '--group', 'mygrp13', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv13', '--group', 'mygrp13', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('activate', '--group', 'mygrp13', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv13', '--group', 'mygrp13', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv13', '--group_name', 'mygrp13')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp13')

    def test_snap_schedule_subvol_and_group_arguments_14(self):
        """
        Test subvol schedule activation succeeds for a subvol within a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp14')
        self._create_subvolume(self.CREATE_VERSION, 'sv14', 'mygrp14')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv14', '--group', 'mygrp14', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv14', '--group', 'mygrp14', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv14', '--group', 'mygrp14', path='.', fs='cephfs')

        self._verify_snap_schedule(self.CREATE_VERSION, 'sv14', 'mygrp14')
        path = self._get_subvol_snapdir_path(self.CREATE_VERSION, 'sv14', 'mygrp14')
        self.remove_snapshots(path, self.get_snap_dir_name())

        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv14', '--group', 'mygrp14', path='.', snap_schedule='1m', fs='cephfs')
        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv14', '--group_name', 'mygrp14')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp14')

    def test_snap_schedule_subvol_and_group_arguments_15(self):
        """
        Test subvol schedule deactivation fails for a subvol without a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp15')
        self._create_subvolume(self.CREATE_VERSION, 'sv15', 'mygrp15')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv15', '--group', 'mygrp15', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv15', '--group', 'mygrp15', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv15', '--group', 'mygrp15', path='.', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv15', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv15', '--group', 'mygrp15', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv15', '--group_name', 'mygrp15')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp15')

    def test_snap_schedule_subvol_and_group_arguments_16(self):
        """
        Test subvol schedule deactivation fails for a subvol without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp16')
        self._create_subvolume(self.CREATE_VERSION, 'sv16', 'mygrp16')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv16', '--group', 'mygrp16', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv16', '--group', 'mygrp16', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv16', '--group', 'mygrp16', path='.', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('deactivate', '--group', 'mygrp16', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv16', '--group', 'mygrp16', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv16', '--group_name', 'mygrp16')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp16')

    def test_snap_schedule_subvol_and_group_arguments_17(self):
        """
        Test subvol schedule deactivation succeeds for a subvol within a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp17')
        self._create_subvolume(self.CREATE_VERSION, 'sv17', 'mygrp17')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv17', '--group', 'mygrp17', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv17', '--group', 'mygrp17', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv17', '--group', 'mygrp17', path='.', fs='cephfs')

        self._verify_snap_schedule(self.CREATE_VERSION, 'sv17', 'mygrp17')
        path = self._get_subvol_snapdir_path(self.CREATE_VERSION, 'sv17', 'mygrp17')
        self.remove_snapshots(path, self.get_snap_dir_name())

        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv17', '--group', 'mygrp17', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv17', '--group', 'mygrp17', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv17', '--group_name', 'mygrp17')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp17')

    def test_snap_schedule_subvol_and_group_arguments_18(self):
        """
        Test subvol schedule retention remove fails for a subvol without a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp18')
        self._create_subvolume(self.CREATE_VERSION, 'sv18', 'mygrp18')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv18', '--group', 'mygrp18', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv18', '--group', 'mygrp18', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv18', '--group', 'mygrp18', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv18', '--group', 'mygrp18', path='.', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('retention', 'remove', '--subvol', 'sv18', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv18', '--group', 'mygrp18', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv18', '--group_name', 'mygrp18')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp18')

    def test_snap_schedule_subvol_and_group_arguments_19(self):
        """
        Test subvol schedule retention remove fails for a subvol without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp19')
        self._create_subvolume(self.CREATE_VERSION, 'sv19', 'mygrp19')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv19', '--group', 'mygrp19', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv19', '--group', 'mygrp19', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv19', '--group', 'mygrp19', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv19', '--group', 'mygrp19', path='.', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('retention', 'remove', '--group', 'mygrp19', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv19', '--group', 'mygrp19', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv19', '--group_name', 'mygrp19')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp19')

    def test_snap_schedule_subvol_and_group_arguments_20(self):
        """
        Test subvol schedule retention remove succeeds for a subvol within a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp20')
        self._create_subvolume(self.CREATE_VERSION, 'sv20', 'mygrp20')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv20', '--group', 'mygrp20', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv20', '--group', 'mygrp20', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv20', '--group', 'mygrp20', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv20', '--group', 'mygrp20', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'remove', '--subvol', 'sv20', '--group', 'mygrp20', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv20', '--group', 'mygrp20', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv20', '--group_name', 'mygrp20')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp20')

    def test_snap_schedule_subvol_and_group_arguments_21(self):
        """
        Test subvol schedule remove fails for a subvol without a subvolgroup argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp21')
        self._create_subvolume(self.CREATE_VERSION, 'sv21', 'mygrp21')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv21', '--group', 'mygrp21', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv21', '--group', 'mygrp21', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv21', '--group', 'mygrp21', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv21', '--group', 'mygrp21', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'remove', '--subvol', 'sv21', '--group', 'mygrp21', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('remove', '--subvol', 'sv21', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv21', '--group', 'mygrp21', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv21', '--group_name', 'mygrp21')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp21')

    def test_snap_schedule_subvol_and_group_arguments_22(self):
        """
        Test subvol schedule remove fails for a subvol without a subvol argument.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp22')
        self._create_subvolume(self.CREATE_VERSION, 'sv22', 'mygrp22')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv22', '--group', 'mygrp22', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv22', '--group', 'mygrp22', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv22', '--group', 'mygrp22', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv22', '--group', 'mygrp22', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'remove', '--subvol', 'sv22', '--group', 'mygrp22', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        with self.assertRaises(CommandFailedError):
            self.fs_snap_schedule_cmd('remove', '--group', 'mygrp22', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv22', '--group', 'mygrp22', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv22', '--group_name', 'mygrp22')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp22')

    def test_snap_schedule_subvol_and_group_arguments_23(self):
        """
        Test subvol schedule remove succeeds for a subvol within a subvolgroup.
        """
        self._fs_cmd('subvolumegroup', 'create', 'cephfs', 'mygrp23')
        self._create_subvolume(self.CREATE_VERSION, 'sv23', 'mygrp23')

        self.fs_snap_schedule_cmd('add', '--subvol', 'sv23', '--group', 'mygrp23', path='.', snap_schedule='1m', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'add', '--subvol', 'sv23', '--group', 'mygrp23', path='.', retention_spec_or_period='h', retention_count=5, fs='cephfs')
        self.fs_snap_schedule_cmd('activate', '--subvol', 'sv23', '--group', 'mygrp23', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('deactivate', '--subvol', 'sv23', '--group', 'mygrp23', path='.', fs='cephfs')
        self.fs_snap_schedule_cmd('retention', 'remove', '--subvol', 'sv23', '--group', 'mygrp23', path='.', retention_spec_or_period='h', retention_count='5', fs='cephfs')
        self.fs_snap_schedule_cmd('remove', '--subvol', 'sv23', '--group', 'mygrp23', path='.', snap_schedule='1m', fs='cephfs')

        self._fs_cmd('subvolume', 'rm', 'cephfs', 'sv23', '--group_name', 'mygrp23')
        self._fs_cmd('subvolumegroup', 'rm', 'cephfs', 'mygrp23')

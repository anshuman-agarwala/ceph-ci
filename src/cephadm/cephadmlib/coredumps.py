# coredumps.py - cephadm functionality related to coredumps

import logging

from pathlib import Path

from cephadmlib.call_wrappers import call_throws
from cephadmlib.context import CephadmContext
from cephadmlib.file_utils import read_file, write_new

logger = logging.getLogger()


def set_coredump_max_size(ctx: CephadmContext, fsid: str, size: str) -> None:
    """
    Override max coredump size with a custom value using a systemd drop-in.
    Useful for debugging in ceph clusters
    """
    coredump_overrides_dir = Path('/etc/systemd/coredump.conf.d')
    coredump_override_path = Path(coredump_overrides_dir).joinpath(
        f'90-cephadm-{fsid}-coredumps-size.conf'
    )

    if coredump_override_path.exists():
        coredump_override_content = read_file([str(coredump_override_path)])
        if (
            f'ProcessSizeMax={size}' in coredump_override_content
            and f'ExternalSizeMax={size}' in coredump_override_content
        ):
            logger.info(
                f'{str(coredump_override_path)} already has sizes set to {size}, skipping...'
            )
            return

    if not coredump_overrides_dir.is_dir():
        coredump_overrides_dir.mkdir(parents=True, exist_ok=True)

    lines = [
        '# created by cephadm',
        '[Coredump]',
        f'ProcessSizeMax={size}',
        f'ExternalSizeMax={size}',
    ]

    with write_new(coredump_override_path, owner=None, perms=None) as f:
        f.write('\n'.join(lines))

    logger.info(
        f'Set coredump max sizes in {str(coredump_override_path)} to {size}. '
        'Restarting systemd-coredump.socket'
    )
    call_throws(ctx, ['systemctl', 'restart', 'systemd-coredump.socket'])
    logger.info('systemd-coredump.socket restarted successfully')

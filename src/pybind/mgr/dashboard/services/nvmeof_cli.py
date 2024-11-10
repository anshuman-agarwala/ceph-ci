# -*- coding: utf-8 -*-
import errno
import json

from mgr_module import CLICheckNonemptyFileInput, CLIReadCommand, CLIWriteCommand

from ..rest_client import RequestException
from .nvmeof_conf import ManagedByOrchestratorException, \
    NvmeofGatewayAlreadyExists, NvmeofGatewaysConfig
from ..services.nvmeof_client import NVMeoFClient


try:
    from ..controllers.nvmeof import NVMeoFSubsystem

    test = NVMeoFSubsystem()

    @CLIReadCommand('dashboard tomer-test2')
    def test123(_, gw_group):
        return 0, test.list(gw_group)
except:
    @CLIReadCommand('dashboard tomer-test-exception-caught')
    def test123(_, gw_group):
        return 0, json.dumps({'a':1})


@CLIReadCommand('dashboard tomer-test3')
def list_nvmeof_gateways2(_, gw_group):
    return NVMeoFClient(gw_group=gw_group).stub.get_gateway_info(
                NVMeoFClient.pb2.get_gateway_info_req()
            )

@CLIReadCommand('dashboard tomer-test')
def list_nvmeof_gateways2(_):
    '''
    List NVMe-oF gateways
    '''
    return 0, json.dumps(NvmeofGatewaysConfig.get_gateways_config()), ''


@CLIReadCommand('dashboard nvmeof-gateway-list')
def list_nvmeof_gateways(_):
    '''
    List NVMe-oF gateways
    '''
    return 0, json.dumps(NvmeofGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard nvmeof-gateway-add')
@CLICheckNonemptyFileInput(desc='NVMe-oF gateway configuration')
def add_nvmeof_gateway(_, inbuf, name: str, group: str, daemon_name: str):
    '''
    Add NVMe-oF gateway configuration. Gateway URL read from -i <file>
    '''
    service_url = inbuf
    try:
        NvmeofGatewaysConfig.add_gateway(name, service_url, group, daemon_name)
        return 0, 'Success', ''
    except NvmeofGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
    except RequestException as ex:
        return -errno.EINVAL, '', str(ex)


@CLIWriteCommand('dashboard nvmeof-gateway-rm')
def remove_nvmeof_gateway(_, name: str, daemon_name: str = ''):
    '''
    Remove NVMe-oF gateway configuration
    '''
    try:
        NvmeofGatewaysConfig.remove_gateway(name, daemon_name)
        return 0, 'Success', ''
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)

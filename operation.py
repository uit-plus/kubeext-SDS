import os
import traceback
from xml.etree.ElementTree import fromstring
from xmljson import badgerfish as bf
from json import dumps, loads
from sys import exit

from utils.exception import *
from utils.libvirt_util import get_pool_info, get_volume_xml
from utils.utils import *
from utils import logger


LOG = 'kubesds.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)

class Operation(object):
    def __init__(self, cmd, params, with_result=False):
        if cmd is None or cmd == '':
            raise Exception('plz give me right cmd.')
        if not isinstance(params, dict):
            raise Exception('plz give me right parameters.')

        self.params = params
        self.cmd = cmd
        self.params = params
        self.with_result = with_result

    def get_cmd(self):
        cmd = self.cmd
        for key in self.params.keys():
            cmd = cmd + ' --' + key + ' ' + self.params[key] + ' '
        return cmd

    def execute(self):
        cmd = self.get_cmd()
        logger.debug(cmd)

        if self.with_result:
            return runCmdWithResult(cmd)
        else:
            return runCmdAndCheckReturnCode(cmd)



# class Executor(object):
#     def __init__(self, ops):
#         self.ops = ops
#
#     def get_cmd(self):
#         cmd = ''
#         for k, v in self.params:
#             cmd = self.cmd + ' ' + k + ' ' + v + ' '
#         return cmd
#
#     def execute(self):
#         if self.cmd is None:
#             raise Exception("not found cmd to execute")
#         cmd = self.get_cmd()
#         if self.with_result:
#             return runCmdWithResult(cmd)
#         else:
#             return runCmdAndCheckReturnCode(cmd)


def createPool(type, params):
    result = None
    try:
        if type == 'dir':
            POOL_PATH = params['target']
            if not os.path.isdir(POOL_PATH):
                os.makedirs(POOL_PATH)

            op = Operation('virsh pool-create-as', {'name': params['poolname'], 'type': 'dir', 'target': params['target']})
            op.execute()

            result = get_pool_info(params['poolname'])
        elif type == 'uus':
            # {"result":{"code":0, "msg":"success"}, "data":{"status": "active", "used": 1000, "poolname": "pool1", "url": "uus_://192.168.3.10:7000", "proto": "uus", "free": 2000, "disktype": "uus_", "export-mode": "3", "maintain": "normal", "total": 3000}, "obj":"pooladd"}
            op = Operation('cstor-cli pooladd-uus', params, with_result=True)
            uus_poolinfo = op.execute()
            result = {'name': params['poolname'], 'pooltype': 'uus', 'capacity': uus_poolinfo['data']['total'], 'autostart': 'yes', 'path': uus_poolinfo['data']['url'], 'state': 'running', 'uuid': randomUUID()}
        elif type == 'nfs':
            op1 = Operation('cstor-cli pooladd-nfs', params, with_result=True)
            poolinfo = op1.execute()
            if poolinfo['result']['code'] != 0:
                print poolinfo
                exit(1)
            # {"result":{"code":0, "msg":"success"}, "data":{"opt": "nolock", "status": "active", "mountpath": "/mnt/cstor/var/lib/libvirt/nfs/", "proto": "nfs", "url": "192.168.3.99:/nfs/nfs", "poolname": "pool2", "free": 549, "disktype": "file", "maintain": "normal", "used": 0, "total": 549}, "obj":"pooladd"}

            # create dir pool in virsh
            kv = {'type': 'dir', 'target': poolinfo['data']['mountpath']+'/'+params['poolname'], 'name': params['poolname']}
            op2 = Operation('virsh pool-create-as', kv)
            op2.execute()

            result = get_pool_info(params['poolname'])
        elif type == 'glusterfs':
            op1 = Operation('cstor-cli pooladd-glusterfs', params, with_result=True)
            poolinfo = op1.execute()
            if poolinfo['result']['code'] != 0:
                print poolinfo
                exit(1)

            # create dir pool in virsh
            kv = {'type': 'dir', 'target': poolinfo['data']['mountpath']+'/'+params['poolname'], 'name': params['poolname']}
            op2 = Operation('virsh pool-create-as', kv)
            op2.execute()

            result = get_pool_info(params['poolname'])
        print dumps({'result': {'code': 0, 'msg': 'create pool '+params['poolname']+' successful.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create pool ' + params['poolname'] + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create pool ' + params['poolname'] + '.'}})
        exit(1)


def deletePool(type, params):
    result = None
    try:
        if type == 'dir':
            op = Operation('virsh pool-destroy '+params['poolname'], {})
            op.execute()
            result = {'code': 0, 'msg': 'delete pool '+params['poolname']+' success'}
        elif type == 'uus':
            op = Operation('cstor-cli pool-remove', params, with_result=True)
            result = op.execute()
        elif type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh pool-destroy '+params['poolname'], {})
            op.execute()

            op = Operation('cstor-cli pool-remove', params, with_result=True)
            result = op.execute()
            # {"result": {"code": 0, "msg": "success"}, "data": {}, "obj": "poolname"}
        print dumps({'result': {'code': 0, 'msg': 'delete pool '+params['poolname']+' successful.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while delete pool ' + params['poolname'] + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool '+ params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while delete pool ' + params['poolname'] + '.'}})
        exit(1)

def createDisk(type, params):
    try:
        if type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-create-as', {'pool': params['poolname'], 'name': params['name'], 'capacity': params['capacity'], 'format': params['format']})
            op.execute()
            vol_xml = get_volume_xml(params['poolname'], params['name'])
            result = loads(xmlToJson(vol_xml))
            print dumps({'result': {'code': 0, 'msg': 'create disk '+params['name']+' successful.'}, 'data': result})
        elif type == 'uus':
            op1 = Operation('cstor-cli vdisk-create', {'poolname': params['poolname'], 'name': params['name'], 'size': params['capacity']}, with_result=True)
            diskinfo = op1.execute()
            if diskinfo['result']['code'] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {'poolname': params['poolname'], 'name': params['name'], 'uni': diskinfo['data']['uni']}
            op2 = Operation('cstor-cli vdisk-prepare', kv, with_result=True)
            prepareInfo = op2.execute()
            # delete the disk
            if prepareInfo['result']['code'] != 0:
                op3 = Operation('cstor-cli vdisk-remove', params, with_result=True)
                rmDiskInfo = op3.execute()
                if rmDiskInfo['result']['code'] == 0:
                    print dumps({'result': {'code': 1, 'msg': 'error: create disk success but can not prepare disk' + params['name'] + '.'}})
                else:
                    print dumps({'result': {'code': 1, 'msg': 'error: can not prepare disk and roll back fail(can not delete the disk)' + params['name'] + '. '}})
                exit(1)
            else:
                result = {
                    'type': 'clouddisk',
                    'name': {'text': params['name']},
                    'capacity': {'text': params['capacity']},
                    'target': {'path': prepareInfo['data']['path']},
                     'uni': diskinfo['data']['uni'],
                    'state': 'running',
                    'uuid': randomUUID()
                }
                print dumps({'result': {'code': 0,
                                        'msg': 'create disk '+params['poolname']+' success.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create disk ' + params['name'] + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create disk ' + params['name']}})
        exit(1)


def deleteDisk(type, params):
    try:
        if type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-delete', {'pool': params['poolname'], 'vol': params['name']})
            op.execute()
            print dumps({'result': {'code': 0, 'msg': 'delete volume '+params['name']+' success.'}})
        elif type == 'uus':
            op1 = Operation('cstor-cli vdisk-show', params, with_result=True)
            diskinfo = op1.execute()
            if diskinfo['result']['code'] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {'poolname': params['poolname'], 'name': params['name'], 'uni': diskinfo['data']['uni']}
            op = Operation('cstor-cli vdisk-release', kv, True)
            releaseInfo = op.execute()
            if releaseInfo['result']['code'] != 0:
                print dumps(releaseInfo)
                exit(1)

            op = Operation('cstor-cli vdisk-remove', params, with_result=True)
            result = op.execute()
            print dumps(result)
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while delete disk ' + params['name'] + '. '+e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'code': 1, 'msg': 'error occur while delete disk ' + params['name'] + '.'}
        exit(1)

def resizeDisk(type, params):
    result = None
    try:
        if type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-resize', {'pool': params['poolname'], 'vol': params['name'], 'capacity': params['capacity']})
            op.execute()
            vol_xml = get_volume_xml(params['poolname'], params['name'])
            result = loads(xmlToJson(vol_xml))
            print dumps({'result': {'code': 0, 'msg': 'resize disk ' + params['name'] + ' successful.'}, 'data': result})

        elif type == 'uus':
            kv = {'poolname': params['poolname'], 'name': params['name'], 'size': params['size']}
            op = Operation('cstor-cli vdisk-expand', kv, True)
            diskinfo = op.execute()

            if diskinfo['result']['code'] == 0:
                result = {
                    '_type': 'clouddisk',
                    'name': {'text': params['name']},
                    'capacity': {'_unit': 'bytes', 'text': params['capacity']},
                    'target': {'format': {'_type: uus'}, 'path': diskinfo['data']['path']},
                    'uni': diskinfo['data']['uni'],
                    'state': 'running',
                    'uuid': randomUUID()
                }
                print dumps({'result': {'code': 0,
                                        'msg': 'resize disk ' + params['poolname'] + ' success.'}, 'data': result})
            else:
                print dumps(diskinfo)
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while resize disk ' + params['name'] + '. '+e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while resize disk ' + params['name']}}
        exit(1)

def cloneDisk(type, params):
    try:
        if type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-clone', {'pool': params['poolname'], 'vol': params['name'], 'newname': params['newname']})
            op.execute()

            vol_xml = get_volume_xml(params['poolname'], params['name'])
            result = loads(xmlToJson(vol_xml))
            print dumps(
                {'result': {'code': 0, 'msg': 'resize disk ' + params['name'] + ' successful.'}, 'data': result})
        elif type == 'uus':
            kv = {'poolname': params['poolname'], 'name': params['name'], 'clonename': params['newname']}
            op = Operation('cstor-cli vdisk-clone', kv, True)
            result = op.execute()
            print dumps(result)
    except ExecuteException, e:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while clone disk ' + params['name'] + '. ' + e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params['poolname'])
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while clone disk ' + params['name']}}
        exit(1)

def xmlToJson(xmlStr):
    json = dumps(bf.data(fromstring(xmlStr)), sort_keys=True, indent=4)
    return json.replace('@', '_').replace('$', 'text').replace(
        'interface', '_interface').replace('transient', '_transient').replace(
        'nested-hv', 'nested_hv').replace('suspend-to-mem', 'suspend_to_mem').replace('suspend-to-disk',
                                                                                      'suspend_to_disk')

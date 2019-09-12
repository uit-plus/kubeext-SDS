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
        print cmd
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


def createPool(params):
    result = None
    try:
        if params.type == 'dir':
            POOL_PATH = params.target
            if not os.path.isdir(POOL_PATH):
                os.makedirs(POOL_PATH)

            op = Operation('virsh pool-create-as', {'name': params.pool, 'type': 'dir', 'target': params.target})
            op.execute()

            result = get_pool_info(params.pool)
        elif params.type == 'uus':
            # {"result":{"code":0, "msg":"success"}, "data":{"status": "active", "used": 1000, "pool": "pool1", "url": "uus_://192.168.3.10:7000", "proto": "uus", "free": 2000, "disktype": "uus_", "export-mode": "3", "maintain": "normal", "total": 3000}, "obj":"pooladd"}
            kv = {'poolname': params.pool, 'url': params.url}
            op = Operation('cstor-cli pooladd-uus', kv, with_result=True)
            uus_poolinfo = op.execute()
            result = {'name': params.pool, 'pooltype': 'uus', 'capacity': uus_poolinfo['data']['total'], 'autostart': 'yes', 'path': uus_poolinfo['data']['url'], 'state': 'running', 'uuid': randomUUID()}
        elif params.type == 'nfs':
            kv = {'poolname': params.pool, 'url': params.url, 'target': params.target}
            if params.opt is not None:
                kv['opt'] = params.opt
            op1 = Operation('cstor-cli pooladd-nfs', kv, with_result=True)
            poolinfo = op1.execute()
            if poolinfo['result']['code'] != 0:
                print poolinfo
                exit(1)
            # {"result":{"code":0, "msg":"success"}, "data":{"opt": "nolock", "status": "active", "mountpath": "/mnt/cstor/var/lib/libvirt/nfs/", "proto": "nfs", "url": "192.168.3.99:/nfs/nfs", "pool": "pool2", "free": 549, "disktype": "file", "maintain": "normal", "used": 0, "total": 549}, "obj":"pooladd"}

            # create dir pool in virsh
            kv = {'type': 'dir', 'target': poolinfo['data']['mountpath']+'/'+params.pool, 'name': params.pool}
            op2 = Operation('virsh pool-create-as', kv)
            op2.execute()

            result = get_pool_info(params.pool)
        elif params.type == 'glusterfs':
            kv = {'poolname': params.pool, 'url': params.url, 'target': params.target}
            op1 = Operation('cstor-cli pooladd-glusterfs', kv, with_result=True)
            poolinfo = op1.execute()
            if poolinfo['result']['code'] != 0:
                print poolinfo
                exit(1)

            # create dir pool in virsh
            kv = {'type': 'dir', 'target': poolinfo['data']['mountpath']+'/'+params.pool, 'name': params.pool}
            op2 = Operation('virsh pool-create-as', kv)
            op2.execute()

            result = get_pool_info(params.pool)
        print dumps({'result': {'code': 0, 'msg': 'create pool '+params.pool+' successful.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create pool ' + params.pool + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create pool ' + params.pool + '.'}})
        exit(1)


def deletePool(params):
    result = None
    try:
        if params.type == 'dir':
            op = Operation('virsh pool-destroy '+params.pool, {})
            op.execute()
            result = {'code': 0, 'msg': 'delete pool '+params.pool+' success'}
        elif params.type == 'uus':
            kv = {'poolname': params.pool}
            op = Operation('cstor-cli pool-remove', kv, with_result=True)
            result = op.execute()
        elif params.type == 'nfs' or type == 'glusterfs':
            kv = {'pool': params.pool}
            op = Operation('virsh pool-destroy', kv)
            op.execute()

            kv = {'poolname': params.pool}
            op = Operation('cstor-cli pool-remove', kv, with_result=True)
            result = op.execute()
            # {"result": {"code": 0, "msg": "success"}, "data": {}, "obj": "pool"}
        print dumps({'result': {'code': 0, 'msg': 'delete pool '+params.pool+' successful.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while delete pool ' + params.pool + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool '+ params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while delete pool ' + params.pool + '.'}})
        exit(1)

def createDisk(params):
    try:
        if params.type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-create-as', {'pool': params.pool, 'name': params.name, 'capacity': params.capacity, 'format': params.format})
            op.execute()
            vol_xml = get_volume_xml(params.pool, params.name)
            result = loads(xmlToJson(vol_xml))
            print dumps({'result': {'code': 0, 'msg': 'create disk '+params.name+' successful.'}, 'data': result})
        elif params.type == 'uus':
            kv = {'poolname': params.pool, 'name': params.name, 'size': params.capacity}
            op1 = Operation('cstor-cli vdisk-create', kv, with_result=True)
            diskinfo = op1.execute()
            if diskinfo['result']['code'] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {'poolname': params.pool, 'name': params.name, 'uni': diskinfo['data']['uni']}
            op2 = Operation('cstor-cli vdisk-prepare', kv, with_result=True)
            prepareInfo = op2.execute()
            # delete the disk
            if prepareInfo['result']['code'] != 0:
                kv = {'poolname': params.pool, 'name': params.vol}
                op3 = Operation('cstor-cli vdisk-remove', kv, with_result=True)
                rmDiskInfo = op3.execute()
                if rmDiskInfo['result']['code'] == 0:
                    print dumps({'result': {'code': 1, 'msg': 'error: create disk success but can not prepare disk' + params.vol + '.'}})
                else:
                    print dumps({'result': {'code': 1, 'msg': 'error: can not prepare disk and roll back fail(can not delete the disk)' + params.vol + '. '}})
                exit(1)
            else:
                result = {
                    'type': 'clouddisk',
                    'name': {'text': params.vol},
                    'capacity': {'text': params.capacity},
                    'target': {'path': prepareInfo['data']['path']},
                     'uni': diskinfo['data']['uni'],
                    'state': 'running',
                    'uuid': randomUUID()
                }
                print dumps({'result': {'code': 0,
                                        'msg': 'create disk '+params.pool+' success.'}, 'data': result})
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create disk ' + params.vol + '. '+e.message}})
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({'result': {'code': 1, 'msg': 'error occur while create disk ' + params.vol}})
        exit(1)


def deleteDisk(params):
    try:
        if params.type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-delete', {'pool': params.pool, 'vol': params.vol})
            op.execute()
            print dumps({'result': {'code': 0, 'msg': 'delete volume '+params.vol+' success.'}})
        elif params.type == 'uus':
            kv = {'poolname': params.pool, 'name': params.vol}
            op1 = Operation('cstor-cli vdisk-show', kv, with_result=True)
            diskinfo = op1.execute()
            if diskinfo['result']['code'] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {'poolname': params.pool, 'name': params.vol, 'uni': diskinfo['data']['uni']}
            op = Operation('cstor-cli vdisk-release', kv, True)
            releaseInfo = op.execute()
            if releaseInfo['result']['code'] != 0:
                print dumps(releaseInfo)
                exit(1)

            kv = {'poolname': params.pool, 'name': params.vol}
            op = Operation('cstor-cli vdisk-remove', kv, with_result=True)
            result = op.execute()
            print dumps(result)
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while delete disk ' + params.vol + '. '+e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'code': 1, 'msg': 'error occur while delete disk ' + params.vol + '.'}
        exit(1)

def resizeDisk(params):
    result = None
    try:
        if params.type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-resize', {'pool': params.pool, 'vol': params.vol, 'capacity': params.capacity})
            op.execute()
            vol_xml = get_volume_xml(params.pool, params.vol)
            result = loads(xmlToJson(vol_xml))
            print dumps({'result': {'code': 0, 'msg': 'resize disk ' + params.vol + ' successful.'}, 'data': result})

        elif params.type == 'uus':
            kv = {'poolname': params.pool, 'name': params.vol, 'size': params.size}
            op = Operation('cstor-cli vdisk-expand', kv, True)
            diskinfo = op.execute()

            if diskinfo['result']['code'] == 0:
                result = {
                    '_type': 'clouddisk',
                    'name': {'text': params.vol},
                    'capacity': {'_unit': 'bytes', 'text': params.capacity},
                    'target': {'format': {'_type: uus'}, 'path': diskinfo['data']['path']},
                    'uni': diskinfo['data']['uni'],
                    'state': 'running',
                    'uuid': randomUUID()
                }
                print dumps({'result': {'code': 0,
                                        'msg': 'resize disk ' + params.pool + ' success.'}, 'data': result})
            else:
                print dumps(diskinfo)
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while resize disk ' + params.vol + '. '+e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while resize disk ' + params.vol}}
        exit(1)

def cloneDisk(params):
    try:
        if params.type == 'dir' or type == 'nfs' or type == 'glusterfs':
            op = Operation('virsh vol-clone', {'pool': params.pool, 'vol': params.vol, 'newname': params.newname})
            op.execute()

            vol_xml = get_volume_xml(params.pool, params.vol)
            result = loads(xmlToJson(vol_xml))
            print dumps(
                {'result': {'code': 0, 'msg': 'resize disk ' + params.vol + ' successful.'}, 'data': result})
        elif params.type == 'uus':
            kv = {'poolname': params.pool, 'name': params.vol, 'clonename': params.newname}
            op = Operation('cstor-cli vdisk-clone', kv, True)
            result = op.execute()
            print dumps(result)
    except ExecuteException, e:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while clone disk ' + params.vol + '. ' + e.message}}
        exit(1)
    except Exception:
        logger.debug('deletePool ' + params.pool)
        logger.debug(type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {'result': {'code': 1, 'msg': 'error occur while clone disk ' + params.vol}}
        exit(1)

def xmlToJson(xmlStr):
    json = dumps(bf.data(fromstring(xmlStr)), sort_keys=True, indent=4)
    return json.replace('@', '_').replace('$', 'text').replace(
        'interface', '_interface').replace('transient', '_transient').replace(
        'nested-hv', 'nested_hv').replace('suspend-to-mem', 'suspend_to_mem').replace('suspend-to-disk',
                                                                                      'suspend_to_disk')

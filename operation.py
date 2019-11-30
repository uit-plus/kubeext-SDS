import os
import traceback
import uuid
from xml.etree.ElementTree import fromstring
from xmljson import badgerfish as bf
from json import dumps, loads
from sys import exit

from utils.exception import *
from utils.libvirt_util import get_volume_xml, get_disks_spec, is_vm_active
from utils.utils import *
from utils import logger


LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

class Operation(object):
    def __init__(self, cmd, params, with_result=False, xml_to_json=False, kv_to_json=False):
        if cmd is None or cmd == "":
            raise Exception("plz give me right cmd.")
        if not isinstance(params, dict):
            raise Exception("plz give me right parameters.")

        self.params = params
        self.cmd = cmd
        self.params = params
        self.with_result = with_result
        self.xml_to_json = xml_to_json
        self.kv_to_json = kv_to_json

    def get_cmd(self):
        cmd = self.cmd
        for key in self.params.keys():
            cmd = cmd + " --" + key + " " + self.params[key] + " "
        return cmd

    def execute(self):
        cmd = self.get_cmd()
        logger.debug(cmd)

        if self.with_result:
            return runCmdWithResult(cmd)
        elif self.xml_to_json:
            return runCmdAndTransferXmlToJson(cmd)
        elif self.kv_to_json:
            return runCmdAndSplitKvToJson(cmd)
        else:
            return runCmd(cmd)



# class Executor(object):
#     def __init__(self, ops):
#         self.ops = ops
#
#     def get_cmd(self):
#         cmd = ""
#         for k, v in self.params:
#             cmd = self.cmd + " " + k + " " + v + " "
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
    logger.debug(params)
    result = None
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            #  {"result":{"code":0, "msg":"success"}, "data":{"status": "active", "mountpath": "/Disk240", "proto": "localfs", "url": "/dev/sdb1", "poolname": "pool1", "free": 223363817472, "disktype": "file", "maintain": "normal", "used": 768970752, "total": 236152303616}, "obj":"pooladd"}
            if params.type == "localfs":
                op = Operation('cstor-cli pooladd-localfs ', {'poolname': params.pool,
                                                              'url': params.url}, with_result=True)
            elif params.type == "nfs":
                kv = {"poolname": params.uuid, "url": params.url, "opt": params.opt}
                if params.path:
                    kv["path"] = params.path
                op = Operation("cstor-cli pooladd-nfs", kv, with_result=True)
            elif params.type == "glusterfs":
                kv = {"poolname": params.uuid, "url": params.url}
                op = Operation("cstor-cli pooladd-glusterfs", kv, with_result=True)
                if params.path:
                    kv["path"] = params.path
                if params.opt:
                    kv["opt"] = params.opt
            elif params.type == "vdiskfs":
                kv = {"poolname": params.pool, "url": params.url}
                if params.force:
                    kv["force"] = params.force
                op = Operation("cstor-cli pooladd-vdiskfs", kv, with_result=True)

            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            if params.type == "nfs" or params.type == "glusterfs":
                POOL_PATH = "%s/%s" % (cstor['data']['mountpath'], params.uuid)
            else:
                POOL_PATH = "%s/%s" % (cstor['data']['mountpath'], params.pool)
            if not os.path.isdir(POOL_PATH):
                raise ExecuteException('', 'cant not get pooladd-localfs mount path')
            # step1 define pool
            op1 = Operation("virsh pool-define-as", {"name": params.pool, "type": "dir", "target": POOL_PATH})
            op1.execute()

            # step2 autostart pool
            if params.autostart:
                try:
                    op2 = Operation("virsh pool-autostart", {"pool": params.pool})
                    op2.execute()
                except ExecuteException, e:
                    op_cancel = Operation("virsh pool-undefine", {"--pool": params.pool})
                    op_cancel.execute()
                    raise e

            op3 = Operation("virsh pool-start", {"pool": params.pool})
            op3.execute()

            with open(POOL_PATH +'/content', 'w') as f:
                f.write(params.content)

            result = get_pool_info(params.pool)
            result['content'] = params.content
            result["pooltype"] = params.type
            if is_pool_started(params.pool):
                result["state"] = "active"
            else:
                result["state"] = "inactive"
        elif params.type == "uus":
            # {"result":{"code":0, "msg":"success"}, "data":{"status": "active", "used": 1000, "pool": "pool1", "url": "uus_://192.168.3.10:7000", "proto": "uus", "free": 2000, "disktype": "uus_", "export-mode": "3", "maintain": "normal", "total": 3000}, "obj":"pooladd"}
            kv = {"poolname": params.pool, "url": params.url, "opt": params.opt}
            op = Operation("cstor-cli pooladd-uus", kv, with_result=True)
            uus_poolinfo = op.execute()

            result = {"name": params.pool, "pooltype": "uus", "capacity": uus_poolinfo["data"]["total"],
                      "autostart": "yes", "path": uus_poolinfo["data"]["url"], "state": "active", "uuid": randomUUID(), "content": 'vmd'}

        print dumps({"result": {"code": 0, "msg": "create pool "+params.pool+" successful."}, "data": result})
    except ExecuteException, e:
        logger.debug("createPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while create pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("createPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while create pool " + params.pool + "."}, "data": {}})
        exit(1)

def deletePool(params):
    result = None
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if is_pool_started(params.pool):
                raise ExecuteException('RunCmdError', 'pool '+params.pool+' still active, plz stop it first.')
            pool_path = get_pool_info(params.pool)['path']
            files = os.listdir(pool_path)
            content = ""
            if len(files) == 1 and 'content' in files:
                with open('%s/content' % pool_path, 'r') as f:
                    content = f.read()
                op = Operation('rm -f %s/content' % pool_path, {})
                op.execute()

            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli pool-remove ', {'poolname': uuid}, with_result=True)
            else:
                op = Operation('cstor-cli pool-remove ', {'poolname': params.pool}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                if content != "":
                    with open('%s/content' % pool_path, 'w') as f:
                        content = f.write(content)
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            if is_pool_defined(params.pool):
                op2 = Operation("virsh pool-undefine", {"pool": params.pool})
                op2.execute()
            result = {"msg": "delete pool "+params.pool+" success"}
        elif params.type == "uus":
            kv = {"poolname": params.pool}
            op = Operation("cstor-cli pool-remove", kv, with_result=True)
            result = op.execute()
        print dumps({"result": {"code": 0, "msg": "delete pool "+params.pool+" successful."}, "data": result})
    except ExecuteException, e:
        logger.debug("deletePool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while delete pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("deletePool "+ params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while delete pool " + params.pool + "."}, "data": {}})
        exit(1)

def startPool(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            op1 = Operation("virsh pool-start", {"pool": params.pool})
            op1.execute()
            result = get_pool_info(params.pool)
            result["pooltype"] = params.type
            with open(result['path'] +'/content', 'r') as f:
                params.content = f.read()
            result['content'] = params.content
            if is_pool_started(params.pool):
                result["state"] = "active"
            else:
                result["state"] = "inactive"
            print dumps(
                {"result": {"code": 0, "msg": "start pool " + params.pool + " successful."}, "data": result})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
    except ExecuteException, e:
        logger.debug("startPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while start pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("startPool "+ params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while start pool " + params.pool + "."}, "data": {}})
        exit(1)

def autoStartPool(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.disable:
                op = Operation("virsh pool-autostart --disable", {"pool": params.pool})
                op.execute()
            else:
                op = Operation("virsh pool-autostart", {"pool": params.pool})
                op.execute()
            result = get_pool_info(params.pool)
            result["pooltype"] = params.type
            if is_pool_started(params.pool):
                result["state"] = "active"
            else:
                result["state"] = "inactive"
            print dumps(
                {"result": {"code": 0, "msg": "autoStart pool " + params.pool + " successful."}, "data": result})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
    except ExecuteException, e:
        logger.debug("autoStartPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while autoStart pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("autoStartPool "+ params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while autoStart pool " + params.pool + "."}, "data": {}})
        exit(1)

def unregisterPool(params):
    try:
        if params.type == "localfs":
            deletePool(params)
        elif params.type == "nfs" or params.type == "glusterfs":
            print dumps(
                {"result": {"code": 500, "msg": params.pool + " is nfs or glusterfs." + "unregister pool " + params.pool + " will make pool be deleted, but mount point still exist."}, "data": {}})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
    except ExecuteException, e:
        logger.debug("unregisterPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while unregister pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("unregisterPool "+ params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while unregister pool " + params.pool + "."}, "data": {}})
        exit(1)

def stopPool(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            op1 = Operation("virsh pool-destroy", {"pool": params.pool})
            op1.execute()

            result = get_pool_info(params.pool)
            result["pooltype"] = params.type
            if is_pool_started(params.pool):
                result["state"] = "active"
            else:
                result["state"] = "inactive"
            print dumps(
                {"result": {"code": 0, "msg": "stop pool " + params.pool + " successful."}, "data": result})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs."}, "data": {}})
    except ExecuteException, e:
        logger.debug("stopPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while stop pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("stopPool "+ params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while stop pool " + params.pool + "."}, "data": {}})
        exit(1)

def showPool(params):
    result = None
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            pool_info = get_pool_info(params.pool)
            with open(pool_info['path'] +'/content', 'r') as f:
                content = f.read()

            result = get_pool_info(params.pool)
            result['content'] = content
            result["pooltype"] = params.type
            if is_pool_started(params.pool):
                result["state"] = "active"
            else:
                result["state"] = "inactive"
        elif params.type == "uus":
            kv = {"poolname": params.pool}
            op = Operation("cstor-cli pool-show", kv, with_result=True)
            uus_poolinfo = op.execute()

            result = {"name": params.pool, "pooltype": uus_poolinfo['data']['pooltype'], "capacity": uus_poolinfo["data"]["total"],
                      "autostart": "yes", "path": uus_poolinfo["data"]["url"], "state": "active", "uuid": randomUUID()}
        print dumps({"result": {"code": 0, "msg": "show pool "+params.pool+" successful."}, "data": result})
    except ExecuteException, e:
        logger.debug("showPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while show pool " + params.pool + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("showPool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while show pool " + params.pool + "."}, "data": {}})
        exit(1)

def createDisk(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-create ', {'poolname': uuid, 'name': params.vol,
                                                           'size': params.capacity}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-create ', {'poolname': params.pool, 'name': params.vol,
                                                           'size': params.capacity}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            pool_info = get_pool_info(params.pool)
            if not os.path.isdir(pool_info['path']):
                raise ExecuteException('', 'can not get pool path.')
            # create disk dir and create disk in dir.
            disk_dir = pool_info['path'] + '/' + params.vol
            if os.path.isdir(disk_dir):
                raise ExecuteException('', 'error: disk dir has exist.')
            os.makedirs(disk_dir)
            disk_path = disk_dir + '/' + params.vol
            op1 = Operation('qemu-img create -f ' + params.format + ' ' + disk_path + ' ' + params.capacity, {})
            op1.execute()

            config = {}
            config['name'] = params.vol
            config['dir'] = disk_dir
            config['current'] = disk_path

            with open(disk_dir + '/config.json', "w") as f:
                dump(config, f)

            result = get_disk_info(disk_path)
            # vol_xml = get_volume_xml(params.pool, params.vol)

            result['disk'] = params.vol
            result["pool"] = params.pool
            print dumps({"result": {"code": 0, "msg": "create disk "+params.vol+" successful."}, "data": result})
        elif params.type == "uus":
            kv = {"poolname": params.pool, "name": params.vol, "size": params.capacity}
            op1 = Operation("cstor-cli vdisk-create", kv, with_result=True)
            diskinfo = op1.execute()
            if diskinfo["result"]["code"] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {"poolname": params.pool, "name": params.vol, "uni": diskinfo["data"]["uni"]}
            op2 = Operation("cstor-cli vdisk-prepare", kv, with_result=True)
            prepareInfo = op2.execute()
            # delete the disk
            if prepareInfo["result"]["code"] != 0:
                kv = {"poolname": params.pool, "name": params.vol}
                op3 = Operation("cstor-cli vdisk-remove", kv, with_result=True)
                rmDiskInfo = op3.execute()
                if rmDiskInfo["result"]["code"] == 0:
                    print dumps({"result": {"code": 1, "msg": "error: create disk success but can not prepare disk" + params.vol + "."}, "data": {}})
                else:
                    print dumps({"result": {"code": 1, "msg": "error: can not prepare disk and roll back fail(can not delete the disk)" + params.vol + ". "}, "data": {}})
                exit(1)
            else:
                result = {
                    "disk": params.vol,
                    "pool": params.pool,
                    "virtual_size": params.capacity,
                    "filename": prepareInfo["data"]["path"],
                    "uni": diskinfo["data"]["uni"],
                    "uuid": randomUUID(),
                }
                print dumps({"result": {"code": 0,
                                        "msg": "create disk "+params.pool+" success."}, "data": result})
    except ExecuteException, e:
        logger.debug("createDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 400, "msg": "error occur while create disk " + params.vol + ". "+e.message}, "data": {}})
        exit(1)
    except Exception:
        logger.debug("createDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 300, "msg": "error occur while create disk " + params.vol}, "data": {}})
        exit(1)

def deleteDisk(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-remove ', {'poolname': uuid, 'name': params.vol}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-remove ', {'poolname': params.pool, 'name': params.vol},
                               with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])
            pool_info = get_pool_info(params.pool)
            disk_dir = pool_info['path'] + '/' + params.vol
            snapshots_path = disk_dir + '/snapshots'
            with open(disk_dir + '/config.json', "r") as f:
                config = load(f)
            if os.path.exists(snapshots_path):
                for file in os.listdir(snapshots_path):
                    if snapshots_path + '/' + file == config['current']:
                        continue
                    else:
                        try:
                            # if success, disk has right snapshot, raise ExecuteException
                            chain = get_sn_chain_path(snapshots_path + '/' + file)
                        except:
                            continue
                        raise ExecuteException('', 'error: disk %s still has snapshot %s.' % (params.vol, file))

            op = Operation("rm -rf " + disk_dir, {})
            op.execute()
            print dumps({"result": {"code": 0, "msg": "delete volume " + params.vol + " success."}, "data": {}})
        elif params.type == "uus":
            kv = {"poolname": params.pool, "name": params.vol}
            op1 = Operation("cstor-cli vdisk-show", kv, with_result=True)
            diskinfo = op1.execute()
            if diskinfo["result"]["code"] != 0:
                print dumps(diskinfo)
                exit(1)

            kv = {"poolname": params.pool, "name": params.vol, "uni": diskinfo["data"]["uni"]}
            op = Operation("cstor-cli vdisk-release", kv, True)
            releaseInfo = op.execute()
            if releaseInfo["result"]["code"] != 0:
                print dumps(releaseInfo)
                exit(1)

            kv = {"poolname": params.pool, "name": params.vol}
            op = Operation("cstor-cli vdisk-remove", kv, with_result=True)
            result = op.execute()
            print dumps(result)
    except ExecuteException, e:
        logger.debug("deleteDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while delete disk " + params.vol + ". "+e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("deleteDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while delete disk "}, "data": {}}
        exit(1)

def resizeDisk(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-expand ', {'poolname': uuid, 'name': params.vol,
                                                       'size': params.capacity}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-expand ', {'poolname': params.pool, 'name': params.vol,
                                                           'size': params.capacity}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            pool_info = get_pool_info(params.pool)
            disk_dir = pool_info['path'] + '/' + params.vol
            with open(disk_dir + '/config.json', "r") as f:
                config = load(f)

            disk_info = get_disk_info(config['current'])
            size = int(params.capacity) - int(disk_info['virtual_size'])
            op = Operation("qemu-img resize " + config['current'] + " +" + str(size), {})
            op.execute()

            with open(disk_dir + '/config.json', "w") as f:
                dump(config, f)
            result = get_disk_info(config['current'])

            result['disk'] = params.vol
            result["pool"] = params.pool
            print dumps({"result": {"code": 0, "msg": "resize disk " + params.vol + " successful."}, "data": result})

        elif params.type == "uus":
            raise ExecuteException("", "not support operation for uus and vdiskfs.")

    except ExecuteException, e:
        logger.debug("resizeDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while resize disk " + params.vol + ". "+e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("resizeDisk " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while resize disk " + params.vol}, "data": {}}
        exit(1)

def cloneDisk(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-clone ', {'poolname': uuid, 'name': params.vol,
                                                       'clonename': params.newname}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-clone ', {'poolname': params.pool, 'name': params.vol,
                                                          'clonename': params.newname}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            pool_info = get_pool_info(params.pool)
            # create disk dir and create disk in dir.
            disk_dir = pool_info['path'] + '/' + params.vol
            clone_disk_dir = pool_info['path'] + '/' + params.newname
            clone_disk_path = clone_disk_dir + '/' + params.newname
            if not os.path.exists(clone_disk_dir):
                os.makedirs(clone_disk_dir)
            if os.path.exists(clone_disk_path):
                raise ExecuteException('', 'disk already exists, aborting clone.')

            with open(disk_dir + '/config.json', "r") as f:
                config = load(f)

            try:
                op1 = Operation('cp -f %s %s' % (config['current'], clone_disk_path), {})
                op1.execute()
            except:
                if os.path.exists(clone_disk_dir):
                    op3 = Operation('rm -rf %s' % clone_disk_dir, {})
                    op3.execute()
                raise ExecuteException('', 'Copy %s to %s failed!, aborting clone.' % (config['current'], clone_disk_path))
            try:
                backing_file = DiskImageHelper.get_backing_file(clone_disk_path)
                if backing_file:
                    op2 = Operation('qemu-img rebase -f %s -b "" %s' % (params.format, clone_disk_path), {})
                    op2.execute()
            except:
                if os.path.exists(clone_disk_dir):
                    op3 = Operation('rm -rf %s' % clone_disk_dir, {})
                    op3.execute()
                raise ExecuteException('', 'Execute "qemu-img rebase %s" failed!, aborting clone.' % clone_disk_path )

            config = {}
            config['name'] = params.newname
            config['dir'] = clone_disk_dir
            config['current'] = clone_disk_path

            with open(clone_disk_dir + '/config.json', "w") as f:
                dump(config, f)

            result = get_disk_info(clone_disk_path)
            # vol_xml = get_volume_xml(params.pool, params.vol)

            result['disk'] = params.newname
            result["pool"] = params.pool
            print dumps({"result": {"code": 0, "msg": "clone disk " + params.vol + " successful."}, "data": result})
        elif params.type == "uus":
            raise ExecuteException("", "not support operation for uus and vdiskfs.")
    except ExecuteException, e:
        logger.debug("deletePool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while clone disk " + params.vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("deletePool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while clone disk " + params.vol}, "data": {}}
        exit(1)

def showDisk(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-show ', {'poolname': uuid, 'name': params.vol}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-show ', {'poolname': params.pool, 'name': params.vol}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])
            pool_info = get_pool_info(params.pool)
            disk_dir = pool_info['path'] + '/' + params.vol
            with open(disk_dir + '/config.json', "r") as f:
                config = load(f)

            result = get_disk_info(config['current'])
            result['disk'] = params.vol
            result["pool"] = params.pool
            result["current"] = config["current"]
            print dumps(
                {"result": {"code": 0, "msg": "show disk " + params.vol + " successful."}, "data": result})
        elif params.type == "uus":
            kv = {"poolname": params.pool, "name": params.vol}
            op = Operation("cstor-cli vdisk-show", kv, True)
            diskinfo = op.execute()

            result = {
                "disk": params.vol,
                "pool": params.pool,
                "virtual_size": params.capacity,
                "filename": diskinfo["data"]["path"],
                "uni": diskinfo["data"]["uni"],
                "uuid": randomUUID(),
                "current": diskinfo["data"]["path"]
            }

            print dumps({"result": {"code": 0,
                                    "msg": "show disk " + params.pool + " success."}, "data": result})
    except ExecuteException, e:
        logger.debug("showDisk " + params.vol)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while show disk " + params.vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("showDisk " + params.vol)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while show disk " + params.vol}, "data": {}}
        exit(1)

def showDiskSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            # op = Operation('cstor-cli vdisk-show ', {'poolname': params.pool, 'name': params.vol,
            #                                          'size': params.capacity}, with_result=True)
            # cstor = op.execute()
            # if cstor['result']['code'] != 0:
            #     raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            disk_config = get_disk_config(params.pool, params.vol)
            snapshot_path = disk_config['dir'] + '/snapshots/' + params.name

            result = get_disk_info(snapshot_path)
            result['disk'] = params.vol
            result["pool"] = params.pool
            print dumps(
                {"result": {"code": 0, "msg": "show disk snapshot " + params.name + " successful."}, "data": result})
        elif params.type == "uus":
            raise ExecuteException("", "not support operation for uus and vdiskfs.")
    except ExecuteException, e:
        logger.debug("showDiskSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while show disk snapshot " + params.name + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("showDiskSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while show disk snapshot " + params.name}, "data": {}}
        exit(1)

def createSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs":
            op = Operation("virsh vol-create-as ", {'pool': params.pool, 'name': params.snapshot, 'capacity': params.capacity,
                                                    'format': params.format, 'backing-vol': params.backing_vol,
                                                    'backing-vol-format': params.backing_vol_format})
            op.execute()
            # get snapshot info
            vol_xml = get_volume_xml(params.pool, params.snapshot)
            result = loads(xmlToJson(vol_xml))['volume']
            print dumps(
                {"result": {"code": 0, "msg": "create snapshot " + params.snapshot + " successful."}, "data": result})

        elif params.type == "uus":
            if params.vmname is None:
                op = Operation("cstor-cli vdisk-add-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
            else:
                op = Operation("cstor-cli vdisk-add-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot, 'vmname': params.vmname}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
    except ExecuteException, e:
        logger.debug("createSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while create Snapshot " + params.snapshot +" on "+ params.backing_vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("createSnapshot " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while create Snapshot " + params.snapshot +" on "+ params.backing_vol}, "data": {}}
        exit(1)

def deleteSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs":
            op = Operation("virsh vol-delete ", {'pool': params.pool, 'vol': params.snapshot})
            op.execute()
            print dumps({"result": {"code": 0, "msg": "delete snapshot " + params.snapshot + " success."}, "data": {}})
        elif params.type == "uus":
            if params.vmname is None:
                op = Operation("cstor-cli vdisk-rm-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
            else:
                op = Operation("cstor-cli vdisk-rm-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot,
                                'vmname': params.vmname}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
    except ExecuteException, e:
        logger.debug("deleteSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while delete Snapshot " + params.snapshot + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("deletePool " + params.pool)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while delete Snapshot " + params.snapshot}, "data": {}}
        exit(1)

def revertSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs":
            print dumps({"result": {"code": 1, "msg": "not support operation."}, "data": {}})
        elif params.type == "uus":
            if params.vmname is None:
                op = Operation("cstor-cli vdisk-rr-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
            else:
                op = Operation("cstor-cli vdisk-rr-ss",
                               {"poolname": params.pool, "name": params.backing_vol, "sname": params.snapshot,
                                'vmname': params.vmname}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
    except ExecuteException, e:
        logger.debug("revertSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while revert Snapshot " + params.backing_vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("revertSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while revert Snapshot " + params.backing_vol}, "data": {}}
        exit(1)

def showSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs":
            vol_xml = get_volume_xml(params.pool, params.snapshot)
            result = loads(xmlToJson(vol_xml))['volume']
            print dumps(
                {"result": {"code": 0, "msg": "get snapshot info " + params.snapshot + " successful."}, "data": result})
        elif params.type == "uus":
            if params.vmname is None:
                op = Operation("cstor-cli vdisk-show-ss",
                               {"pool": params.pool, "name": params.backing_vol, "sname": params.snapshot}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
            else:
                op = Operation("cstor-cli vdisk-show-ss",
                               {"pool": params.pool, "name": params.backing_vol, "sname": params.snapshot, 'vmname': params.vmname}, True)
                ssInfo = op.execute()
                print dumps(ssInfo)
    except ExecuteException, e:
        logger.debug("showSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while show Snapshot " + params.backing_vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("showSnapshot " + params.snapshot)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while show Snapshot " + params.backing_vol}, "data": {}}
        exit(1)

def createExternalSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-add-ss ', {'poolname': uuid, 'name': params.vol,
                                                                'sname': params.name}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-add-ss ', {'poolname': params.pool, 'name': params.vol,
                                                           'sname': params.name}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])
            if params.domain is None:
                disk_config = get_disk_config(params.pool, params.vol)
                if check_disk_in_use(disk_config['current']):
                    raise ExecuteException('', 'disk in using, current file %s is using by another process, '
                                               'is there a vm using the current file, plz check.' % disk_config['current'])
                ss_dir = disk_config['dir'] + '/snapshots'
                if not os.path.exists(ss_dir):
                    os.makedirs(ss_dir)
                ss_path = ss_dir + '/' + params.name

                op1 = Operation('qemu-img create -f %s -b %s -F %s %s' %
                                (params.format, disk_config['current'], params.format, ss_path), {})
                op1.execute()

                with open(disk_config['dir'] + '/config.json', "r") as f:
                    config = load(f)
                    config['current'] = ss_path
                with open(disk_config['dir'] + '/config.json', "w") as f:
                    dump(config, f)

                result = get_disk_info(ss_path)
                result['disk'] = config['name']
                result["pool"] = params.pool
                # result["current"] = DiskImageHelper.get_backing_file(ss_path)
                print dumps(
                    {"result": {"code": 0, "msg": "create disk external snapshot " + params.name + " successful."},
                     "data": result})
            else:
                specs = get_disks_spec(params.domain)
                disk_config = get_disk_config(params.pool, params.vol)
                if disk_config['current'] not in specs.keys():
                    logger.debug('disk %s current is %s.' % (params.vol, disk_config['current']))
                    raise ExecuteException('', 'domain %s not has disk %s' % (params.domain, params.vol))

                vm_disk = specs[disk_config['current']]
                ss_path = disk_config['dir'] + '/snapshots/' + params.name
                ss_dir = disk_config['dir'] + '/snapshots'
                if not os.path.exists(ss_dir):
                    os.makedirs(ss_dir)
                not_need_snapshot_spec = ''
                for disk_path in specs.keys():
                    if disk_path == disk_config['current']:
                        continue
                    not_need_snapshot_spec = not_need_snapshot_spec + '--diskspec %s,snapshot=no ' % specs[disk_path]
                    # '/var/lib/libvirt/pooltest3/wyw123/snapshots/wyw123.6'
                    # 'vdb,snapshot=no'

                op = Operation('virsh snapshot-create-as --domain %s --name %s --atomic --disk-only --no-metadata '
                               '--diskspec %s,snapshot=external,file=%s,driver=%s %s' %
                               (params.domain, params.name, vm_disk, ss_path, params.format, not_need_snapshot_spec),
                               {})
                op.execute()
                config_path = os.path.dirname(ss_dir) + '/config.json'
                with open(config_path, "r") as f:
                    config = load(f)
                    config['current'] = ss_path
                with open(config_path, "w") as f:
                    dump(config, f)
                result = get_disk_info(ss_path)
                result['disk'] = config['name']
                result["pool"] = params.pool
                print dumps(
                    {"result": {"code": 0, "msg": "create disk external snapshot " + params.name + " successful."},
                     "data": result})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
    except ExecuteException, e:
        logger.debug("createExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while createExternalSnapshot " + params.name +" on "+ params.vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("createExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while createExternalSnapshot " + params.name +" on "+ params.vol}, "data": {}}
        exit(1)

# create snapshot on params.name, then rename snapshot to current
def revertExternalSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.domain and is_vm_active(params.domain):
                raise ExecuteException('', 'domain %s is still active, plz stop it first.')

            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-rr-ss ', {'poolname': uuid, 'name': params.vol,
                                                           'sname': params.name}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-rr-ss ', {'poolname': params.pool, 'name': params.vol,
                                                          'sname': params.name}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])


            disk_config = get_disk_config(params.pool, params.vol)
            if check_disk_in_use(disk_config['current']):
                raise ExecuteException('', 'error: current disk in use, plz check or set real domain field.')

            ss_path = disk_config['dir'] + '/snapshots/' + params.name
            if ss_path is None:
                raise ExecuteException('', 'error: can not get snapshot backing file.')

            uuid = randomUUID().replace('-', '')
            new_file_path = os.path.dirname(params.backing_file)+'/'+uuid
            op1 = Operation('qemu-img create -f %s -b %s -F %s %s' %
                            (params.format, params.backing_file, params.format, new_file_path), {})
            op1.execute()
            # change vm disk
            if params.domain and not change_vm_os_disk_file(params.domain, disk_config['current'], new_file_path):
                op2 = Operation('rm -f %s' % new_file_path, {})
                op2.execute()
                raise ExecuteException('', 'can not change disk source in domain xml')

            # modify json file, make os_event_handler to modify data on api server .
            with open(disk_config['dir'] + '/config.json', "r") as f:
                config = load(f)
                config['current'] = new_file_path
            with open(disk_config['dir'] + '/config.json', "w") as f:
                dump(config, f)

            result = get_disk_info(config['current'])
            result['disk'] = config['name']
            result["pool"] = params.pool

            print dumps({"result": {"code": 0, "msg": "revert disk external snapshot " + params.name + " successful."}, "data": result})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs."}, "data": {}})
    except ExecuteException, e:
        logger.debug("revertExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while revertExternalSnapshot " + params.name +" on "+ params.vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("revertExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while revertExternalSnapshot " + params.name +" on "+ params.vol}, "data": {}}
        exit(1)

def deleteExternalSnapshot(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
            if params.type == "nfs" or params.type == "glusterfs":
                uuid = get_cstor_real_poolname(params.pool)
                op = Operation('cstor-cli vdisk-rm-ss ', {'poolname': uuid, 'name': params.vol,
                                                          'sname': params.name}, with_result=True)
            else:
                op = Operation('cstor-cli vdisk-rm-ss ', {'poolname': params.pool, 'name': params.vol,
                                                          'sname': params.name}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: ' + cstor['result']['msg'])

            if params.domain:
                specs = get_disks_spec(params.domain)
                disk_config = get_disk_config(params.pool, params.vol)
                if disk_config['current'] not in specs.keys():
                    raise ExecuteException('', 'domain %s not has disk %s' % (params.domain, params.vol))

            disk_config = get_disk_config(params.pool, params.vol)

            # get all snapshot to delete(if the snapshot backing file chain contains params.backing_file), except current.
            snapshots_to_delete = []
            files = os.listdir(disk_config['dir'] + '/snapshots')
            for df in files:
                try:
                    bf_paths = get_sn_chain_path(disk_config['dir'] + '/snapshots/' + df)
                    if params.backing_file in bf_paths:
                        snapshots_to_delete.append(df)
                except:
                    continue

            # if snapshot to delete is current, delelte vmsn from server.
            if params.name not in snapshots_to_delete:
                snapshots_to_delete.append(params.name)

            if params.domain:
                current_backing_file = DiskImageHelper.get_backing_file(disk_config['current'])
                # reconnect the snapshot chain
                bf_bf_path = DiskImageHelper.get_backing_file(params.backing_file)
                if bf_bf_path:
                    op = Operation('virsh blockpull --domain %s --path %s --base %s --wait' %
                                   (params.domain, disk_config['current'], params.backing_file), {})
                    op.execute()
                else:
                    op = Operation('virsh blockpull --domain %s --path %s --wait' %
                                   (params.domain, disk_config['current']), {})
                    op.execute()
                    op = Operation('rm -f %s' % params.backing_file, {})
                    op.execute()

                # # if the snapshot to delete is not current, delete snapshot's backing file
                # if current_backing_file != params.backing_file:
                #     op = Operation('rm -f %s' % params.backing_file, {})
                #     op.execute()

            else:
                current_backing_file = DiskImageHelper.get_backing_file(disk_config['current'])
                # reconnect the snapshot chain
                paths = get_sn_chain_path(disk_config['current'])
                if params.backing_file in paths:
                    bf_bf_path = DiskImageHelper.get_backing_file(params.backing_file)
                    if bf_bf_path:
                        # effect current and backing file is not head, rabse current to reconnect
                        op = Operation('qemu-img rebase -b %s %s' % (bf_bf_path, disk_config['current']), {})
                        op.execute()
                    else:
                        # effect current and backing file is head, rabse current to itself
                        op = Operation('qemu-img rebase -b "" %s' % disk_config['current'], {})
                        op.execute()
                        op = Operation('rm -f %s' % params.backing_file, {})
                        op.execute()
                # # if the snapshot to delete is not current, delete snapshot's backing file
                # if current_backing_file != params.backing_file:
                #     op = Operation('rm -f %s' % params.backing_file, {})
                #     op.execute()

            for df in snapshots_to_delete:
                if df != os.path.basename(disk_config['current']):
                    op = Operation('rm -f %s/snapshots/%s' % (disk_config['dir'], df), {})
                    op.execute()
            # modify json file, make os_event_handler to modify data on api server .
            with open(disk_config['dir'] + '/config.json', "r") as f:
                config = load(f)
                config['current'] = config['current']
            with open(disk_config['dir'] + '/config.json', "w") as f:
                dump(config, f)

            result = {'delete_ss': snapshots_to_delete, 'disk': disk_config['name'],
                      'need_to_modify': config['current'], "pool": params.pool}
            print dumps({"result": {"code": 0, "msg": "delete disk external snapshot " + params.name + " successful."}, "data": result})


        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs."}, "data": {}})
    except ExecuteException, e:
        logger.debug("deleteExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while deleteExternalSnapshot " + params.name +" on "+ params.vol + ". " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("deleteExternalSnapshot " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while deleteExternalSnapshot " + params.name +" on "+ params.vol}, "data": {}}
        exit(1)

def updateDiskCurrent(params):
    try:
        if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs":
            for current in params.current:
                if params.current.find("snapshots") > 0:
                    config_path = os.path.dirname(os.path.dirname(current)) + '/config.json'
                else:
                    config_path = os.path.dirname(current) + '/config.json'
                with open(config_path, "r") as f:
                    config = load(f)
                    config['current'] = current
                with open(config_path, "w") as f:
                    dump(config, f)
                print dumps({"result": {"code": 0, "msg": "updateDiskCurrent successful."}, "data": {}})
        elif params.type == "uus":
            print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
    except ExecuteException, e:
        logger.debug("updateDiskCurrent")
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while updateDiskCurrent. " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("updateDiskCurrent " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while updateDiskCurrent."}, "data": {}}
        exit(1)

def customize(params):
    try:
        op = Operation('virt-customize --add %s --password %s:password:%s' % (params.add, params.user, params.password), {})
        op.execute()
        print {"result": {"code": 0, "msg": "customize  successful."}, "data": {}}
    except ExecuteException, e:
        logger.debug("customize")
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while customize. " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while customize."}, "data": {}}
        exit(1)

def createDiskFromImage(params):
    try:
        pool_info = get_pool_info(params.targetPool)
        dest_dir = '%s/%s' % (pool_info['path'], params.name)
        dest = '%s/%s' % (dest_dir, params.name)
        dest_config_file = '%s/config.json' % (dest_dir)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir, 0711)
        if os.path.exists(dest_config_file):
            raise Exception('Path %s already in use, aborting copy.' % dest_dir)

        if params.full_copy:
            try:
                op = Operation('cp -f %s %s' % (params.source, dest), {})
                op.execute()
            except:
                if os.path.exists(dest_dir):
                    op = Operation('rm -rf %s' % dest_dir, {})
                    op.execute()
                raise Exception('Copy %s to %s failed!' % (params.source, dest))

            try:
                op = Operation('qemu-img rebase -f qcow2 %s -b "" -u' % (dest), {})
                op.execute()
            except:
                if os.path.exists(dest_dir):
                    op = Operation('rm -rf %s' % dest_dir, {})
                    op.execute()
                raise Exception('Execute "qemu-img rebase -f qcow2 %s" failed!' % (dest))

        else:
            if params.source.find('snapshots') >= 0:
                source_disk_dir = os.path.dirname(os.path.dirname(params.source))
            else:
                source_disk_dir = os.path.dirname(params.source)
            config = get_disk_config_by_path('%s/config.json' % source_disk_dir)
            disk_info = get_disk_info(config['current'])
            op = Operation(
                'qemu-img create -f %s -b %s -F %s %s' %
                            (disk_info['format'], config['current'], disk_info['format'], dest), {})
            op.execute()
        config = {}
        config['name'] = params.name
        config['dir'] = dest_dir
        config['current'] = dest

        with open(dest_dir + '/config.json', "w") as f:
            dump(config, f)
        result = get_disk_info(dest)
        result['disk'] = config['name']
        result["pool"] = params.targetPool
        print dumps(
            {"result": {"code": 0, "msg": "createDiskFromImage " + params.name + " successful."},
             "data": result})
    except ExecuteException, e:
        logger.debug("createDiskFromImage")
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while createDiskFromImage. " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("createDiskFromImage " + params.name)
        logger.debug(params.type)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while createDiskFromImage."}, "data": {}}
        exit(1)

def migrate(params):
    try:
        if not is_vm_disk_driver_cache_none(params.domain):
            raise ExecuteException('', 'error: disk driver cache is not none')
        if not is_vm_disk_not_shared_storage(params.domain):
            raise ExecuteException('', 'error: still has disk not create in shared storage.')

        op = Operation('virsh migrate --live --undefinesource --persistent %s qemu+ssh://%s/system tcp://%s' % (params.domain, params.ip, params.ip), {})
        op.execute()
        print {"result": {"code": 0, "msg": "migrate vm %s successful." % params.domain}, "data": {}}
    except ExecuteException, e:
        logger.debug("migrate")
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 400, "msg": "error occur while migrate. " + e.message}, "data": {}}
        exit(1)
    except Exception:
        logger.debug("migrate " + params.domain)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print {"result": {"code": 300, "msg": "error occur while migrate."}, "data": {}}
        exit(1)

def xmlToJson(xmlStr):
    json = dumps(bf.data(fromstring(xmlStr)), sort_keys=True, indent=4)
    return json.replace("@", "_").replace("$", "text").replace(
        "interface", "_interface").replace("transient", "_transient").replace(
        "nested-hv", "nested_hv").replace("suspend-to-mem", "suspend_to_mem").replace("suspend-to-disk",
                                                                                      "suspend_to_disk")

def get_cstor_real_poolname(pool):
    pool_info = get_pool_info(pool)
    return os.path.basename(pool_info['path'])
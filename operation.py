from xml.etree.ElementTree import fromstring
from xmljson import badgerfish as bf

from netutils import get_host_IP
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
            cmd = "%s --%s %s " % (cmd, key, self.params[key])
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
    #  {"result":{"code":0, "msg":"success"}, "data":{"status": "active", "mountpath": "/Disk240", "proto": "localfs", "url": "/dev/sdb1", "poolname": "pool1", "free": 223363817472, "disktype": "file", "maintain": "normal", "used": 768970752, "total": 236152303616}, "obj":"pooladd"}
    kv = {"type": params.type, "poolname": params.uuid, "url": params.url, "opt": params.opt, "uuid": params.pool}
    op = Operation("cstor-cli pool-add", kv, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type == 'uus':
        result = {
                    "pooltype": params.type,
                    "pool": params.pool,
                    "poolname": params.uuid,
                    "capacity": cstor["data"]["total"],
                    "autostart": "no",
                    "path": cstor["data"]["url"],
                    "state": cstor["data"]["status"],
                    "uuid": randomUUID(),
                    "content": 'vmd'
                }
    else:
        if not os.path.isdir(cstor['data']['mountpath']):
            raise ExecuteException('', 'cant not get cstor mount path')
        POOL_PATH = "%s/%s" % (cstor['data']['mountpath'], params.uuid)
        if not os.path.isdir(POOL_PATH):
            os.makedirs(POOL_PATH)
        # step1 define pool
        op1 = Operation("virsh pool-define-as", {"name": params.uuid, "type": "dir", "target": POOL_PATH})
        op1.execute()

        try:
            # step2 autostart pool
            if params.autostart:
                op2 = Operation("virsh pool-autostart", {"pool": params.uuid})
                op2.execute()
            op3 = Operation("virsh pool-start", {"pool": params.uuid})
            op3.execute()
        except ExecuteException, e:
            op = Operation("cstor-cli pool-remove", {"poolname": params.uuid}, with_result=True)
            cstor = op.execute()
            if cstor['result']['code'] != 0:
                raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
                    cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
            op_cancel = Operation("virsh pool-undefine", {"--pool": params.uuid})
            op_cancel.execute()
            raise e

        with open('%s/content' % POOL_PATH, 'w') as f:
            f.write(params.content)

        result = get_pool_info(params.uuid)
        result['content'] = params.content
        result["pooltype"] = params.type
        result["pool"] = params.pool
        result["poolname"] = params.uuid
        result["state"] = "active"

    success_print("create pool %s successful." % params.pool, result)

def deletePool(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    if params.type != "uus":
        if is_pool_started(poolname):
            raise ExecuteException('RunCmdError', 'pool %s still active, plz stop it first.' % poolname)

        if is_pool_defined(poolname):
            op2 = Operation("virsh pool-undefine", {"pool": poolname})
            op2.execute()

    op = Operation("cstor-cli pool-remove", {"poolname": poolname}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    helper = K8sHelper("VirtualMahcinePool")
    helper.delete(params.pool)
    success_print("delete pool %s successful." % params.pool, {})

def startPool(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    if params.type != "uus":
        op1 = Operation("virsh pool-start", {"pool": poolname})
        op1.execute()
        pool_info["state"] = "active"
        success_print("start pool %s successful." % params.pool, pool_info)
    else:
        error_print(500, "not support operation for uus")

def autoStartPool(params):
    if params.type != "uus":
        pool_info = get_pool_info_from_k8s(params.pool)
        poolname = pool_info['poolname']
        if params.disable:
            op = Operation("virsh pool-autostart --disable", {"pool": poolname})
            op.execute()
            pool_info["autostart"] = 'no'
        else:
            op = Operation("virsh pool-autostart", {"pool": poolname})
            op.execute()
            pool_info["autostart"] = 'yes'
        success_print("autoStart pool %s successful." % params.pool, pool_info)
    else:
        error_print(500, "not support operation for uus")


def stopPool(params):
    if params.type != "uus":
        pool_info = get_pool_info_from_k8s(params.pool)
        poolname = pool_info['poolname']
        op1 = Operation("virsh pool-destroy", {"pool": poolname})
        op1.execute()

        pool_info["state"] = "inactive"
        success_print("stop pool %s successful." % poolname, pool_info)
    elif params.type == "uus":
        error_print(500, "not support operation for uus")

def showPool(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    if params.type != 'uus':
        result = get_pool_info(poolname)
        if is_pool_started(poolname):
            result['state'] = "active"
        else:
            result['state'] = "inactive"
        result['content'] = pool_info["content"]
        result["pooltype"] = pool_info["pooltype"]
        result["pool"] = params.pool
        result["poolname"] = pool_info["poolname"]
    else:
        cstor = get_cstor_pool_info(poolname)
        result = {
            "pooltype": params.type,
            "pool": params.pool,
            "poolname": poolname,
            "capacity": cstor["data"]["total"],
            "autostart": "no",
            "path": cstor["data"]["url"],
            "state": cstor["data"]["status"],
            "uuid": randomUUID(),
            "content": 'vmd'
        }
    # update pool
    if cmp(pool_info, result) != 0:
        k8s = K8sHelper('VirtualMahcinePool')
        k8s.update(pool_info['pool'], 'pool', result)

    success_print("show pool %s successful." % poolname, result)

def cstor_prepare_disk(type, pool, vol, uni):
    kv = {"poolname": pool, "name": vol, "uni": uni}
    op = Operation("cstor-cli vdisk-prepare", kv, with_result=True)

    prepareInfo = op.execute()
    # delete the disk
    if prepareInfo["result"]["code"] != 0:
        kv = {"poolname": pool, "name": vol}
        op3 = Operation("cstor-cli vdisk-remove", kv, with_result=True)
        rmDiskInfo = op3.execute()
        if rmDiskInfo["result"]["code"] != 0:
            raise ExecuteException(rmDiskInfo["result"]["code"],
                                   'cstor raise exception while prepare disk, cstor error code: %d, msg: %s, obj: %s and try delete disk fail, cstor error code: %d, msg: %s, obj: %s' % (
                                   prepareInfo['result']['code'], prepareInfo['result']['msg'], prepareInfo['obj'],
                                   rmDiskInfo["result"]["code"], rmDiskInfo["result"]["msg"], rmDiskInfo['obj']))
        if type != "uus":
            disk_dir = get_disk_dir(pool, vol)
            op = Operation('rm -rf %s' % disk_dir, {})
            op.execute()
        raise ExecuteException(prepareInfo["result"]["code"],
                               'cstor raise exception while prepare disk, cstor error code: %d, msg: %s, obj: %s' % (
                               prepareInfo['result']['code'], prepareInfo['result']['msg'], prepareInfo['obj']))

    return prepareInfo
def cstor_create_disk(pool, vol, capacity):
    op = Operation('cstor-cli vdisk-create ', {'poolname': pool, 'name': vol,
                                               'size': capacity}, with_result=True)
    createInfo = op.execute()
    if createInfo['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s' % (
            createInfo['result']['code'], createInfo['result']['msg']))

    return createInfo

def get_disk_dir(pool, vol):
    pool_info = get_pool_info(pool)
    if not os.path.isdir(pool_info['path']):
        raise ExecuteException('', 'can not get virsh pool path.')
    # create disk dir and create disk in dir.
    disk_dir = "%s/%s" % (pool_info['path'], vol)

def qemu_create_disk(pool, poolname, vol, format, capacity):
    pool_info = get_pool_info(poolname)
    if not os.path.isdir(pool_info['path']):
        raise ExecuteException('', 'can not get virsh pool path.')
    # create disk dir and create disk in dir.
    disk_dir = "%s/%s" % (pool_info['path'], vol)
    if os.path.isdir(disk_dir):
        raise ExecuteException('', 'error: disk dir has exist.')
    os.makedirs(disk_dir)
    disk_path = "%s/%s" % (disk_dir, vol)
    op = Operation('qemu-img create -f %s %s %s' % (format, disk_path, capacity), {})
    op.execute()

    config = {}
    config['name'] = vol
    config['dir'] = disk_dir
    config['current'] = disk_path
    config['pool'] = pool
    config['poolname'] = poolname

    with open('%s/config.json' % disk_dir, "w") as f:
        dump(config, f)
    result = get_disk_info(disk_path)
    result['disk'] = vol
    result["uni"] = disk_path
    result['current'] = disk_path
    result['pool'] = pool
    result['poolname'] = poolname
    return result


def createDisk(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    createInfo = cstor_create_disk(poolname, params.vol, params.capacity)

    if params.type != 'uus':
        result = qemu_create_disk(params.pool, poolname, params.vol, params.format, params.capacity)
        uni = result["uni"]
        prepareInfo = cstor_prepare_disk(params.type, poolname, params.vol, uni)
    else:
        uni = createInfo["data"]["uni"]
        prepareInfo = cstor_prepare_disk(params.type, poolname, params.vol, uni)
        result = {
            "disk": params.vol,
            "pool": params.pool,
            "poolname": pool_info['poolname'],
            "uni": createInfo["data"]["uni"],
            "current": prepareInfo["data"]["path"],
            "virtual_size": params.capacity,
            "filename": prepareInfo["data"]["path"]
        }
    success_print("create disk %s successful." % params.vol, result)

def deleteDisk(params):
    disk_info = get_vol_info_from_k8s(params.vol)
    poolname = disk_info['poolname']
    kv = {"poolname": disk_info['poolname'], "name": params.vol, "uni": disk_info["uni"]}
    op = Operation("cstor-cli vdisk-release", kv, True)
    releaseInfo = op.execute()
    if releaseInfo['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            releaseInfo['result']['code'], releaseInfo['result']['msg'], releaseInfo['obj']))

    op = Operation('cstor-cli vdisk-remove ', {'poolname': disk_info['poolname'], 'name': params.vol},
                   with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        pool_info = get_pool_info(poolname)
        disk_dir = '%s/%s' %(pool_info['path'], params.vol)
        snapshots_path = '%s/snapshots' % disk_dir
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)
        if os.path.exists(snapshots_path):
            for file in os.listdir(snapshots_path):
                if '%s/%s' %(snapshots_path, file) == config['current']:
                    continue
                else:
                    try:
                        # if success, disk has right snapshot, raise ExecuteException
                        chain = get_sn_chain_path('%s/%s' %(snapshots_path, file))
                    except:
                        continue
                    raise ExecuteException('', 'error: disk %s still has snapshot %s.' % (params.vol, file))

        op = Operation("rm -rf %s" % disk_dir, {})
        op.execute()

    helper = K8sHelper("VirtualMachineDisk")
    helper.delete(params.vol)
    success_print("delete volume %s success." % params.vol, {})

def resizeDisk(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    disk_info = get_vol_info_from_k8s(params.vol)
    prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.vol, disk_info['uni'])
    op = Operation('cstor-cli vdisk-expand ', {'poolname': disk_info['poolname'], 'name': params.vol,
                                               'size': params.capacity}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        disk_dir = '%s/%s' % (pool_info['path'], params.vol)
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)

        disk_info = get_disk_info(config['current'])
        size = int(params.capacity) - int(disk_info['virtual_size'])
        op = Operation("qemu-img resize %s +%s" % (config['current'], str(size)), {})
        op.execute()

        with open('%s/config.json' % disk_dir, "w") as f:
            dump(config, f)
        result = get_disk_info(config['current'])

        result['disk'] = params.vol
        result["pool"] = params.pool
        result["poolname"] = pool_info['poolname']
        result["uni"] = config['current']
        result["current"] = config['current']
    else:
        result = {
            "disk": params.vol,
            "pool": params.pool,
            "poolname": pool_info['poolname'],
            "uni": cstor["data"]["uni"],
            "current": prepareInfo["data"]["path"],
            "virtual_size": params.capacity,
            "filename": prepareInfo["data"]["path"]
        }
    success_print("success resize disk %s." % params.vol, result)

def cloneDisk(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    disk_info = get_vol_info_from_k8s(params.vol)
    prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.vol, disk_info['uni'])
    op = Operation('cstor-cli vdisk-clone ', {'poolname': poolname, 'name': params.vol,
                                              'clonename': params.newname}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        # create disk dir and create disk in dir.
        disk_dir = '%s/%s' % (pool_info['path'], params.vol)
        clone_disk_dir = '%s/%s' %(pool_info['path'], params.newname)
        clone_disk_path = '%s/%s' % (clone_disk_dir, params.newname)
        if not os.path.exists(clone_disk_dir):
            os.makedirs(clone_disk_dir)
        if os.path.exists(clone_disk_path):
            raise ExecuteException('', 'disk already exists, aborting clone.')

        with open('%s/config.json' % disk_dir, "r") as f:
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
            raise ExecuteException('', 'Execute "qemu-img rebase %s" failed!, aborting clone.' % clone_disk_path)

        prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.newname, clone_disk_path)
        config = {}
        config['name'] = params.newname
        config['dir'] = clone_disk_dir
        config['current'] = clone_disk_path
        config['poolname'] = poolname
        config['pool'] = params.pool
        with open('%s/config.json' % clone_disk_dir, "w") as f:
            dump(config, f)

        result = get_disk_info(clone_disk_path)
        # vol_xml = get_volume_xml(params.pool, params.vol)

        result['disk'] = params.newname
        result["pool"] = params.pool
        result["poolname"] = poolname
        result["uni"] = clone_disk_path
        result["current"] = clone_disk_path
    else:
        prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.newname, cstor['data']['uni'])
        result = {
            "disk": params.newname,
            "pool": params.pool,
            "poolname": pool_info['poolname'],
            "uni": cstor["data"]["uni"],
            "current": prepareInfo["data"]["path"],
            "virtual_size": params.capacity,
            "filename": prepareInfo["data"]["path"]
        }
    helper = K8sHelper("VirtualMachineDisk")
    helper.create(params.newname, "volume", result)
    success_print("success clone disk %s." % params.vol, result)

def createDiskFromImage(params):
    pool_info = get_pool_info_from_k8s(params.targetPool)
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
    config["pool"] = params.targetPool
    config["poolname"] = pool_info['poolname']

    with open('%s/config.json' % dest_dir, "w") as f:
        dump(config, f)
    result = get_disk_info(dest)
    result['disk'] = params.name
    result["pool"] = params.targetPool
    result["poolname"] = pool_info['poolname']
    result["uni"] = config['current']
    result["current"] = config['current']

    helper = K8sHelper("VirtualMachineDisk")
    helper.update(params.name, "volume", result)
    success_print("success createDiskFromImage %s." % params.name, result)

def cstor_disk_prepare(pool, vol, uni):
    op = Operation('cstor-cli vdisk-prepare ', {'poolname': pool, 'name': vol,
                                                'uni': uni}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
    return cstor

def prepareDisk(params):
    if params.domain:
        disk_paths = get_disks_spec(params.domain).keys()
        logger.debug(disk_paths)
        for path in disk_paths:
            prepare_disk_by_path(path)
    if params.vol:
        prepare_disk_by_metadataname(params.vol)
    if params.path:
        prepare_disk_by_path(params.path)

    success_print("prepare disk successful.", {})

def cstor_release_disk(pool, vol, uni):
    op = Operation('cstor-cli vdisk-release ', {'poolname': pool, 'name': vol,
                                                    'uni': uni}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
def releaseDisk(params):
    if params.domain:
        disk_paths = get_disks_spec(params.domain).keys()
        logger.debug(disk_paths)
        for path in disk_paths:
            release_disk_by_path(path)
    if params.vol:
        release_disk_by_metadataname(params.vol)
    if params.path:
        release_disk_by_path(params.path)
    success_print("success release disk %s." % params.vol, {})

def showDisk(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    if params.type != "uus":
        op = Operation('cstor-cli vdisk-show ', {'poolname': poolname, 'name': params.vol}, with_result=True)
        cstor = op.execute()
        if cstor['result']['code'] != 0:
            raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
                cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
        pool_info = get_pool_info(poolname)
        disk_dir = '%s/%s' %(pool_info['path'], params.vol)
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)

        result = get_disk_info(config['current'])
        result['disk'] = params.vol
        result["pool"] = params.pool
        result["poolname"] = poolname
        result["uni"] = config['current']
        result["current"] = config['current']
    else:
        kv = {"poolname": poolname, "name": params.vol}
        op = Operation("cstor-cli vdisk-show", kv, True)
        diskinfo = op.execute()
        if diskinfo['result']['code'] != 0:
            raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
                diskinfo['result']['code'], diskinfo['result']['msg'], diskinfo['obj']))

        result = {
            "disk": params.vol,
            "pool": params.pool,
            "poolname": poolname,
            "virtual_size": diskinfo["data"]["size"],
            "filename": diskinfo["data"]["path"],
            "uni": diskinfo["data"]["uni"]
        }

    success_print("show disk %s success." % params.pool, result)

def showDiskSnapshot(params):
    if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
        ss_info = get_snapshot_info_from_k8s(params.name)
        poolname = ss_info['poolname']
        disk_config = get_disk_config(poolname, params.vol)
        ss_path = '%s/snapshots/%s' % (disk_config['dir'], params.name)

        result = get_disk_info(ss_path)
        result['disk'] = params.vol
        result["pool"] = params.pool
        result["poolname"] = poolname
        result['snapshot'] = ss_info['snapshot']
        result["uni"] = ss_path
        success_print("success show disk snapshot %s." % params.name, ss_info)
    elif params.type == "uus":
        raise ExecuteException("", "not support operation for uus.")

def createExternalSnapshot(params):
    disk_info = get_vol_info_from_k8s(params.vol)
    poolname = disk_info['poolname']
    if params.type != 'uus':
        # prepare base
        disk_config = get_disk_config(poolname, params.vol)
        ss = os.path.basename(disk_config['current'])
        cstor_disk_prepare(poolname, ss, disk_config['current'])
    else:
        # prepare base
        cstor_disk_prepare(poolname, params.vol, disk_info['uni'])
    op = Operation('cstor-cli vdisk-add-ss ', {'poolname': poolname, 'name': params.vol,
                                               'sname': params.name}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        disk_config = get_disk_config(poolname, params.vol)
        if params.domain is None:
            if check_disk_in_use(disk_config['current']):
                raise ExecuteException('', 'disk in using, current file %s is using by another process, '
                                           'is there a vm using the current file, plz check.' % disk_config['current'])
            ss_dir = '%s/snapshots' % disk_config['dir']
            if not os.path.exists(ss_dir):
                os.makedirs(ss_dir)
            ss_path = '%s/%s' %(ss_dir, params.name)

            op1 = Operation('qemu-img create -f %s -b %s -F %s %s' %
                            (params.format, disk_config['current'], params.format, ss_path), {})
            op1.execute()

            # prepare snapshot
            cstor_disk_prepare(poolname, os.path.basename(ss_path), ss_path)

            with open('%s/config.json' % disk_config['dir'], "r") as f:
                config = load(f)
                config['current'] = ss_path
            with open('%s/config.json' % disk_config['dir'], "w") as f:
                dump(config, f)
        else:
            specs = get_disks_spec(params.domain)
            if disk_config['current'] not in specs.keys():
                logger.debug('disk %s current is %s.' % (params.vol, disk_config['current']))
                raise ExecuteException('', 'domain %s not has disk %s' % (params.domain, params.vol))

            vm_disk = specs[disk_config['current']]
            ss_path = '%s/snapshots/%s' %(disk_config['dir'], params.name)
            ss_dir = '%s/snapshots' % disk_config['dir']
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

            # prepare snapshot
            cstor_disk_prepare(poolname, os.path.basename(ss_path), ss_path)

            config_path = '%s/config.json' % os.path.dirname(ss_dir)
            with open(config_path, "r") as f:
                config = load(f)
                config['current'] = ss_path
            with open(config_path, "w") as f:
                dump(config, f)

        result = get_snapshot_info_to_k8s(poolname, params.vol, params.name)
        # modify disk in k8s
        modify_disk_info_in_k8s(poolname, params.vol)

        success_print("success create disk external snapshot %s" % params.name, result)
    else:
        # prepare snapshot
        cstor_disk_prepare(poolname, params.name, cstor['data']['uni'])
        print dumps(cstor)

# create snapshot on params.name, then rename snapshot to current
def revertExternalSnapshot(params):
    disk_info = get_pool_info_from_k8s(params.pool)
    poolname = disk_info['poolname']

    helper = K8sHelper("VirtualMachineDiskSnapshot")
    k8s_ss_info = helper.get_data(params.name, "volume")
    backing_file = k8s_ss_info['full_backing_filename']
    if params.type != 'uus':
        # prepare base
        disk_config = get_disk_config(poolname, params.vol)
        cstor_disk_prepare(poolname, os.path.basename(backing_file), backing_file)
    else:
        # prepare base
        cstor_disk_prepare(poolname, params.vol, disk_info['uni'])

    op = Operation('cstor-cli vdisk-rr-ss ', {'poolname': poolname, 'name': params.vol,
                                              'sname': params.name}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.domain and is_vm_active(params.domain):
        raise ExecuteException('', 'domain %s is still active, plz stop it first.')

    disk_config = get_disk_config(poolname, params.vol)
    if check_disk_in_use(disk_config['current']):
        raise ExecuteException('', 'error: current disk in use, plz check or set real domain field.')

    ss_path = '%s/snapshots/%s' %(disk_config['dir'], params.name)
    if ss_path is None:
        raise ExecuteException('', 'error: can not get snapshot backing file.')

    uuid = randomUUID().replace('-', '')
    new_file_path = '%s/%s' %(os.path.dirname(backing_file), uuid)
    op1 = Operation('qemu-img create -f %s -b %s -F %s %s' %
                    (params.format, backing_file, params.format, new_file_path), {})
    op1.execute()
    # change vm disk
    if params.domain and not change_vm_os_disk_file(params.domain, disk_config['current'], new_file_path):
        op2 = Operation('rm -f %s' % new_file_path, {})
        op2.execute()
        raise ExecuteException('', 'can not change disk source in domain xml')

    # modify json file, make os_event_handler to modify data on api server .
    with open('%s/config.json' % disk_config['dir'], "r") as f:
        config = load(f)
        config['current'] = new_file_path
    with open('%s/config.json' % disk_config['dir'], "w") as f:
        dump(config, f)

    # result = get_disk_info(config['current'])
    # result['disk'] = params.vol
    # result["pool"] = params.pool
    # result["poolname"] = poolname
    # result['snapshot'] = params.name
    # result["uni"] = ss_path

    # prepare snapshot
    if params.type != 'uus':
        cstor_disk_prepare(poolname, os.path.basename(ss_path), ss_path)
    else:
        cstor_disk_prepare(poolname, os.path.basename(ss_path), cstor['data']['uni'])

    # modify disk in k8s
    modify_disk_info_in_k8s(poolname, params.vol)

    success_print("success revert disk external snapshot %s." % params.name, {})

def deleteExternalSnapshot(params):
    disk_info = get_pool_info_from_k8s(params.pool)
    poolname = disk_info['poolname']

    helper = K8sHelper("VirtualMachineDiskSnapshot")
    k8s_ss_info = helper.get_data(params.name, "volume")
    backing_file = k8s_ss_info['full_backing_filename']

    if params.type != 'uus':
        # prepare base
        disk_config = get_disk_config(poolname, params.vol)
        cstor_disk_prepare(poolname, os.path.basename(backing_file), backing_file)
        cstor_disk_prepare(poolname, os.path.basename(disk_config['current']), disk_config['current'])
    else:
        # prepare base
        cstor_disk_prepare(poolname, params.vol, disk_info['uni'])

    op = Operation('cstor-cli vdisk-rm-ss ', {'poolname': poolname, 'name': params.vol,
                                              'sname': params.name}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        if params.domain:
            specs = get_disks_spec(params.domain)
            disk_config = get_disk_config(poolname, params.vol)
            if disk_config['current'] not in specs.keys():
                raise ExecuteException('', 'domain %s not has disk %s' % (params.domain, params.vol))

        disk_config = get_disk_config(poolname, params.vol)

        # get all snapshot to delete(if the snapshot backing file chain contains backing_file), except current.
        snapshots_to_delete = []
        files = os.listdir('%s/snapshots' % disk_config['dir'])
        for df in files:
            try:
                bf_paths = get_sn_chain_path('%s/snapshots/%s' %(disk_config['dir'], df))
                if backing_file in bf_paths:
                    snapshots_to_delete.append(df)
            except:
                continue

        # if snapshot to delete is current, delelte vmsn from server.
        if params.name not in snapshots_to_delete:
            snapshots_to_delete.append(params.name)

        if params.domain:
            current_backing_file = DiskImageHelper.get_backing_file(disk_config['current'])
            # reconnect the snapshot chain
            bf_bf_path = DiskImageHelper.get_backing_file(backing_file)
            if bf_bf_path:
                op = Operation('virsh blockpull --domain %s --path %s --base %s --wait' %
                               (params.domain, disk_config['current'], backing_file), {})
                op.execute()
            else:
                op = Operation('virsh blockpull --domain %s --path %s --wait' %
                               (params.domain, disk_config['current']), {})
                op.execute()
                op = Operation('rm -f %s' % backing_file, {})
                op.execute()

            # # if the snapshot to delete is not current, delete snapshot's backing file
            # if current_backing_file != backing_file:
            #     op = Operation('rm -f %s' % backing_file, {})
            #     op.execute()

        else:
            current_backing_file = DiskImageHelper.get_backing_file(disk_config['current'])
            # reconnect the snapshot chain
            paths = get_sn_chain_path(disk_config['current'])
            if backing_file in paths:
                bf_bf_path = DiskImageHelper.get_backing_file(backing_file)
                if bf_bf_path:
                    # effect current and backing file is not head, rabse current to reconnect
                    op = Operation('qemu-img rebase -b %s %s' % (bf_bf_path, disk_config['current']), {})
                    op.execute()
                else:
                    # effect current and backing file is head, rabse current to itself
                    op = Operation('qemu-img rebase -b "" %s' % disk_config['current'], {})
                    op.execute()
                    op = Operation('rm -f %s' % backing_file, {})
                    op.execute()
            # # if the snapshot to delete is not current, delete snapshot's backing file
            # if current_backing_file != backing_file:
            #     op = Operation('rm -f %s' % backing_file, {})
            #     op.execute()

        for df in snapshots_to_delete:
            if df != os.path.basename(disk_config['current']):
                op = Operation('rm -f %s/snapshots/%s' % (disk_config['dir'], df), {})
                op.execute()
        # modify json file, make os_event_handler to modify data on api server .
        with open('%s/config.json' % disk_config['dir'], "r") as f:
            config = load(f)
            config['current'] = config['current']
        with open('%s/config.json' % disk_config['dir'], "w") as f:
            dump(config, f)

        for ss in snapshots_to_delete:
            helper.delete(ss)

        modify_disk_info_in_k8s(poolname, params.vol)

        # result = {'delete_ss': snapshots_to_delete, 'disk': disk_config['name'],
        #           'need_to_modify': config['current'], "pool": params.pool, "poolname": poolname}
        success_print("success delete disk external snapshot %s." % params.name, {})
    else:
        print dumps(cstor)

def updateDiskCurrent(params):
    if params.type != "uus":
        for current in params.current:
            if params.current.find("snapshots") > 0:
                config_path = '%s/config.json' % os.path.dirname(os.path.dirname(current))
            else:
                config_path = '%s/config.json' % os.path.dirname(current)
            with open(config_path, "r") as f:
                config = load(f)
                config['current'] = current
            with open(config_path, "w") as f:
                dump(config, f)
            success_print("updateDiskCurrent successful.",{})
    else:
       error_print(400, "not support operation for uus")

def customize(params):
    op = Operation('virt-customize --add %s --password %s:password:%s' % (params.add, params.user, params.password), {})
    op.execute()
    success_print("customize  successful.", {})

def migrate(params):
    if not is_vm_disk_driver_cache_none(params.domain):
        raise ExecuteException('', 'error: disk driver cache is not none')
    if not is_vm_disk_not_shared_storage(params.domain):
        raise ExecuteException('', 'error: still has disk not create in shared storage.')

    if params.ip in get_host_IP():
        raise ExecuteException('', 'error: not valid ip address.')

    if params.offline:
        op = Operation('virsh migrate --offline --undefinesource --persistent %s qemu+ssh://%s/system tcp://%s' % (
            params.domain, params.ip, params.ip), {})
        op.execute()
    else:
        op = Operation('virsh migrate --live --undefinesource --persistent %s qemu+ssh://%s/system tcp://%s' % (
            params.domain, params.ip, params.ip), {})
        op.execute()

    success_print("migrate vm %s successful." % params.domain, {})

def xmlToJson(xmlStr):
    json = dumps(bf.data(fromstring(xmlStr)), sort_keys=True, indent=4)
    return json.replace("@", "_").replace("$", "text").replace(
        "interface", "_interface").replace("transient", "_transient").replace(
        "nested-hv", "nested_hv").replace("suspend-to-mem", "suspend_to_mem").replace("suspend-to-disk",
                                                                                      "suspend_to_disk")

def is_cstor_pool_exist(pool):
    op = Operation('cstor-cli pool-show ', {'poolname': pool}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        return True
    return False

def get_cstor_pool_info(pool):
    op = Operation('cstor-cli pool-show ', {'poolname': pool}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
    return cstor['data']

def prepare_disk_by_metadataname(uuid):
    success = False
    output = runCmdAndGetOutput(
        'kubectl get vmd -o=jsonpath="{range .items[?(@.metadata.name==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % uuid)
    if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
        success = True
    if not success:
        raise ExecuteException('', 'can not get right disk info from k8s by metadataname.')
    lines = output.splitlines()
    if len(lines) != 1:
        logger.debug(lines)
        raise ExecuteException('', 'can not get right disk info from k8s by path.')
    columns = lines[0].split()
    if len(columns) != 4:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    diskinfo = {}
    pool = columns[0]
    disk = columns[1]
    uni = columns[2]
    nodeName = columns[3]

    # if is_pool_exists(pool):
    #     pool_info = get_pool_info(pool)
    #     pool = os.path.basename(pool_info['path'])
    cstor_disk_prepare(pool, disk, uni)
    return diskinfo

def prepare_disk_by_path(path):
    success = False
    if not success:
        output = runCmdAndGetOutput('kubectl get vmd -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdsn -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdi -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    lines = output.splitlines()
    columns = lines[0].split()
    if len(columns) != 4:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    diskinfo = {}
    pool = columns[0]
    disk = columns[1]
    uni = columns[2]
    nodeName = columns[3]

    # if is_pool_exists(pool):
    #     pool_info = get_pool_info(pool)
    #     pool = os.path.basename(pool_info['path'])
    cstor_disk_prepare(pool, disk, uni)
    return diskinfo

def release_disk_by_metadataname(uuid):
    success = False
    output = runCmdAndGetOutput('kubectl get vmd -o=jsonpath="{range .items[?(@.metadata.name==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % uuid)
    if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
        success = True
    if not success:
        raise ExecuteException('', 'can not get right disk info from k8s by metadataname.')
    lines = output.splitlines()
    if len(lines) != 1:
        logger.debug(lines)
        raise ExecuteException('', 'can not get right disk info from k8s by path.')
    columns = lines[0].split()
    if len(columns) != 4:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    pool = columns[0]
    disk = columns[1]
    uni = columns[2]

    cstor_release_disk(pool, disk, uni)

def release_disk_by_path(path):
    success = False
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmd -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdsn -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdi -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 4:
            success = True
    if not success:
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    lines = output.splitlines()
    columns = lines[0].split()
    if len(columns) != 4:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    pool = columns[0]
    disk = columns[1]
    uni = columns[2]

    cstor_release_disk(pool, disk, uni)

if __name__ == '__main__':
    prepare_disk_by_path(
        '/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    prepare_disk_by_metadataname('vm00aadd6coddpdssdn')
    release_disk_by_path('/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    release_disk_by_metadataname('vm00aadd6coddpdssdn')
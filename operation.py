from xml.etree.ElementTree import fromstring
from xmljson import badgerfish as bf

from netutils import get_host_IP
from utils.k8s import get_node_name, get_hostname_in_lower_case
from utils.utils import *
from utils import logger


LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

class Operation(object):
    def __init__(self, cmd, params, with_result=False, xml_to_json=False, kv_to_json=False, remote=False, ip=None):
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
        self.remote = remote
        self.ip = ip

    def get_cmd(self):
        cmd = self.cmd
        for key in self.params.keys():
            cmd = "%s --%s %s " % (cmd, key, self.params[key])
        return cmd

    def execute(self):
        cmd = self.get_cmd()
        logger.debug(cmd)
        if self.remote:
            if self.with_result:
                logger.debug(self.remote)
                logger.debug(self.ip)
                return remoteRunCmdWithResult(self.ip, cmd)
            else:
                logger.debug(self.remote)
                logger.debug(self.ip)
                return remoteRunCmd(self.ip, cmd)
        else:
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

        result = get_pool_info_to_k8s(params.type, params.pool, params.uuid, params.content)

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

    write_config(vol, disk_dir, disk_path, pool, poolname)
    result = get_disk_info_to_k8s(poolname, vol)
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

def cstor_delete_disk(poolname, vol):
    op = Operation('cstor-cli vdisk-remove ', {'poolname': poolname, 'name': vol},
                   with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))



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
    disk_info = get_vol_info_from_k8s(params.vol)
    poolname = disk_info['poolname']
    prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.vol, disk_info['uni'])
    op = Operation('cstor-cli vdisk-expand ', {'poolname': poolname, 'name': params.vol,
                                               'size': params.capacity}, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    if params.type != "uus":
        disk_dir = '%s/%s' % (get_pool_info(poolname)['path'], params.vol)
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)

        disk_info = get_disk_info(config['current'])
        size = int(params.capacity) - int(disk_info['virtual_size'])
        op = Operation("qemu-img resize %s +%s" % (config['current'], str(size)), {})
        op.execute()

        result = get_disk_info_to_k8s(poolname, params.vol)
    else:
        result = {
            "disk": params.vol,
            "pool": params.pool,
            "poolname": poolname,
            "uni": cstor["data"]["uni"],
            "current": prepareInfo["data"]["path"],
            "virtual_size": params.capacity,
            "filename": prepareInfo["data"]["path"]
        }
    success_print("success resize disk %s." % params.vol, result)

def cloneDisk(params):
    result = None
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(params.vol)
    pool_helper = K8sHelper('VirtualMahcinePool')
    disk_node_name = get_node_name(disk_heler.get(params.vol))
    pool_node_name = get_node_name(pool_helper.get(params.pool))

    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    disk_info = get_vol_info_from_k8s(params.vol)
    old_pool_info = get_pool_info_from_k8s(disk_info['pool'])

    prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.vol, disk_info['uni'])

    if params.type != "uus":
        # create disk dir and create disk in dir.
        disk_dir = '%s/%s' % (old_pool_info['path'], params.vol)
        uuid = randomUUID().replace('-', '')
        middle_disk_dir = '%s/%s' % (old_pool_info['path'], uuid)
        middle_disk_path = '%s/%s' % (middle_disk_dir, params.newname)
        clone_disk_dir = '%s/%s' % (pool_info['path'], params.newname)
        clone_disk_path = '%s/%s' % (clone_disk_dir, params.newname)

        if not os.path.exists(middle_disk_dir):
            os.makedirs(middle_disk_dir)

        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)

        try:
            op1 = Operation('cp -f %s %s' % (config['current'], middle_disk_path), {})
            op1.execute()
        except:
            if os.path.exists(middle_disk_dir):
                op3 = Operation('rm -rf %s' % middle_disk_dir, {})
                op3.execute()
            raise ExecuteException('', 'Copy %s to middle_disk_path %s failed!, aborting clone.' % (config['current'], middle_disk_path))
        try:
            backing_file = DiskImageHelper.get_backing_file(middle_disk_path)
            if backing_file:
                op2 = Operation('qemu-img rebase -f %s -b "" %s' % (params.format, middle_disk_path), {})
                op2.execute()
        except:
            if os.path.exists(middle_disk_dir):
                op3 = Operation('rm -rf %s' % middle_disk_dir, {})
                op3.execute()
            raise ExecuteException('', 'Execute "qemu-img rebase %s" failed!, aborting clone.' % middle_disk_path)

        # write config
        config = {}
        config['name'] = params.newname
        config['dir'] = clone_disk_dir
        config['current'] = clone_disk_path
        config['pool'] = params.pool
        config['poolname'] = pool_info['poolname']

        with open('%s/config.json' % middle_disk_dir, "w") as f:
            dump(config, f)

        if disk_node_name == pool_node_name:
            op = Operation('mv %s %s/%s' % (middle_disk_dir, pool_info['path'], params.newname), {})
            op.execute()
            prepareInfo = cstor_disk_prepare(pool_info['poolname'], params.newname, clone_disk_path)

            jsondicts = get_disk_jsondict(params.pool, params.newname)
            create_all_jsondict(jsondicts)
        else:
            ip = get_node_ip_by_node_name(pool_node_name)
            op = Operation('scp -r %s root@%s:%s' % (middle_disk_dir, ip, clone_disk_dir), {})
            op.execute()
            prepareInfo = remote_cstor_disk_prepare(ip, pool_info['poolname'], params.newname, clone_disk_path)

            op = Operation('rm -rf %s' % middle_disk_dir, {})
            op.execute()

            op = Operation('kubesds-adm registerDiskToK8s --pool %s --vol %s' % (params.pool, params.newname), {}, ip=ip, remote=True, with_result=True)
            remote_result = op.execute()
            if remote_result['result']['code'] != 0:
                raise ExecuteException('RunCmdError', 'remote run cmd kubesds-adm registerDiskToK8s error.')

    else:
        op = Operation('cstor-cli vdisk-clone ', {'poolname': poolname, 'name': params.vol,
                                                  'clonename': params.newname}, with_result=True)
        cstor = op.execute()
        if cstor['result']['code'] != 0:
            raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
                cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
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
    if result:
        helper = K8sHelper("VirtualMachineDisk")
        helper.create(params.newname, "volume", result)
        success_print("success clone disk %s." % params.vol, result)
    else:
        success_print("success clone disk %s." % params.vol, {})

def registerDiskToK8s(params):
    jsondicts = get_disk_jsondict(params.pool, params.vol)

    create_all_jsondict(jsondicts)

    success_print("success register disk %s to k8s." % params.vol, {})

# only use when migrate disk to another node
def rebaseDiskSnapshot(params):
    rebase_snapshot_with_config(params.pool, params.vol)
    disk_info = get_vol_info_from_k8s(params.vol)
    cstor_disk_prepare(disk_info['poolname'], disk_info['disk'], disk_info['uni'])
    success_print("success rebase disk.", {})


def createDiskFromImage(params):
    pool_info = get_pool_info_from_k8s(params.targetPool)
    poolname = pool_info['poolname']
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

    write_config(params.name, dest_dir, dest, params.targetPool, poolname)

    result = get_disk_info_to_k8s(poolname, params.name)

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

def remote_cstor_disk_prepare(ip, pool, vol, uni):
    op = Operation('cstor-cli vdisk-prepare ', {'poolname': pool, 'name': vol,
                                                'uni': uni}, remote=True, ip=ip, with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'remote prepare disk fail. cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
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

        result = get_disk_info_to_k8s(poolname, params.vol)
    else:
        kv = {"poolname": poolname, "name": params.vol}
        op = Operation("cstor-cli vdisk-show", kv, True)
        diskinfo = op.execute()
        if diskinfo['result']['code'] != 0:
            raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
                diskinfo['result']['code'], diskinfo['result']['msg'], diskinfo['obj']))
        prepareInfo = cstor_disk_prepare(poolname, params.vol, diskinfo['data']['uni'])
        result = {
            "disk": params.vol,
            "pool": params.pool,
            "poolname": pool_info['poolname'],
            "uni": diskinfo["data"]["uni"],
            "current": prepareInfo["data"]["path"],
            "virtual_size": params.capacity,
            "filename": prepareInfo["data"]["path"]
        }

    success_print("show disk %s success." % params.pool, result)

def showDiskSnapshot(params):
    if params.type == "localfs" or params.type == "nfs" or params.type == "glusterfs" or params.type == "vdiskfs":
        ss_info = get_snapshot_info_from_k8s(params.name)
        poolname = ss_info['poolname']
        disk_config = get_disk_config(poolname, params.vol)
        ss_path = '%s/snapshots/%s' % (disk_config['dir'], params.name)

        result = get_snapshot_info_to_k8s(poolname, params.vol, params.name)
        success_print("success show disk snapshot %s." % params.name, result)
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

    # prepare snapshot
    if params.type != 'uus':
        cstor_disk_prepare(poolname, os.path.basename(ss_path), ss_path)
    else:
        cstor_disk_prepare(poolname, os.path.basename(ss_path), cstor['data']['uni'])

    # modify disk in k8s
    modify_disk_info_in_k8s(poolname, params.vol)

    # delete lifecycle
    helper.delete_lifecycle(params.name)

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

        # delete snapshot in k8s
        for ss in snapshots_to_delete:
            helper.delete(ss)

        # modify disk current info in k8s
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
    # if not is_vm_disk_not_shared_storage(params.domain):
    #     raise ExecuteException('', 'error: still has disk not create in shared storage.')

    if params.ip in get_host_IP():
        raise ExecuteException('', 'error: not valid ip address.')

    # prepare all disk
    specs = get_disks_spec(params.domain)
    for disk_path in specs.keys():
        remote_prepare_disk_by_path(params.ip, disk_path)

    if params.offline:
        op = Operation('virsh migrate --offline --undefinesource --persistent %s qemu+ssh://%s/system tcp://%s' % (
            params.domain, params.ip, params.ip), {})
        op.execute()
    else:
        op = Operation('virsh migrate --live --undefinesource --persistent %s qemu+ssh://%s/system tcp://%s' % (
            params.domain, params.ip, params.ip), {})
        op.execute()

    # get disk node label in ip
    node_name = get_node_name_by_node_ip(params.ip)
    logger.debug("node_name: %s" % node_name)
    if node_name:
        all_jsondicts = []
        logger.debug(specs)
        for disk_path in specs.keys():
            prepare_info = get_disk_prepare_info_by_path(disk_path)
            pool_info = get_pool_info_from_k8s(prepare_info['pool'])
            pools = get_pools_by_path(pool_info['path'])

            # change disk node label in k8s.
            targetPool = None
            for pool in pools:
                if pool['host'] == node_name:
                    targetPool = pool['pool']
            if targetPool:
                logger.debug("targetPool is %s." % targetPool)
                if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs']:
                    config = get_disk_config(pool_info['poolname'], prepare_info['disk'])
                    write_config(config['name'], config['dir'], config['current'], targetPool, config['poolname'])
                    jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                    all_jsondicts.extend(jsondicts)
                else:
                    cstor_release_disk(prepare_info['poolname'], prepare_info['disk'], prepare_info['uni'])
                    jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                    all_jsondicts.extend(jsondicts)
        apply_all_jsondict(all_jsondicts)

    success_print("migrate vm %s successful." % params.domain, {})

def migrateDiskFunc(sourceVol, targetPool):
    disk_info = get_vol_info_from_k8s(sourceVol)
    # prepare disk
    prepareInfo = cstor_disk_prepare(disk_info['poolname'], sourceVol, disk_info['uni'])
    source_pool_info = get_pool_info_from_k8s(disk_info['pool'])
    pool_info = get_pool_info_from_k8s(targetPool)
    logger.debug(disk_info)
    logger.debug(pool_info)
    if disk_info['pool'] == pool_info['pool']:
        raise ExecuteException('RunCmdError', 'can not migrate disk to its pool.')
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(sourceVol)
    pool_helper = K8sHelper('VirtualMahcinePool')
    pool_node_name = get_node_name(pool_helper.get(targetPool))
    disk_node_name = get_node_name(disk_heler.get(sourceVol))
    if source_pool_info['pooltype'] != 'uus' and disk_node_name != get_hostname_in_lower_case():
        raise ExecuteException('RunCmdError', 'disk is not in this node.')
    logger.debug(pool_info['pooltype'])
    if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', "vdiskfs"]:
        if source_pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', "vdiskfs"]:  # file to file
            source_dir = '%s/%s' % (get_pool_info(disk_info['poolname'])['path'], sourceVol)
            if pool_node_name == disk_node_name:
                if disk_info['poolname'] != pool_info['poolname']:
                    # cp and rebase backing file and config, then update k8s
                    op = Operation('cp -r %s %s/' % (source_dir, pool_info['path']), {})
                    op.execute()
                    rebase_snapshot_with_config(targetPool, sourceVol)
                    disk_info = get_vol_info_from_k8s(sourceVol)
                    cstor_disk_prepare(pool_info['poolname'], sourceVol, disk_info['uni'])
                    op = Operation('rm -rf %s' % source_dir, {})
                    op.execute()
            else:
                if pool_info['pooltype'] in ['nfs', 'glusterfs'] and disk_info['poolname'] == pool_info['poolname']:
                    # just change pool, label and nodename
                    config = get_disk_config(pool_info['poolname'], sourceVol)
                    write_config(sourceVol, config['dir'], config['current'], targetPool, pool_info['poolname'])
                    ip = get_node_ip_by_node_name(pool_node_name)
                    disk_info = get_vol_info_from_k8s(sourceVol)
                    remote_cstor_disk_prepare(ip, pool_info['poolname'], sourceVol, disk_info['uni'])
                    jsondicts = get_disk_jsondict(targetPool, sourceVol)
                    apply_all_jsondict(jsondicts)
                else:
                    # scp
                    ip = get_node_ip_by_node_name(pool_node_name)
                    op = Operation('scp -r %s root@%s:%s/' % (source_dir, ip, pool_info['path']), {})
                    op.execute()
                    op = Operation('kubesds-adm rebaseDiskSnapshot --pool %s --vol %s' % (targetPool, sourceVol), {},
                                   ip=ip, remote=True, with_result=True)
                    remote_result = op.execute()
                    if remote_result['result']['code'] != 0:
                        raise ExecuteException('RunCmdError', 'remote run cmd kubesds-adm rebaseDiskSnapshot error.')
                    op = Operation('rm -rf %s' % source_dir, {})
                    op.execute()
        else:  # dev to file
            cstor_disk_prepare(disk_info['poolname'], sourceVol, disk_info['uni'])
            this_node_name = get_hostname_in_lower_case()
            logger.debug('this_node_name: %s' % this_node_name)
            if pool_node_name == this_node_name:  # in same node, create file then convert.
                cstor_create_disk(pool_info['poolname'], sourceVol, prepareInfo['data']['size'])
                target_disk_dir = '%s/%s' % (pool_info['path'], sourceVol)
                if not os.path.exists(target_disk_dir):
                    os.makedirs(target_disk_dir)
                target_disk_file = '%s/%s' % (target_disk_dir, sourceVol)
                op = Operation(
                    'qemu-img convert -f raw %s -O qcow2 %s' % (prepareInfo['data']['path'], target_disk_file), {})
                op.execute()
                write_config(sourceVol, target_disk_dir, target_disk_file, targetPool, pool_info['poolname'])
                result = get_disk_info_to_k8s(pool_info['poolname'], sourceVol)
                disk_heler.update(sourceVol, 'volume', result)
                cstor_release_disk(disk_info['poolname'], sourceVol, disk_info['uni'])
                cstor_delete_disk(disk_info['poolname'], sourceVol)
            else:
                # remote prepare disk, then migrate disk in remote node
                pools = get_pools_by_poolname(pool_info['poolname'])

                # change disk node label in k8s.
                remote_dev_pool = None
                for pool in pools:
                    if pool['host'] == pool_node_name:
                        remote_dev_pool = pool['pool']
                if remote_dev_pool:
                    ip = get_node_ip_by_node_name(pool_node_name)
                    remote_cstor_disk_prepare(ip, disk_info['poolname'], sourceVol, disk_info['uni'])
                    op = Operation('kubesds-adm migrateDisk --pool %s --vol %s' % (remote_dev_pool, sourceVol), {},
                                   ip=ip, remote=True, with_result=True)
                    result = op.execute()
                    if result['result']['code'] != 0:
                        raise ExecuteException('RunCmdError', 'can not migrate disk on remote node.')
                    cstor_release_disk(disk_info['poolname'], sourceVol, disk_info['uni'])
                    cstor_delete_disk(disk_info['poolname'], sourceVol)
    else:
        if source_pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', "vdiskfs"]:  # file to dev
            raise ExecuteException('RumCmdError', 'not support storage type, can not migrate file to dev.')
            # # create disk
            # newCreateInfo = cstor_create_disk(pool_info['poolname'], params.vol, disk_info['virtual_size'])
            # uni = newCreateInfo["data"]["uni"]
            # prepareInfo = cstor_prepare_disk("uus", pool_info['poolname'], params.vol, uni)
            # op = Operation('qemu-img convert -f %s %s -O raw %s' % (disk_info['format'], disk_info['filename'], prepareInfo['data']['path']),
            #                {})
            # op.execute()
            # if pool_node_name != disk_node_name:
            #     cstor_release_disk(pool_info['poolname'], params.vol, uni)
            #     ip = get_node_ip_by_node_name(pool_node_name)
            #     remotePrepareInfo = remote_cstor_disk_prepare(ip, pool_info['poolname'], params.vol, uni)
            #     # register to k8s
            #     result = {
            #         "disk": params.vol,
            #         "pool": params.pool,
            #         "poolname": pool_info['poolname'],
            #         "uni": newCreateInfo["data"]["uni"],
            #         "current": remotePrepareInfo["data"]["path"],
            #         "virtual_size": remotePrepareInfo["data"]["size"],
            #         "filename": remotePrepareInfo["data"]["path"]
            #     }
            #     disk_heler.change_node(params.vol, pool_node_name)
            # else:
            #     # register to k8s
            #     result = {
            #         "disk": params.vol,
            #         "pool": params.pool,
            #         "poolname": pool_info['poolname'],
            #         "uni": newCreateInfo["data"]["uni"],
            #         "current": prepareInfo["data"]["path"],
            #         "virtual_size": prepareInfo["data"]["size"],
            #         "filename": prepareInfo["data"]["path"]
            #     }
            # disk_heler.update(params.vol, 'volume', result)
            # # release old disk
            # cstor_release_disk(disk_info['poolname'], params.vol, disk_info['uni'])
            # cstor_delete_disk(disk_info['poolname'], params.vol)
            # # delete disk
            # op = Operation('rm -rf %s/%s' % (source_pool_info['path'], params.vol))
            # op.execute()
        else:  # dev to dev
            # same poolname, just prepare and release
            if disk_info['poolname'] == pool_info['poolname']:
                if pool_node_name == disk_node_name:
                    raise ExecuteException('RunCmdError', 'can not migrate disk to its pool.')
                else:
                    # remote prepare disk
                    ip = get_node_ip_by_node_name(pool_node_name)
                    prepareInfo = remote_cstor_disk_prepare(ip, disk_info['poolname'], sourceVol, disk_info['uni'])
                    # release old disk
                    cstor_release_disk(disk_info['poolname'], sourceVol, disk_info['uni'])
                    result = {
                        "disk": sourceVol,
                        "pool": targetPool,
                        "poolname": pool_info['poolname'],
                        "uni": prepareInfo["data"]["uni"],
                        "current": prepareInfo["data"]["path"],
                        "virtual_size": disk_info['virtual_size'],
                        "filename": prepareInfo["data"]["path"]
                    }
                    disk_heler.update(sourceVol, 'volume', result)
                    disk_heler.change_node(sourceVol, pool_node_name)
            else:
                raise ExecuteException('RunCmdError',
                                       'can not migrate disk to this pool. Not support operation.')
                # source_pool_info = get_pool_info_from_k8s(disk_info['pool'])
                # if pool_info['path'] == source_pool_info['path']:
                #     raise ExecuteException('RunCmdError',
                #                            'can not migrate disk to this pool. Because their uni is equal.')
                # # raise ExecuteException('RunCmdError', 'can not migrate disk to this pool. Because their poolname is not equal.')
                # # prepare disk
                # prepareInfo = cstor_disk_prepare(disk_info['poolname'], params.vol, disk_info['uni'])
                # ifFile = prepareInfo["data"]["path"]
                # # create same disk in target pool
                # newCreateInfo = cstor_create_disk(pool_info['poolname'], params.vol, disk_info['virtual_size'])
                # uni = newCreateInfo["data"]["uni"]
                # newPrepareInfo = cstor_prepare_disk("uus", pool_info['poolname'], params.vol, uni)
                # ofFile = newPrepareInfo["data"]["path"]
                # # dd
                # op = Operation('dd if=%s of=%s' % (ifFile, ofFile), {})
                # op.execute()
                # if pool_node_name != disk_node_name:
                #     cstor_release_disk(pool_info['poolname'], params.vol, uni)
                #     ip = get_node_ip_by_node_name(pool_node_name)
                #     remotePrepareInfo = remote_cstor_disk_prepare(ip, pool_info['poolname'], params.vol, uni)
                #     # register to k8s
                #     result = {
                #         "disk": params.vol,
                #         "pool": params.pool,
                #         "poolname": pool_info['poolname'],
                #         "uni": newCreateInfo["data"]["uni"],
                #         "current": remotePrepareInfo["data"]["path"],
                #         "virtual_size": remotePrepareInfo["data"]["size"],
                #         "filename": remotePrepareInfo["data"]["path"]
                #     }
                #     disk_heler.change_node(params.vol, pool_node_name)
                # else:
                #     # register to k8s
                #     result = {
                #         "disk": params.vol,
                #         "pool": params.pool,
                #         "poolname": pool_info['poolname'],
                #         "uni": newCreateInfo["data"]["uni"],
                #         "current": newPrepareInfo["data"]["path"],
                #         "virtual_size": newPrepareInfo["data"]["size"],
                #         "filename": newPrepareInfo["data"]["path"]
                #     }
                # disk_heler.update(params.vol, 'volume', result)
                # # release old disk
                # cstor_release_disk(disk_info['poolname'], params.vol, disk_info['uni'])
                # cstro_delete_disk(disk_info['poolname'], params.vol)


def migrateDisk(params):
    migrateDiskFunc(params.vol, params.pool)
    success_print("success migrate disk.", {})

# cold migrate
def migrateVMDisk(params):
    if not is_vm_disk_driver_cache_none(params.domain):
        raise ExecuteException('', 'error: disk driver cache is not none')
    # if not is_vm_disk_not_shared_storage(params.domain):
    #     raise ExecuteException('', 'error: still has disk not create in shared storage.')

    if params.ip in get_host_IP():
        raise ExecuteException('', 'error: not valid ip address.')

    # prepare all disk
    specs = get_disks_spec(params.domain)
    oldPools = {}
    for disk_path in specs.keys():
        prepare_info = get_disk_prepare_info_by_path(disk_path)
        oldPools[prepare_info['disk']] = prepare_info['pool']
    vps = []
    migrateVols = []
    for line in params.migratedisks.split(';'):
        vp = {}
        vol = None
        pool = None
        for arg in line.split(','):
            if arg.split('=')[0] == 'vol':
                vol = arg.split('=')[1]
            if arg.split('=')[0] == 'pool':
                pool = arg.split('=')[1]
        if vol and pool:
            prepare_info = get_disk_prepare_info_by_path(vol)
            source_pool_info = get_pool_info_from_k8s(prepare_info['pool'])
            target_pool_info = get_pool_info_from_k8s(pool)
            if source_pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs'] and target_pool_info['pooltype'] == 'uus':
                raise ExecuteException('RunCmdError', 'not support migrate disk file to dev.')
            if source_pool_info['pooltype'] == 'uus' and target_pool_info['pooltype'] == 'uus' and source_pool_info['poolname'] != target_pool_info['poolname']:
                raise ExecuteException('RunCmdError', 'not support migrate disk dev to dev with different poolname.')
            migrateVols.append(vol)
            vp['vol'] = vol
            vp['pool'] = pool
        else:
            raise ExecuteException('RunCmdError', 'migratedisks param is illegal.')
    for disk_path in specs.keys():
        # prepare
        prepare_disk_by_path(disk_path)
        if disk_path not in migrateVols:
            # remote prepare
            remote_prepare_disk_by_path(params.ip, disk_path)
    uuid = randomUUID().replace('-', '')
    xmlfile = '/tmp/%s.xml' % uuid
    logger.debug("xmlfile: %s" % xmlfile)
    op = Operation('virsh dumpxml %s > %s' % (params.domain, xmlfile), {})
    op.execute()

    # get disk node label in ip
    node_name = get_node_name_by_node_ip(params.ip)
    logger.debug("node_name: %s" % node_name)
    if node_name:
        all_jsondicts = []
        logger.debug(specs)
        try:
            for disk_path in specs.keys():
                if disk_path not in migrateVols:
                    prepare_info = get_disk_prepare_info_by_path(disk_path)
                    pool_info = get_pool_info_from_k8s(prepare_info['pool'])
                    pools = get_pools_by_path(pool_info['path'])

                    # change disk node label in k8s.
                    targetPool = None
                    for pool in pools:
                        if pool['host'] == node_name:
                            targetPool = pool['pool']
                    if targetPool:
                        logger.debug("targetPool is %s." % targetPool)
                        if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs']:
                            config = get_disk_config(pool_info['poolname'], prepare_info['disk'])
                            write_config(config['name'], config['dir'], config['current'], targetPool,
                                         config['poolname'])
                            jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                            all_jsondicts.extend(jsondicts)
                        else:
                            jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                            all_jsondicts.extend(jsondicts)
                else:
                    for vp in vps:
                        vol = get_disk_prepare_info_by_path(vp['vol'])['disk']
                        migrateDiskFunc(vol, vp['pool'])
                        disk_info = get_vol_info_from_k8s(vol)
                        if not modofy_vm_disk_file(xmlfile, vp['vol'], disk_info['current']):
                            raise ExecuteException('RunCmdError', 'Can not change vm disk file.')
        except ExecuteException, e:
            for vp in vps:
                try:
                    vol = get_disk_prepare_info_by_path(vp['vol'])['disk']
                    migrateDiskFunc(vol, oldPools[vol])
                except:
                    pass
            raise e
        op = Operation('scp %s root@%s:%s' % (xmlfile, params.ip, xmlfile), {})
        op.execute()
        op = Operation('virsh define %s' % xmlfile, {}, ip=params.ip, remote=True)
        op.execute()
        try:
            op = Operation('virsh start %s' % params.domain, {}, ip=params.ip, remote=True)
            op.execute()
        except ExecuteException, e:
            op = Operation('virsh undefine %s' % params.domain, {}, ip=params.ip, remote=True)
            op.execute()
            for vp in vps:
                try:
                    vol = get_disk_prepare_info_by_path(vp['vol'])['disk']
                    migrateDiskFunc(vol, oldPools[vol])
                except:
                    pass
            raise e
        apply_all_jsondict(all_jsondicts)

    for disk_path in specs.keys():
        # release
        release_disk_by_path(disk_path)
    op = Operation('virsh undefine %s' % params.domain, {})
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

def get_disk_prepare_info_by_path(path):
    success = False
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmd -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\t\\"}{.spec.volume.pool}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 5:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdsn -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\t\\"}{.spec.volume.pool}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 5:
            success = True
    if not success:
        output = runCmdAndGetOutput(
            'kubectl get vmdi -o=jsonpath="{range .items[?(@.spec.volume.filename==\\"%s\\")]}{.spec.volume.poolname}{\\"\\t\\"}{.spec.volume.disk}{\\"\\t\\"}{.spec.volume.uni}{\\"\\t\\"}{.spec.nodeName}{\\"\\t\\"}{.spec.volume.pool}{\\"\\n\\"}{end}"' % path)
        if output and len(output.splitlines()) == 1 and len(output.splitlines()[0].split()) == 5:
            success = True
    if not success:
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    lines = output.splitlines()
    columns = lines[0].split()
    if len(columns) != 5:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path. less info')
    diskinfo = {}
    diskinfo['poolname'] = columns[0]
    diskinfo['disk'] = columns[1]
    diskinfo['uni'] = columns[2]
    diskinfo['nodeName'] = columns[3]
    diskinfo['pool'] = columns[4]
    return diskinfo

def prepare_disk_by_path(path):
    diskinfo = get_disk_prepare_info_by_path(path)
    pool = diskinfo['poolname']
    disk = diskinfo['disk']
    uni = diskinfo['uni']
    nodeName = diskinfo['nodeName']

    cstor_disk_prepare(pool, disk, uni)
    return diskinfo

def remote_prepare_disk_by_path(ip, path):
    diskinfo = get_disk_prepare_info_by_path(path)
    pool = diskinfo['poolname']
    disk = diskinfo['disk']
    uni = diskinfo['uni']
    remote_cstor_disk_prepare(ip, pool, disk, uni)
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
    diskinfo = get_disk_prepare_info_by_path(path)
    pool = diskinfo['poolname']
    disk = diskinfo['disk']
    uni = diskinfo['uni']

    cstor_release_disk(pool, disk, uni)

if __name__ == '__main__':
    # print get_disks_spec('vm006')
    print get_disk_prepare_info_by_path('/var/lib/libvirt/cstor/1709accdd174caced76b0db2235/1709accdd174caced76b0db2235/vm006migratedisk2/snapshots/vm006migratedisk2.1')
    # prepare_disk_by_path(
    #     '/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    # prepare_disk_by_metadataname('vm00aadd6coddpdssdn')
    # release_disk_by_path('/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    # release_disk_by_metadataname('vm00aadd6coddpdssdn')
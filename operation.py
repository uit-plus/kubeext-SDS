from netutils import get_host_IP
from utils.ftp import *
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
    if params.opt:
        kv = {"type": params.type, "poolname": params.uuid, "url": params.url, "opt": params.opt, "uuid": params.pool}
    else:
        kv = {"type": params.type, "poolname": params.uuid, "url": params.url, "uuid": params.pool}
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
            "free": cstor["data"]["free"],
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

        if params.type == 'vdiskfs' and cstor['data']['status'] == 'active':
            # make other vdiskfs pool inactive
            pool_path = '%s/%s' % (cstor['data']['mountpath'], params.uuid)
            pools = get_pools_by_path(pool_path)
            node_name = get_hostname_in_lower_case()
            poolHelper = K8sHelper('VirtualMachinePool')
            for pool in pools:
                if pool['host'] != node_name:
                    pool_info = get_pool_info_from_k8s(pool['pool'])
                    pool_info['state'] = 'inactive'
                    poolHelper.update(pool['pool'], 'pool', pool_info)
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

    helper = K8sHelper("VirtualMachinePool")
    helper.delete(params.pool)
    success_print("delete pool %s successful." % params.pool, {})


def startPool(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    poolname = pool_info['poolname']
    if params.type != "uus":
        if pool_info['pooltype'] == 'vdiskfs':
            poolActive(pool_info['poolname'])
        if not is_pool_started(pool_info['poolname']):
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
    cstor = get_cstor_pool_info(poolname)
    if params.type != 'uus':
        result = get_pool_info(poolname)
        if is_pool_started(poolname) and cstor['status'] == 'active':
            result['state'] = "active"
        else:
            result['state'] = "inactive"
        result['content'] = pool_info["content"]
        result["pooltype"] = pool_info["pooltype"]
        result["pool"] = params.pool
        result["free"] = cstor["free"]
        result["poolname"] = pool_info["poolname"]
    else:
        result = {
            "pooltype": params.type,
            "pool": params.pool,
            "poolname": poolname,
            "capacity": cstor["total"],
            "free": cstor["free"],
            "autostart": "no",
            "path": cstor["url"],
            "state": cstor["status"],
            "uuid": randomUUID(),
            "content": 'vmd'
        }
    # update pool
    if cmp(pool_info, result) != 0:
        k8s = K8sHelper('VirtualMachinePool')
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
    check_pool_active(pool_info)
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


def updateOS(params):
    if not is_vm_exist(params.domain):
        raise ExecuteException('', 'not exist domain %s.' % params.domain)

    if is_vm_active(params.domain):
        raise ExecuteException('', 'domain %s is still running, plz stop it first.' % params.domain)

    prepare_disk_by_path(params.source)
    prepare_disk_by_path(params.target)

    disks = get_disks_spec(params.domain)
    if params.source not in disks.keys() or disks[params.source] != 'vda':
        raise ExecuteException('', '%s is not in domain %s disks.' % (params.source, params.domain))

    if not os.path.exists(params.source):
        raise ExecuteException('', 'source file %s not exist.' % params.source)
    if not os.path.exists(params.target):
        raise ExecuteException('', 'target file %s not exist.' % params.target)

    info = get_disk_prepare_info_by_path(params.source)

    vol = info['disk']
    pool = info['pool']
    vol_info = get_vol_info_from_k8s(vol)
    pool_info = get_pool_info_from_k8s(pool)

    # disk_file_need_delete = []
    snapshots_need_to_delete = []

    disk_dir = '%s/%s' % (pool_info['path'], vol)

    snapshots_dir = '%s/snapshots' % disk_dir
    if os.path.exists(snapshots_dir):
        for df in os.listdir(snapshots_dir):
            try:
                ss_info = get_snapshot_info_from_k8s(df)
                snapshots_need_to_delete.append(df)
            except:
                pass

    new_path = '%s/%s/%s' % (pool_info['path'], vol, vol)
    op = Operation('cp -f %s %s' % (params.target, new_path), {})
    op.execute()

    # write_config(vol, '%s/%s' % (pool_info['path'], vol), new_path, pool, pool_info['poolname'])

    for df in os.listdir(disk_dir):
        try:
            if os.path.isdir('%s/%s' % (disk_dir, df)):
                op = Operation('rm -rf %s/%s' % (disk_dir, df), {})
                op.execute()
            else:
                if df == 'config.json' or df == vol:
                    continue
                else:
                    op = Operation('rm -f %s/%s' % (disk_dir, df), {})
                    op.execute()
        except:
            pass
    change_vol_current(vol, new_path)
    change_vm_os_disk_file(params.domain, params.source, new_path)
    modifyVMOnNode(params.domain)
    ss_helper = K8sHelper("VirtualMachineDiskSnapshot")
    for ss in snapshots_need_to_delete:
        if ss_helper.exist(ss):
            ss_helper.delete(ss)

    success_print("updateOS %s successful." % params.domain, {})


def createCloudInitUserDataImage(params):
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)
    poolname = pool_info['poolname']

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'uus pool %s not support.' % params.pool)

    # cfg = '/tmp/%s.cfg' % randomUUID()
    # logger.debug(params.userData)
    # with open(cfg, 'w') as f:
    #     data = ''
    #     for line in params.userData:
    #         data += line.replace(';;;', '\r\n').replace('+', '-')
    #     logger.debug(data)
    #     f.write(data)

    createInfo = cstor_create_disk(poolname, params.vol, 1000000)

    disk_dir = '%s/%s' % (pool_info['path'], params.vol)
    if not os.path.exists(disk_dir):
        os.makedirs(disk_dir)

    disk_path = '%s/%s' % (disk_dir, params.vol)
    op = Operation('cloud-localds %s %s' % (disk_path, params.userData), {})
    op.execute()

    cstor_disk_prepare(poolname, params.vol, disk_path)
    write_config(params.vol, disk_dir, disk_path, params.pool, poolname)
    result = get_disk_info_to_k8s(poolname, params.vol)

    success_print("create CloudInitUserDataImage %s successful." % params.vol, result)


def deleteCloudInitUserDataImage(params):
    try:
        helper = K8sHelper("VirtualMachineDisk")
        disk_info = helper.get_data(params.vol, "volume")
        if disk_info is None:
            helper.delete(params.vol)
            success_print("delete disk %s successful." % params.vol, {})
    except ExecuteException, e:
        error_print(400, e.message)

    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)

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

    if pool_info['pooltype'] != "uus":
        pool_info = get_pool_info(poolname)
        disk_dir = '%s/%s' % (pool_info['path'], params.vol)
        snapshots_path = '%s/snapshots' % disk_dir
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)
        if os.path.exists(snapshots_path):
            for file in os.listdir(snapshots_path):
                if '%s/%s' % (snapshots_path, file) == config['current']:
                    continue
                else:
                    try:
                        ss_info = get_snapshot_info_from_k8s(file)
                    except:
                        continue
                    raise ExecuteException('', 'error: disk %s still has snapshot %s.' % (params.vol, file))

        op = Operation("rm -rf %s" % disk_dir, {})
        op.execute()

    helper = K8sHelper("VirtualMachineDisk")
    helper.delete(params.vol)
    success_print("delete CloudInitUserDataImage %s successful." % params.vol, {})


def cstor_delete_disk(poolname, vol):
    op = Operation('cstor-cli vdisk-remove ', {'poolname': poolname, 'name': vol},
                   with_result=True)
    cstor = op.execute()
    if cstor['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))


# only can delete disk which not has snapshot.
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
        disk_dir = '%s/%s' % (pool_info['path'], params.vol)
        snapshots_path = '%s/snapshots' % disk_dir
        with open('%s/config.json' % disk_dir, "r") as f:
            config = load(f)
        if os.path.exists(snapshots_path):
            for file in os.listdir(snapshots_path):
                if '%s/%s' % (snapshots_path, file) == config['current']:
                    continue
                else:
                    try:
                        ss_info = get_snapshot_info_from_k8s(file)
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
    pool_helper = K8sHelper('VirtualMachinePool')
    disk_node_name = get_node_name(disk_heler.get(params.vol))
    pool_node_name = get_node_name(pool_helper.get(params.pool))

    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)
    poolname = pool_info['poolname']
    disk_info = get_vol_info_from_k8s(params.vol)
    old_pool_info = get_pool_info_from_k8s(disk_info['pool'])
    check_pool_active(old_pool_info)

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
            raise ExecuteException('', 'Copy %s to middle_disk_path %s failed!, aborting clone.' % (
                config['current'], middle_disk_path))
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

            op = Operation('kubesds-adm registerDiskToK8s --pool %s --vol %s' % (params.pool, params.newname), {},
                           ip=ip, remote=True, with_result=True)
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
    check_pool_active(pool_info)
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
        raise ExecuteException('',
                               'remote prepare disk fail. cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
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
            "virtual_size": prepareInfo["data"]["size"],
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
            ss_path = '%s/%s' % (ss_dir, params.name)

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
            ss_path = '%s/snapshots/%s' % (disk_config['dir'], params.name)
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
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)
    poolname = pool_info['poolname']

    helper = K8sHelper("VirtualMachineDiskSnapshot")
    k8s_ss_info = helper.get_data(params.name, "volume")
    backing_file = k8s_ss_info['full_backing_filename']
    if params.type != 'uus':
        # prepare base
        disk_config = get_disk_config(poolname, params.vol)
        cstor_disk_prepare(poolname, os.path.basename(backing_file), backing_file)
    else:
        # prepare base
        cstor_disk_prepare(poolname, params.vol, pool_info['uni'])

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

    ss_path = '%s/snapshots/%s' % (disk_config['dir'], params.name)
    if ss_path is None:
        raise ExecuteException('', 'error: can not get snapshot backing file.')

    uuid = randomUUID().replace('-', '')
    new_file_path = '%s/%s' % (os.path.dirname(backing_file), uuid)
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
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)
    poolname = pool_info['poolname']

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
        cstor_disk_prepare(poolname, params.vol, pool_info['uni'])

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
                bf_paths = get_sn_chain_path('%s/snapshots/%s' % (disk_config['dir'], df))
                if backing_file in bf_paths:
                    snapshots_to_delete.append(df)
            except:
                continue

        # if snapshot to delete is current, delete vmsn from server.
        if params.name not in snapshots_to_delete:
            snapshots_to_delete.append(params.name)

        if backing_file in get_sn_chain_path(disk_config['current']):
            if params.domain and is_vm_active(params.domain):
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
            success_print("updateDiskCurrent successful.", {})
    else:
        error_print(400, "not support operation for uus")


def customize(params):
    if params.user and params.password:
        op = Operation('virt-customize --add %s --password %s:password:%s' % (params.add, params.user, params.password),
                       {})
        op.execute()
    elif params.ssh_inject:
        cmd = 'virt-customize --add %s --ssh-inject \"%s\"' % (params.add, params.ssh_inject)
        # print cmd
        op = Operation(cmd, {})
        op.execute()
    else:
        raise ExecuteException('', 'plz give right args and value.')
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
            # check_pool_active(pool_info)

            pools = get_pools_by_path(pool_info['path'])

            # change disk node label in k8s.
            targetPool = None
            for pool in pools:
                if pool['host'] == node_name:
                    targetPool = pool['pool']
            if targetPool:
                logger.debug("targetPool is %s." % targetPool)
                if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs']:
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


def changeDiskPool(params):
    if not os.path.exists(params.xml):
        raise ExecuteException('RunCmdError', 'can not find vm xml file: %s.' % params.xml)

    # get disk node label in ip
    node_name = get_hostname_in_lower_case()
    # node_name = get_node_name_by_node_ip(params.ip)
    logger.debug("node_name: %s" % node_name)
    specs = get_disks_spec_by_xml(params.xml)
    all_jsondicts = []
    logger.debug(specs)
    for disk_path in specs.keys():
        prepare_info = get_disk_prepare_info_by_path(disk_path)
        pool_info = get_pool_info_from_k8s(prepare_info['pool'])
        # check_pool_active(pool_info)

        pools = get_pools_by_path(pool_info['path'])

        # change disk node label in k8s.
        targetPool = None
        for pool in pools:
            if pool['host'] == node_name:
                targetPool = pool['pool']
        if targetPool:
            logger.debug("targetPool is %s." % targetPool)
            if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs']:
                if pool_info['pooltype'] == 'vdiskfs':
                    poolActive(pool_info['poolname'])
                config = get_disk_config(pool_info['poolname'], prepare_info['disk'])
                write_config(config['name'], config['dir'], config['current'], targetPool, config['poolname'])
                jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                all_jsondicts.extend(jsondicts)
            else:
                cstor_release_disk(prepare_info['poolname'], prepare_info['disk'], prepare_info['uni'])
                jsondicts = get_disk_jsondict(targetPool, prepare_info['disk'])
                all_jsondicts.extend(jsondicts)
        else:
            raise ExecuteException('RunCmdError',
                                   'can not find pool %s on node %s.' % (pool_info['poolname'], node_name))
    apply_all_jsondict(all_jsondicts)
    success_print("register vm disk %s successful.", {})


def migrateDiskFunc(sourceVol, targetPool):
    disk_info = get_vol_info_from_k8s(sourceVol)
    # prepare disk
    prepareInfo = cstor_disk_prepare(disk_info['poolname'], sourceVol, disk_info['uni'])
    source_pool_info = get_pool_info_from_k8s(disk_info['pool'])
    check_pool_active(source_pool_info)
    pool_info = get_pool_info_from_k8s(targetPool)
    check_pool_active(pool_info)

    logger.debug(disk_info)
    logger.debug(pool_info)
    if disk_info['pool'] == pool_info['pool']:
        raise ExecuteException('RunCmdError', 'can not migrate disk to its pool.')
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(sourceVol)
    pool_helper = K8sHelper('VirtualMachinePool')
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
                    op = Operation('cp -rf %s %s/' % (source_dir, pool_info['path']), {})
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
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(params.vol)
    migrateDiskFunc(params.vol, params.pool)
    success_print("success migrate disk.", {})


def modifyVM(params):
    modifyVMOnNode(params.domain)
    success_print("success modifyVM.", {})


# cold migrate
def migrateVMDisk(params):
    if is_vm_active(params.domain):
        raise ExecuteException('', 'error: vm is still running, plz stop it firstly.')
    if not is_vm_disk_driver_cache_none(params.domain):
        raise ExecuteException('', 'error: disk driver cache is not none')
    # if not is_vm_disk_not_shared_storage(params.domain):
    #     raise ExecuteException('', 'error: still has disk not create in shared storage.')

    # prepare all disk
    specs = get_disks_spec(params.domain)
    oldPools = {}
    vmVols = []
    for disk_path in specs.keys():
        prepare_info = get_disk_prepare_info_by_path(disk_path)
        vmVols.append(prepare_info['disk'])
        oldPools[prepare_info['disk']] = prepare_info['pool']
    vps = []
    migrateVols = []
    notReleaseVols = []
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
            check_pool_active(source_pool_info)
            target_pool_info = get_pool_info_from_k8s(pool)
            check_pool_active(target_pool_info)
            if source_pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs'] and target_pool_info[
                'pooltype'] == 'uus':
                raise ExecuteException('RunCmdError', 'not support migrate disk file to dev.')
            if source_pool_info['pooltype'] == 'uus' and target_pool_info['pooltype'] == 'uus' and source_pool_info[
                'poolname'] != target_pool_info['poolname']:
                raise ExecuteException('RunCmdError', 'not support migrate disk dev to dev with different poolname.')
            migrateVols.append(vol)
            notReleaseVols.append(prepare_info['disk'])
            vp['vol'] = vol
            vp['pool'] = pool
            vps.append(vp)
        else:
            raise ExecuteException('RunCmdError', 'migratedisks param is illegal.')

    uuid = randomUUID().replace('-', '')
    xmlfile = '/tmp/%s.xml' % uuid
    logger.debug("xmlfile: %s" % xmlfile)
    op = Operation('virsh dumpxml %s > %s' % (params.domain, xmlfile), {})
    op.execute()

    # get disk node label in ip
    node_name = get_node_name_by_node_ip(params.ip)
    logger.debug("node_name: %s" % node_name)

    if params.ip in get_host_IP():
        # not migrate vm, just migrate some disk to other pool
        for disk_path in specs.keys():
            # prepare
            prepare_info = prepare_disk_by_path(disk_path)
        logger.debug(specs)
        for vp in vps:
            vol = get_disk_prepare_info_by_path(vp['vol'])['disk']
            logger.debug('migrate disk %s to %s.' % (vol, vp['pool']))
            migrateDiskFunc(vol, vp['pool'])
            disk_info = get_vol_info_from_k8s(vol)
            if not modofy_vm_disk_file(xmlfile, vp['vol'], disk_info['current']):
                raise ExecuteException('RunCmdError', 'Can not change vm disk file.')

        op = Operation('virsh define %s' % xmlfile, {})
        op.execute()

        modifyVMOnNode(params.domain)
        success_print("migrate vm disk %s successful." % params.domain, {})
    else:
        # migrate vm to another node
        if node_name:
            for disk_path in specs.keys():
                # prepare
                prepare_info = prepare_disk_by_path(disk_path)
                if disk_path not in migrateVols:
                    # remote prepare
                    remote_prepare_disk_by_path(params.ip, disk_path)
            all_jsondicts = []
            logger.debug(specs)
            try:
                for disk_path in specs.keys():
                    if disk_path not in migrateVols:
                        prepare_info = get_disk_prepare_info_by_path(disk_path)
                        pool_info = get_pool_info_from_k8s(prepare_info['pool'])
                        # check_pool_active(pool_info)
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
                        logger.debug(vps)
                        logger.debug('migrate disks')
                        for vp in vps:
                            vol = get_disk_prepare_info_by_path(vp['vol'])['disk']
                            logger.debug('migrate disk %s to %s.' % (vol, vp['pool']))
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
            for vol in vmVols:
                if vol not in notReleaseVols:
                    # release
                    release_disk_by_metadataname(vol)
            apply_all_jsondict(all_jsondicts)
            op = Operation('kubesds-adm modifyVM --domain %s' % params.domain, {}, ip=params.ip, remote=True,
                           with_result=True)
            result = op.execute()
            if result['result']['code'] != 0:
                raise ExecuteException('RunCmdError', 'can not modify vm on k8s.')
            vmHelper = K8sHelper('VirtualMachine')
            vmHelper.change_node(params.domain, node_name)
            op = Operation('virsh undefine %s' % params.domain, {})
            op.execute()
            success_print("migrate vm disk %s successful." % params.domain, {})
        else:
            error_print(1, 'can not migrate vm disk, can not find target node.')


def exportVM(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    if not is_vm_exist(params.domain):
        raise ExecuteException('', 'domain %s is not exist. plz check it.' % params.domain)

    target_path = '%s/%s' % (params.path, params.domain)
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    # save vm xml file
    op = Operation('virsh dumpxml %s > %s/%s.xml' % (params.domain, target_path, params.domain), {})
    op.execute()
    disk_specs = get_disks_spec(params.domain)
    for disk_path in disk_specs.keys():
        disk_info = get_disk_prepare_info_by_path(disk_path)
        pool_info = get_pool_info_from_k8s(disk_info['pool'])
        check_pool_active(pool_info)

        if pool_info['pooltype'] == 'localfs':
            if not os.path.exists(disk_path):
                raise ExecuteException('', 'vm disk file %s not exist, plz check it.' % disk_path)
            dest = '%s/%s' % (target_path, os.path.basename(disk_path))

            # snapshot
            op1 = Operation('cp -f %s %s' % (disk_path, dest), {})
            op1.execute()

            qemu_info = get_disk_info(dest)
            if 'full_backing_filename' in qemu_info.keys():
                disk_format = qemu_info['format']
                op2 = Operation('qemu-img rebase -f %s -b "" %s' % (disk_format, dest), {})
                op2.execute()
    success_print("success exportVM.", {})


def backupDisk(params):
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(params.vol)
    ftp = FtpHelper(params.remote, params.port, params.username, params.password)

    if params.full:
        backup_vm_disk(params.domain, params.pool, params.vol, params.version, params.full, None)
    else:
        full_version = get_full_version(params.domain, params.pool, params.vol, params.version)
        backup_vm_disk(params.domain, params.pool, params.vol, params.version, params.full, full_version)
    if params.remote:
        push_disk_backup(params.domain, params.pool, params.vol, params.version, params.remote, params.port, params.username, params.password)
    success_print("success backupDisk.", {})


def backup_vm_disk(domain, pool, disk, version, is_full, full_version):
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(disk)

    # check vm exist or not
    if not is_vm_exist(domain):
        raise ExecuteException('', 'domain %s is not exist. plz check it.' % domain)

    disk_info = get_vol_info_from_k8s(disk)
    disk_pool_info = get_pool_info_from_k8s(disk_info['pool'])
    check_pool_active(disk_pool_info)

    if disk_pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk %s is uus type, not support backup.' % disk)

    # check backup pool path exist or not
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk backup pool can not be uus.')
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    disk_specs = get_disks_spec(domain)
    vm_disks = {}
    disk_tag = {}
    for disk_path in disk_specs.keys():
        if disk_path.find('snapshots') < 0:
            disk_mn = os.path.basename(os.path.dirname(disk_path))
        else:
            disk_mn = os.path.basename(os.path.dirname(os.path.dirname(disk_path)))
        vm_disks[disk_mn] = disk_path
        disk_tag[disk_mn] = disk_specs[disk_path]
    if disk not in vm_disks.keys():
        raise ExecuteException('', 'domain not attach diak %s, can find disk %s used by domain %s xml.' % (
            disk, disk, domain))

    # check backup version exist or not
    disk_backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(disk_backup_dir):
        os.makedirs(disk_backup_dir)
    history_file_path = '%s/history.json' % disk_backup_dir
    if is_disk_backup_exist(domain, pool, disk, version):
        raise ExecuteException('', 'disk %s backup version %s has exist, plz use another version.' % (
            disk, version))

    # do vm snapshots
    uuid = randomUUID().replace('-', '')
    cmd = 'virsh snapshot-create-as --domain %s --name %s --atomic --disk-only --no-metadata ' % (domain, uuid)

    cstor_disk_prepare(disk_info['poolname'], disk_info['disk'], disk_info['uni'])

    disk_dir = '%s/%s' % (disk_pool_info['path'], disk_info['disk'])
    ss_path = '%s/%s' % (disk_dir, uuid)
    cmd = '%s --diskspec %s,snapshot=external,file=%s,driver=qcow2' % (cmd, disk_specs[vm_disks[disk]], ss_path)
    for disk_path in disk_specs.keys():
        if disk_path != vm_disks[disk]:
            cmd = '%s --diskspec %s,snapshot=no' % (cmd, disk_specs[disk_path])
    if not os.path.exists(disk_dir):
        raise ExecuteException('', 'vm disk %s dir %s not exist, plz check it.' % (disk, disk_dir))

    op = Operation(cmd, {})
    op.execute()

    # backup disk dir
    if full_version:    # vm backup, use vm full version
        current_full_version = version
    else:
        if is_full:
            current_full_version = version
        else:
            current_full_version = get_disk_backup_current(domain, pool, disk)
    if not os.path.exists(disk_backup_dir):
        os.makedirs(disk_backup_dir)
    backup_dir = '%s/%s' % (disk_backup_dir, current_full_version)
    try:
        chain = backup_snapshots_chain(ss_path, backup_dir)

        # write backup record
        if not os.path.exists(history_file_path):
            history = {}
        else:
            with open(history_file_path, 'r') as f:
                history = load(f)

        if current_full_version not in history.keys():
            history[current_full_version] = {}

        count = len(history[current_full_version].keys())

        chain['index'] = count + 1
        chain['time'] = time.time()
        history[current_full_version][version] = chain
        if 'current' not in history.keys():
            history['current'] = {}
        history['current'] = current_full_version

        with open(history_file_path, 'w') as f:
            dump(history, f)
    except ExecuteException, e:
        raise e
    finally:
        # change disk current
        # change_vol_current(disk, ss_path)
        base = DiskImageHelper.get_backing_file(ss_path)
        if is_vm_active(domain):
            op = Operation('virsh blockcommit --domain %s %s --base %s --pivot --active' % (domain, disk_tag[disk], base),
                           {})
            op.execute()
        else:
            op = Operation('qemu-img commit -b %s %s' % (base, ss_path), {})
            op.execute()
            change_vm_os_disk_file(domain, ss_path, base)
        op = Operation('rm -f %s' % ss_path, {})
        op.execute()


def restore_vm_disk(domain, pool, disk, version, newname, target):
    if newname and target is None:
        raise ExecuteException('', 'newname and target must be set together.' % domain)
    # check vm exist or not
    if not newname and not is_vm_exist(domain):
        raise ExecuteException('', 'domain %s is not exist. plz check it.' % domain)

    if not newname and is_vm_active(domain):
        raise ExecuteException('', 'domain %s is still running. plz stop it first.' % domain)

    # check backup pool path exist or not
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk backup pool can not be uus.')
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    disk_backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(disk_backup_dir):
        raise ExecuteException('', 'not exist disk %s backup dir %s' % (disk, disk_backup_dir))

    # check backup version exist or not
    history_file_path = '%s/history.json' % disk_backup_dir
    if not is_disk_backup_exist(domain, pool, disk, version):
        raise ExecuteException('', 'not exist disk %s backup version in history file %s' % (disk, history_file_path))

    with open(history_file_path, 'r') as f:
        history = load(f)

    full_version = get_full_version(domain, pool, disk, version)
    if newname:
        if newname is None or target is None:
            raise ExecuteException('', 'new disk name or target pool must be set.')

        disk_heler = K8sHelper('VirtualMachineDisk')
        if disk_heler.exist(newname):
            raise ExecuteException('', 'new disk %s has exist' % newname)

        disk_pool_info = get_pool_info_from_k8s(target)
        check_pool_active(disk_pool_info)

        if disk_pool_info['pooltype'] == 'uus':
            raise ExecuteException('', 'target pooltype must be localfs, nfs, glusterfs or vdiskfs.')

        if not os.path.exists(disk_pool_info['path']):
            raise ExecuteException('', 'not exist pool %s mount path %s.' % (target, disk_pool_info['path']))

        new_disk_dir = '%s/%s' % (disk_pool_info['path'], newname)
        if not os.path.exists(new_disk_dir):
            os.mkdir(new_disk_dir)

        disk_back_dir = '%s/%s/diskbackup' % (disk_backup_dir, full_version)
        backupRecord = history[full_version][version]
        current, file_to_delete = restore_snapshots_chain(disk_back_dir, backupRecord, new_disk_dir)

        write_config(newname, os.path.dirname(current), current, target, disk_pool_info['poolname'])
        disk_heler.create(newname, "volume", get_disk_info_to_k8s(disk_pool_info['poolname'], newname))
    else:
        disk_info = get_vol_info_from_k8s(disk)
        disk_pool_info = get_pool_info_from_k8s(disk_info['pool'])
        check_pool_active(disk_pool_info)

        cstor_disk_prepare(disk_info['poolname'], disk_info['disk'], disk_info['uni'])

        disk_specs = get_disks_spec(domain)
        vm_disks = {}
        for disk_path in disk_specs.keys():
            if disk_path.find('snapshots') < 0:
                disk_mn = os.path.basename(os.path.dirname(disk_path))
            else:
                disk_mn = os.path.basename(os.path.dirname(os.path.dirname(disk_path)))
            vm_disks[disk_mn] = disk_path
        if disk not in vm_disks.keys():
            raise ExecuteException('', 'domain not attach diak %s, can find disk %s used by domain %s xml.' % (
                disk, disk, domain))

        # do vm snapshots

        disk_back_dir = '%s/%s/diskbackup' % (disk_backup_dir, full_version)

        disk_dir = '%s/%s' % (disk_pool_info['path'], disk_info['disk'])
        # restore disk dir
        backupRecord = history[full_version][version]
        current, file_to_delete = restore_snapshots_chain(disk_back_dir, backupRecord, disk_dir)
        # change vm disk
        modofy_vm_disks(domain, {vm_disks[disk]: current})

        # change disk current
        change_vol_current(disk, current)

    for file in file_to_delete:
        runCmd('rm -f %s' % file)

    return current

def restoreDisk(params):
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(params.vol)

    if params.targetDomain:
        if not is_vm_exist(params.targetDomain):
            raise ExecuteException('', 'target domain %s will be attached new disk not set.')

    current = restore_vm_disk(params.domain, params.pool, params.vol, params.version, params.newname, params.target)

    # attach vm disk
    if params.targetDomain:
        attach_vm_disk(params.targetDomain, current)
    success_print("success restoreDisk.", {})


def backupVM(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    if not is_vm_exist(params.domain):
        raise ExecuteException('', 'domain %s is not exist. plz check it.' % params.domain)

    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)

    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], params.domain)
    history_file_path = '%s/history.json' % backup_dir
    if is_vm_backup_exist(params.domain, params.pool, params.version):
        raise ExecuteException('', 'domain %s has exist backup version %s in %s. plz check it.' % (
        params.domain, params.version, history_file_path))

    history = {}
    if os.path.exists(history_file_path):
        with open(history_file_path, 'r') as f:
            history = load(f)
    disk_full_version = None
    newestV = None
    if not params.full:
        if len(history.keys()) == 0:
            raise ExecuteException('', 'domain %s not exist full backup version %s in %s. plz check it.' % (
            params.domain, params.version, history_file_path))
        btime = 0.0
        for v in history.keys():
            for disk in history[v].keys():
                if history[v][disk]['time'] > btime:
                    btime = history[v][disk]['time']
                    newestV = v
        disk_full_version = {}
        for disk in history[newestV].keys():
            disk_full_version[disk] = history[newestV][disk]['full']

    disk_tags = {}
    disk_specs = get_disks_spec(params.domain)

    # do vm snapshots
    for disk_path in disk_specs.keys():
        if not params.all and disk_specs[disk_path] != 'vda':
            continue
        if disk_path.find('snapshots') < 0:
            disk_mn = os.path.basename(os.path.dirname(disk_path))
        else:
            disk_mn = os.path.basename(os.path.dirname(os.path.dirname(disk_path)))
        disk_info = get_vol_info_from_k8s(disk_mn)
        pool_info = get_pool_info_from_k8s(disk_info['pool'])
        check_pool_active(pool_info)

        if pool_info['pooltype'] == 'uus':
            if disk_specs[disk_path] == 'vda':
                raise ExecuteException('', 'uus disk %s is vm os disk, not support backup vm %s' % (
                    disk_path, params.domain))
            else:
                continue
        cstor_disk_prepare(disk_info['poolname'], disk_info['disk'], disk_info['uni'])
        disk_dir = '%s/%s' % (pool_info['path'], disk_info['disk'])
        if not os.path.exists(disk_dir):
            raise ExecuteException('', 'vm disk dir %s not exist, plz check it.' % disk_dir)
        disk_tags[disk_mn] = disk_specs[disk_path]

    # check domain all disk has full backup
    if not params.full:
        for disk in disk_full_version.keys():
            if disk not in disk_tags.keys():
                raise ExecuteException('', 'vm %s disk %s may be first attach, plz make full backup firstly.' % (params.domain, disk))

    # save vm xml file
    xml_file = '%s/%s.xml' % (backup_dir, params.version)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    op = Operation('virsh dumpxml %s > %s' % (params.domain, xml_file), {})
    op.execute()
    delete_vm_cdrom_file_in_xml(xml_file)

    # backup disk
    disk_version = {}
    for disk in disk_tags.keys():
        uuid = randomUUID().replace('-', '')
        disk_version[disk] = uuid
        if disk_full_version:
            backup_vm_disk(params.domain, params.pool, disk, uuid, params.full, disk_full_version[disk])
        else:
            backup_vm_disk(params.domain, params.pool, disk, uuid, params.full, None)

    history[params.version] = {}
    for disk in disk_version.keys():
        if disk_full_version:
            history[params.version][disk] = {
                'time': time.time(),
                'tag': disk_tags[disk],
                'version': disk_version[disk],
                'full': disk_full_version[disk]
            }
        else:
            history[params.version][disk] = {
                'time': time.time(),
                'tag': disk_tags[disk],
                'version': disk_version[disk],
                'full': disk_version[disk]
            }
        if newestV:
            history[params.version][disk]['vm_full'] = newestV
        else:
            history[params.version][disk]['vm_full'] = params.full
    with open(history_file_path, 'w') as f:
        dump(history, f)

    if params.remote:
        if is_remote_vm_backup_exist(params.domain, params.version, params.remote, params.port, params.username,
                                     params.password):
            raise ExecuteException('', 'domain %s has exist backup version %s in ftp server. plz check it.' % (
                params.domain, params.version))

        # history file
        history_file = '%s/history.json' % backup_dir
        with open(history_file, 'r') as f:
            history = load(f)

        ftp = FtpHelper(params.remote, params.port, params.username, params.password)

        ftp_history_file = '/%s/history.json' % params.domain

        if ftp.is_exist_file(ftp_history_file):
            ftp_history = ftp.get_json_file_data(ftp_history_file)
        else:
            ftp_history = {}

        # upload file
        fin = []
        record = history[params.version]
        try:
            for disk in record.keys():
                push_disk_backup(params.domain, params.pool, disk, record[disk]['version'], params.remote,
                                 params.port, params.username, params.password)
                fin.append(disk)
        except Exception, e:
            for disk in fin:
                delete_remote_disk_backup(params.domain, disk, record[disk]['version'], params.remote, params.port,
                                          params.username, params.password)
            for disk in record.keys():
                delete_disk_backup(params.domain, params.pool, disk, record[disk]['version'])

            del history[params.version]
            with open(history_file_path, 'w') as f:
                dump(history, f)
            logger.debug(traceback.format_exc())
            raise ExecuteException('', 'can not upload backup record to ftp server.')

        ftp_history[params.version] = history[params.version]
        with open('/tmp/history.json', 'w') as f:
            dump(ftp_history, f)
        ftp.upload_file("/tmp/history.json", '/%s' % params.domain)

    backup_helper = K8sHelper('VirtualMachineBackup')

    data = {
        'domain': params.domain,
        'pool': params.pool,
        'time': time.time(),
        'disk':  ''
    }


    if newestV:
        data['full'] = newestV
    else:
        data['full'] = params.full
    backup_helper.create(params.version, 'backup', data)

    success_print("success backupVM.", {})


def restoreVM(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)
    if not params.newname and is_vm_active(params.domain):
        raise ExecuteException('', 'vm %s is still active, plz stop it first.' % params.domain)

    # default backup path
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)

    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (params.pool, pool_info['path']))

    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], params.domain)
    history_file = '%s/history.json' % backup_dir
    if not is_vm_backup_exist(params.domain, params.pool, params.version):
        raise ExecuteException('', 'domain %s not has backup %s, location: %s.' % (
            params.domain, params.version, history_file))

    disk_version = {}
    with open(history_file, 'r') as f:
        history = load(f)
        record = history[params.version]
        for disk in record.keys():
            disk_version[disk] = record[disk]['version']

    disk_specs = get_disks_spec(params.domain)

    # be sure vm still use the disks in the backup record.
    vm_disks = []
    for disk_path in disk_specs.keys():
        if disk_path.find('snapshots') >= 0:
            vm_disk = os.path.basename(os.path.dirname(os.path.dirname(disk_path)))
        else:
            vm_disk = os.path.basename(os.path.dirname(disk_path))
        vm_disks.append(vm_disk)

    if params.all:
        for disk in disk_version.keys():
            if disk not in vm_disks:
                raise ExecuteException('', 'some disk in backup %s has not been attached in domain %s.' % (
                    dumps(disk_version), params.domain))

    # restore vm disk snapshot chain
    for disk in vm_disks:
        if not params.all and record[disk]['tag'] != 'vda':
            continue
        disk_info = get_vol_info_from_k8s(disk)
        cstor_disk_prepare(disk_info['poolname'], disk, disk_info['uni'])

    # restore vm disk snapshot chain
    disk_currents = {}
    for disk in disk_version.keys():
        if not params.all and record[disk]['tag'] != 'vda':
            continue
        if params.newname:
            newdisk = randomUUID().replace('-', '')
            current = restore_vm_disk(params.domain, params.pool, disk, disk_version[disk], newdisk, params.target)
        else:
            current = restore_vm_disk(params.domain, params.pool, disk, disk_version[disk], None, None)
        disk_currents[disk] = current
    if params.newname:
        # current.
        vm_xml_file = '%s/%s.xml' % (backup_dir, params.version)
        source_to_target = {}
        disk_specs = get_disks_spec_by_xml(vm_xml_file)
        for name in disk_version.keys():
            for disk_path in disk_specs.keys():
                if disk_path.find(name) >= 0:
                    source_to_target[disk_path] = disk_currents[name]
                    break
        define_and_restore_vm_disks(vm_xml_file, params.newname, source_to_target)

    success_print("success restoreVM.", {})


def delete_disk_backup(domain, pool, disk, version):
    # default backup path
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    full_version = get_full_version(domain, pool, disk, version)

    backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(backup_dir):
        raise ExecuteException('', 'disk %s not has backup %s, location: %s.' % (
            disk, version, backup_dir))

    history_file = '%s/history.json' % backup_dir
    if not os.path.exists(history_file):
        raise ExecuteException('', 'can not find disk %s backup record %s' % (disk, version))

    checksum_to_deletes = []
    with open(history_file, 'r') as f:
        history = load(f)
        for chain in history[full_version][version]['chains']:
            checksum_to_deletes.append(chain['checksum'])

    # be sure disk backup not used by other backup record.
    for v in history[full_version].keys():
        if v == version:
            continue
        chains = history[full_version][v]['chains']
        for chain in chains:
            if chain['checksum'] in checksum_to_deletes:
                checksum_to_deletes.remove(chain['checksum'])

    disk_backup_dir = '%s/%s/diskbackup' % (backup_dir, full_version)
    checksum_file = '%s/checksum.json' % disk_backup_dir
    if os.path.exists(checksum_file):
        with open(checksum_file, 'r') as f:
            checksums = load(f)
            for checksum in checksum_to_deletes:
                file_path = '%s/%s' % (disk_backup_dir, checksums[checksum])
                runCmd('rm -f %s' % file_path)
                del checksums[checksum]
        with open(checksum_file, 'w') as f:
            dump(checksums, f)

    with open(history_file, 'r') as f:
        history = load(f)
        del history[full_version][version]
        if len(history[full_version].keys()) == 0:
            runCmd('rm -rf %s/%s' % (backup_dir, full_version))
    with open(history_file, 'w') as f:
        dump(history, f)

def delete_vm_backup(domain, pool, version):
    # default backup path
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], domain)
    history_file_path = '%s/history.json' % backup_dir
    if not is_vm_backup_exist(domain, pool, version):
        raise ExecuteException('', 'domain %s not exist backup version %s in %s. plz check it.' % (
            domain, version, history_file_path))

    disk_version = {}
    with open(history_file_path, 'r') as f:
        history = load(f)
        record = history[version]
        for disk in record.keys():
            disk_version[disk] = record[disk]['version']
    for disk in disk_version.keys():
        delete_disk_backup(domain, pool, disk, disk_version[disk])

    op = Operation('rm -f %s.xml' % version, {})
    op.execute()
    if history['current'] == version:
        del history['current']
    del history[version]

    with open(history_file_path, 'w') as f:
        dump(history, f)

def deleteVMBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)
    delete_vm_backup(params.domain, params.pool, params.version)
    backup_helper = K8sHelper('VirtualMachineBackup')
    backup_helper.delete(params.version)
    success_print("success deleteVMBackup.", {})


def deleteVMDiskBackup(params):
    disk_heler = K8sHelper('VirtualMachineDisk')
    disk_heler.delete_lifecycle(params.vol)
    delete_disk_backup(params.domain, params.pool, params.vol, params.version)
    backup_helper = K8sHelper('VirtualMachineBackup')
    backup_helper.delete(params.version)
    success_print("success deleteVMDiskBackup.", {})


def deleteRemoteBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)
    # default backup path
    if params.vol:
        delete_remote_disk_backup(params.domain, params.vol, params.version, params.remote, params.port, params.username, params.password)
    else:
        delete_remote_vm_backup(params.domain, params.version, params.remote, params.port, params.username, params.password)

    success_print("success deleteRemoteBackup.", {})


def delete_remote_disk_backup(domain, disk, version, remote, port, username, password):
    if not remote or not port or not username or not password:
        raise ExecuteException('', 'ftp port, username, password must be set.')
    ftp = FtpHelper(remote, port, username, password)

    backup_dir = '/%s/diskbackup/%s' % (domain, disk)
    history_file = '%s/history.json' % backup_dir
    logger.debug('history_file: ' + history_file)
    if not ftp.is_exist_file(history_file):
        raise ExecuteException('',
                               'can not find disk %s backup record %s in ftp server' % (disk, version))
    history = ftp.get_json_file_data(history_file)
    if version not in history.keys():
        raise ExecuteException('',
                               'can not find disk %s backup record %s in ftp server' % (disk, version))
    full_version = get_full_version_by_history(disk, version, history)
    record = history[full_version][version]
    chains = record['chains']
    checksum_to_deletes = []
    for chain in chains:
        checksum_to_deletes.append(chain['checksum'])

    for v in history[full_version].keys():
        if v == version:
            continue
        chains = history[full_version][v]['chains']
        for chain in chains:
            if chain['checksum'] in checksum_to_deletes:
                checksum_to_deletes.remove(chain['checksum'])

    disk_backup_dir = '%s/%s/diskbackup' % (backup_dir, full_version)
    checksum_file = '%s/checksum.json' % disk_backup_dir
    checksums = ftp.get_json_file_data(checksum_file)
    logger.debug(checksums)
    logger.debug(checksum_to_deletes)
    for checksum in checksum_to_deletes:
        file_path = '%s/%s' % (disk_backup_dir, checksums[checksum])
        ftp.delete_file(file_path)
        del checksums[checksum]
    tmp_file = '/tmp/checksum.json'
    with open(tmp_file, 'w') as f:
        dump(checksums, f)
    ftp.upload_file(tmp_file, disk_backup_dir)

    del history[full_version][version]
    if len(history[full_version].keys()) == 0:
        del history[full_version]

    tmp_file = '/tmp/history.json'
    with open(tmp_file, 'w') as f:
        dump(history, f)
    ftp.upload_file(tmp_file, backup_dir)


def delete_remote_vm_backup(domain, version, remote, port, username, password):
    ftp = FtpHelper(remote, port, username, password)
    history_file = '/%s/history.json' % domain
    history = ftp.get_json_file_data(history_file)

    if not history or version not in history.keys():
        raise ExecuteException('', 'not exist vm %s backup record version %s in %s. ' % (
            domain, version, history_file))
    record = history[version]
    for disk in record.keys():
        if disk == 'current':
            continue
        delete_remote_disk_backup(domain, disk, record[disk]['version'], remote, port,
                                  username, password)
    history_file = '/%s/history.json' % domain
    history = ftp.get_json_file_data(history_file)
    del history[version]
    tmp_file = '/tmp/history.json'
    with open(tmp_file, 'w') as f:
        dump(history, f)
    ftp.upload_file(tmp_file, '/%s' % domain)

def pushVMBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)

    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (params.pool, pool_info['path']))

    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], params.domain)
    if is_remote_vm_backup_exist(params.domain, params.version, params.remote, params.port, params.username, params.password):
        raise ExecuteException('', 'domain %s has exist backup version %s in ftp server. plz check it.' % (
            params.domain, params.version))

    # history file
    history_file = '%s/history.json' % backup_dir
    with open(history_file, 'r') as f:
        history = load(f)

    ftp = FtpHelper(params.remote, params.port, params.username, params.password)

    ftp_history_file = '/%s/history.json' % params.domain

    if ftp.is_exist_file(ftp_history_file):
       ftp_history = ftp.get_json_file_data(ftp_history_file)
    else:
        ftp_history = {}

    # upload file
    if params.vol:
        push_disk_backup(params.domain, params.pool, params.vol, params.version, params.remote,
                         params.port, params.username, params.password)
    else:
        fin = []
        record = history[params.version]
        try:
            for disk in record.keys():
                push_disk_backup(params.domain, params.pool, disk, record[disk]['version'], params.remote,
                                 params.port, params.username, params.password)
                fin.append(disk)
        except:
            for disk in fin:
                delete_remote_disk_backup(params.domain, disk, record[disk]['version'], params.remote, params.port,
                                          params.username, params.password)
            raise ExecuteException('', 'can not upload backup record to ftp server.')

        ftp_history[params.version] = history[params.version]
        with open('/tmp/history.json', 'w') as f:
            dump(ftp_history, f)
        ftp.upload_file("/tmp/history.json", '/%s' % params.domain)
    success_print("success pushVMBackup.", {})


# def pushVMDiskBackup(params):
#     vm_heler = K8sHelper('VirtualMachine')
#     vm_heler.delete_lifecycle(params.domain)
#
#     push_disk_backup(params.domain, params.pool, params.vol, params.version, params.remote, params.port, params.username, params.password)
#     success_print("success pushVMDiskBackup.", {})


def push_disk_backup(domain, pool, disk, version, remote, port, username, password):
    if not port or not username or not password:
        raise ExecuteException('', 'ftp port, username, password must be set.')
    ftp = FtpHelper(remote, port, username, password)
    if is_remote_disk_backup_exist(domain, disk, version, remote, port, username, password):
        raise ExecuteException('', 'ftp server has exist vm %s backup record version %s. ' % (
            domain, version))

    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk %s is uus type, not support backup.' % disk)

    disk_backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(disk_backup_dir):
        os.makedirs(disk_backup_dir)
    history_file = '%s/history.json' % disk_backup_dir
    if not os.path.exists(history_file) or not is_disk_backup_exist(domain, pool, disk, version):
        raise ExecuteException('', 'not exist vm %s backup record version %s in %s. ' % (
            domain, version, history_file))
    full_version = None
    record = None
    with open(history_file, 'r') as f:
        history = load(f)
        for fv in history.keys():
            if fv == 'current':
                continue
            for v in history[fv].keys():
                if v == version:
                    full_version = fv
                    record = history[fv][version]
    if full_version is None or record is None:
        raise ExecuteException('', 'can not get domain %s right backup record version %s in %s. ' % (
            domain, version, history_file))

    remote_disk_dir = '/%s/diskbackup/%s' % (domain, disk)

    # history file
    ftp_history_file = '%s/history.json' % remote_disk_dir
    if ftp.is_exist_file(ftp_history_file):
        ftp.download_file(ftp_history_file, '/tmp/history.json')
        with open('/tmp/history.json', 'r') as f:
            ftp_history = load(f)
            ftp_history[full_version][version] = record
    else:
        ftp_history = {}
        ftp_history[full_version] = {}
        ftp_history[full_version][version] = record
    with open('/tmp/history.json', 'w') as f:
        dump(ftp_history, f)
    ftp.upload_file('/tmp/history.json', remote_disk_dir)

    # modify checksum file
    ftp_checksum_file = '%s/%s/diskbackup/checksum.json' % (remote_disk_dir, full_version)
    local_checksum_file = '%s/%s/diskbackup/checksum.json' % (disk_backup_dir, full_version)
    if ftp.is_exist_file(ftp_checksum_file):
        ftp.download_file(ftp_checksum_file, '/tmp/checksum.json')
        with open(local_checksum_file, 'r') as f1:
            local_checksum = load(f1)
            with open('/tmp/checksum.json', 'r') as f:
                remote_checksum = load(f)
    else:
        with open(local_checksum_file, 'r') as f1:
            local_checksum = load(f1)
            remote_checksum = {}

    for record in record['chains']:
        if record['checksum'] not in remote_checksum.keys():
            remote_checksum[record['checksum']] = local_checksum[record['checksum']]
            # upload disk file
            backup_file = '%s/%s/diskbackup/%s' % (
                disk_backup_dir, full_version, local_checksum[record['checksum']])
            ftp.upload_file(backup_file, '%s/%s/diskbackup' % (remote_disk_dir, full_version))
    with open('/tmp/checksum.json', 'w') as f:
        dump(remote_checksum, f)
    ftp.upload_file('/tmp/checksum.json', '%s/%s/diskbackup' % (remote_disk_dir, full_version))


def pullRemoteBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)
    # default backup path
    checksum_to_pull = []
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'can not find pool path %s' % pool_info['path'])
    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], params.domain)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    if params.vol:
        pull_disk_backup(params.domain, params.pool, params.vol, params.version, params.remote, params.port,
                         params.username, params.password)
    else:
        if not is_remote_vm_backup_exist(params.domain, params.version, params.remote, params.port, params.username, params.password):
            raise ExecuteException('', 'can not find vm backup record %s in ftp server %s.' % (params.version, params.remote))

        ftp = FtpHelper(params.remote, params.port, params.username, params.password)

        ftp_history_file = '/%s/history.json' % params.domain

        ftp_history = ftp.get_json_file_data(ftp_history_file)
        record = ftp_history[params.version]
        fin = []
        try:
            for disk in record.keys():
                pull_disk_backup(params.domain, params.pool, disk, record[disk]['version'], params.remote, params.port,
                                 params.username, params.password)
                fin.append(disk)
        except ExecuteException, e:
            for disk in fin:
                delete_disk_backup(params.domain, params.pool, disk, record[disk]['version'])
            raise e


        history_file = '%s/history.json' % backup_dir
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                history = load(f)
        else:
            history = {}

        history[params.version] = record
        with open(history_file, 'w') as f:
            dump(history, f)

    success_print("success pullRemoteBackup.", {})


def pull_disk_backup(domain, pool, disk, version, remote, port, username, password):
    # default backup path
    ftp = FtpHelper(remote, port, username, password)
    remote_backup_dir = '/%s/diskbackup/%s' % (domain, disk)
    remote_history_file = '%s/history.json' % remote_backup_dir
    remote_history = ftp.get_json_file_data(remote_history_file)

    full_version = get_full_version_by_history(disk, version, remote_history)
    if full_version not in remote_history.keys() and version not in remote_history[full_version].keys():
        raise ExecuteException('',
                               'can not find disk %s backup record %s in ftp server' % (disk, version))

    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'can not find pool path %s' % pool_info['path'])

    backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    history_file = '%s/history.json' % backup_dir
    if not os.path.exists('%s/%s/diskbackup' % (backup_dir, full_version)):
        os.makedirs('%s/%s/diskbackup' % (backup_dir, full_version))
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history = load(f)
        if full_version in history.keys() and version in history[full_version].keys():
            raise ExecuteException('', 'disk backup %s has exist in pool %s .' % (version, pool))
    else:
        history = {}

    remote_checksum = ftp.get_json_file_data('%s/%s/diskbackup/checksum.json' % (remote_backup_dir, full_version))
    record = remote_history[full_version][version]
    chains = record['chains']
    for chain in chains:
        df = '%s/%s/diskbackup/%s' % (remote_backup_dir, full_version, remote_checksum[chain['checksum']])
        ftp.download_file(df, '%s/%s/diskbackup/%s' % (backup_dir, full_version, remote_checksum[chain['checksum']]))

    if full_version not in history.keys():
        history[full_version] = {}
    history[full_version][version] = record

    with open(history_file, 'w') as f:
        dump(history, f)

    local_checksum_file = '%s/%s/diskbackup/checksum.json' % (backup_dir, full_version)
    if os.path.exists(local_checksum_file):
        with open(local_checksum_file, 'r') as f:
            local_checksum = load(f)
    else:
        local_checksum = {}

    for chain in chains:
        local_checksum[chain['checksum']] = remote_checksum[chain['checksum']]
    with open(local_checksum_file, 'w') as f:
        dump(local_checksum, f)

def clean_disk_backup(domain, pool, disk, versions):
    # check backup pool path exist or not
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk backup pool can not be uus.')
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    disk_backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], domain, disk)
    if not os.path.exists(disk_backup_dir):
        raise ExecuteException('', 'not exist disk %s backup dir %s' % (disk, disk_backup_dir))

    # check backup version exist or not
    history_file_path = '%s/history.json' % disk_backup_dir
    backup_helper = K8sHelper('VirtualMachineBackup')

    for version in versions:
        if not is_disk_backup_exist(domain, pool, disk, version):
            try:
                backup_helper.delete(version)
            except:
                pass

    disk_versions = get_disk_backup_version(domain, pool, disk)

    for version in disk_versions:
        if version not in versions:
            delete_disk_backup(domain, pool, disk, version)
            try:
                backup_helper.delete(version)
            except:
                pass

def clean_vm_backup(domain, pool, versions):
    # check backup pool path exist or not
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk backup pool can not be uus.')
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (pool, pool_info['path']))

    backup_dir = '%s/vmbackup/%s' % (pool_info['path'], domain)
    if not os.path.exists(backup_dir):
        raise ExecuteException('', 'not exist domain %s backup dir %s' % (domain, backup_dir))

    # check backup version exist or not
    backup_helper = K8sHelper('VirtualMachineBackup')

    history_file = '%s/history.json' % backup_dir
    with open(history_file, 'r') as f:
        history = load(history_file)
        for v in history.keys():
            if v not in versions:
                delete_vm_backup(domain, pool, v)
                try:
                    backup_helper.delete(v)
                except:
                    pass
        for v in versions:
            if v not in history.keys():
                try:
                    backup_helper.delete(v)
                except:
                    pass


def cleanBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    versions = params.version.split(',')
    if params.vol:
        clean_disk_backup(params.domain, params.pool, params.vol, versions)
    else:
        clean_vm_backup(params.domain, params.pool, versions)

    success_print("success cleanBackup", {})


def clean_disk_remote_backup(domain, pool, disk, versions, remote, port, username, password):
    backup_helper = K8sHelper('VirtualMachineBackup')

    for version in versions:
        if not is_remote_disk_backup_exist(domain, disk, version, remote, port, username, password):
            # try:
            #     backup_helper.delete(version)
            # except:
            #     pass
            pass

    disk_versions = get_disk_backup_version(domain, pool, disk)

    for version in disk_versions:
        if version not in versions:
            delete_remote_disk_backup(domain, disk, version, remote, port, username, password)
            # try:
            #     backup_helper.delete(version)
            # except:
            #     pass
            pass


def clean_vm_remote_backup(domain, versions, remote, port, username, password):
    ftp = FtpHelper(remote, port, username, password)
    remote_backup_dir = '/%s/diskbackup/%s' % domain
    remote_history_file = '%s/history.json' % remote_backup_dir
    remote_history = ftp.get_json_file_data(remote_history_file)
    if remote_history is None:
        remote_history = {}

    for v in remote_history.keys():
        if v not in versions:
            delete_remote_vm_backup(domain, v, remote, port, username, password)


def cleanRemoteBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    versions = params.version.split(',')
    if params.vol:
        clean_disk_remote_backup(params.domain, params.pool, params.vol, versions, params.remote, params.port, params.username, params.password)
    else:
        clean_vm_remote_backup(params.domain, params.pool, versions, params.remote, params.port, params.username, params.password)

    success_print("success cleanRemoteBackup", {})


def scanBackup(params):
    vm_heler = K8sHelper('VirtualMachine')
    vm_heler.delete_lifecycle(params.domain)

    # check backup pool path exist or not
    pool_info = get_pool_info_from_k8s(params.pool)
    check_pool_active(pool_info)

    if pool_info['pooltype'] == 'uus':
        raise ExecuteException('', 'disk backup pool can not be uus.')
    if not os.path.exists(pool_info['path']):
        raise ExecuteException('', 'pool %s path %s not exist. plz check it.' % (params.pool, pool_info['path']))

    backup_helper = K8sHelper('VirtualMachineBackup')

    if params.vol:
        backup_dir = '%s/vmbackup/%s/diskbackup/%s' % (pool_info['path'], params.domain, params.vol)
        if not os.path.exists(backup_dir):
            raise ExecuteException('', 'not exist domain %s backup dir %s' % (params.domain, backup_dir))

        # check backup version exist or not
        history_file = '%s/history.json' % backup_dir
        with open(history_file, 'r') as f:
            history = load(history_file)
            disk_full_versions = get_disk_backup_full_version(params.domain, params.pool, params.vol)

            for fv in disk_full_versions:
                for v in history[fv].keys():
                    if not backup_helper.exist(v):
                        data = {
                            'domain': params.domain,
                            'disk': params.vol,
                            'pool': params.pool,
                            'full': fv,
                            'time': history[fv][v]['time']
                        }
                        backup_helper.create(v, 'backup', data)
    else:
        backup_dir = '%s/vmbackup/%s' % (pool_info['path'], params.domain)
        if not os.path.exists(backup_dir):
            raise ExecuteException('', 'not exist domain %s backup dir %s' % (params.domain, backup_dir))

        # check backup version exist or not
        history_file = '%s/history.json' % backup_dir
        with open(history_file, 'r') as f:
            history = load(history_file)
            for v in history.keys():
                if not backup_helper.exist(v):
                    time = ''
                    for disk in history[v].keys():
                        time = history[v][disk]['time']
                    data = {
                        'domain': params.domain,
                        'disk': '',
                        'pool': params.pool,
                        'full': history[v][disk]['vm_full'],
                        'time': time,
                    }
                    backup_helper.create(v, 'backup', data)
    success_print("success scanBackup", {})

def showDiskPool(params):
    prepare_info = get_disk_prepare_info_by_path(params.path)
    pool_info = get_pool_info_from_k8s(prepare_info['pool'])
    success_print("success show pool info by disk path", pool_info)


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
        raise ExecuteException('', 'can not get right disk info from k8s by path: %s. less info' % path)
    lines = output.splitlines()
    columns = lines[0].split()
    if len(columns) != 5:
        logger.debug(columns)
        raise ExecuteException('', 'can not get right disk info from k8s by path: %s. less info' % path)
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
    print get_node_name_by_node_ip('172.16.1.25')
    # print get_disk_prepare_info_by_path('/var/lib/libvirt/cstor/39829673ec934c2786b7715a96a7d878/39829673ec934c2786b7715a96a7d878/ff8538567f1a4ec8ab0257e5b2ece4b3/30ca01637b444a0c9c9e3c0adcd3e364')
    # print get_disks_spec('vm006')
    # print get_disk_prepare_info_by_path('/var/lib/libvirt/cstor/1709accf174vccaced76b0dbfccdev/1709accf174vccaced76b0dbfccdev/vm003migratevmdisk2/snapshots/vm003migratevmdisk2.1')
    # prepare_disk_by_path(
    #     '/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    # prepare_disk_by_metadataname('vm00aadd6coddpdssdn')
    # release_disk_by_path('/var/lib/libvirt/cstor/1709accdd174caced76b0dbfccdev/1709accdd174caced76b0dbfccdev/vm00aadd6coddpdssdn/vm00aadd6coddpdssdn')
    # release_disk_by_metadataname('vm00aadd6coddpdssdn')

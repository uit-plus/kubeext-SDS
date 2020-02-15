import argparse

from operation import *

from utils import logger
from utils.exception import ConditionException

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

SUPPORT_STORAGE_TYPE = ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]

# os.putenv('LANG', 'en_US.UTF-8')

def execute(f_name, params):
    moudle = __import__('operation')
    func = getattr(moudle, f_name)
    try:
        check(f_name, params)
        func(params)
    except ExecuteException, e:
        logger.debug(f_name)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        error_print(400, "error occur while %s. %s" % (f_name, e.message))
    except Exception:
        logger.debug(f_name)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        error_print(300, "error occur while %s. traceback: %s" % (f_name, traceback.format_exc()))


def check(f_name, args):
    check_storage_type(args)
    check_pool(f_name, args)


def check_storage_type(args):
    if hasattr(args, 'type') and args.type not in SUPPORT_STORAGE_TYPE:
        error_print(100, "unsupported value type: %s" % args.type)


def check_pool_active(info):
    if info['pooltype'] == 'uus':
        cstor = get_cstor_pool_info(info['poolname'])
        result = {
            "pooltype": info['pooltype'],
            "pool": info['pool'],
            "poolname": info['poolname'],
            "capacity": cstor["data"]["total"],
            "autostart": "no",
            "path": cstor["data"]["url"],
            "state": cstor["data"]["status"],
            "uuid": randomUUID(),
            "content": 'vmd'
        }
    else:
        result = get_pool_info(info['poolname'])
        if is_pool_started(info['poolname']):
            result['state'] = "active"
        else:
            result['state'] = "inactive"
        result['content'] = info["content"]
        result["pooltype"] = info["pooltype"]
        result["pool"] = info["pool"]
        result["poolname"] = info["poolname"]

    # update pool
    if cmp(info, result) != 0:
        k8s = K8sHelper('VirtualMahcinePool')
        try:
            k8s.update(info['pool'], 'pool', result)
        except:
            pass

    if result['state'] != 'active':
        error_print(221, 'pool is not active, please run "startPool" first')


def get_cstor_pool_info(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    result = op.execute()
    return result


# check pool type, if pool type not match, stop delete pool
def check_pool_type(args):
    try:
        if not hasattr(args, 'type'):
            return
        if not hasattr(args, 'pool'):
            return
        pool_info = get_pool_info_from_k8s(args.pool)
        if pool_info['pooltype'] == args.type:
            return
        else:
            error_print(221, "check_pool_type, pool type is not match. given is %s, actual is %s" % (
            args.type, pool_info['pooltype']))
    except ExecuteException:
        logger.debug(traceback.format_exc())
        error_print(202, "check_pool_type, cannot get pool info from k8s.")


def check_pool(f_name, args):
    try:
        if f_name == 'cloneDisk':
            return
        if not hasattr(args, 'type'):
            return
        if not hasattr(args, 'pool'):
            return
        if f_name == 'createPool':
            if args.type != 'uus':
                if is_pool_exists(args.uuid):
                    raise ConditionException(201, "virsh pool %s has exist" % args.uuid)
            if is_cstor_pool_exist(args.uuid):
                raise ConditionException(204, "cstor pool %s not exist" % args.uuid)
        else:
            if f_name == 'deletePool':
                # if pool is not create successful, delete it from k8s.
                helper = K8sHelper("VirtualMahcinePool")
                pool_info = helper.get_data(args.pool, "pool")
                if pool_info is None:
                    helper.delete(args.pool)
                    success_print("delete pool %s successful." % args.pool, {})

            check_pool_type(args)
            pool_info = get_pool_info_from_k8s(args.pool)
            pool = pool_info['poolname']
            if not is_cstor_pool_exist(pool):
                raise ConditionException(204, "cstor pool %s not exist" % pool)
            if args.type != 'uus':
                if not is_pool_exists(pool):
                    raise ConditionException(203, "virsh pool %s not exist" % pool)
    except ExecuteException, e1:
        logger.debug(traceback.format_exc())
        error_print(202, "check_pool, cannot get pool info. %s" % e1.message)
    except ConditionException, e2:
        logger.debug(traceback.format_exc())
        error_print(e2.code, e2.msg)


def is_cstor_pool_exist(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    cstor = op.execute()
    if cstor["result"]["code"] == 0:
        return True
    else:
        return False


def is_cstor_disk_exist(pool, diskname):
    op = Operation("cstor-cli vdisk-show", {"poolname": pool, "name": diskname}, with_result=True)
    cstor = op.execute()
    if cstor["result"]["code"] == 0:
        return True
    return False


def is_virsh_disk_exist(pool, diskname):
    pool_info = get_pool_info(pool)
    if os.path.isdir('%s/%s' % (pool_info['path'], diskname)):
        return True
    return False


def check_virsh_disk_exist(pool, diskname):
    pool_info = get_pool_info(pool)
    if os.path.isdir('%s/%s' % (pool_info['path'], diskname)):
        error_print(207, "virsh disk %s is in pool %s" % (diskname, pool))


def check_virsh_disk_not_exist(pool, diskname):
    pool_info = get_pool_info(pool)
    if not os.path.isdir('%s/%s' % (pool_info['path'], diskname)):
        error_print(209, "virsh disk %s is not in pool %s" % (diskname, pool))


def check_virsh_disk_snapshot_exist(pool, diskname, snapshot):
    pool_info = get_pool_info(pool)
    if os.path.exists('%s/%s/snapshots/%s' % (pool_info['path'], diskname, snapshot)):
        error_print(209, "virsh disk snapshot %s is in volume %s" % (snapshot, diskname))


def check_virsh_disk_snapshot_not_exist(pool, diskname, snapshot):
    pool_info = get_pool_info(pool)
    if not os.path.exists('%s/%s/snapshots/%s' % (pool_info['path'], diskname, snapshot)):
        error_print(209, "virsh disk snapshot %s is not in volume %s" % (snapshot, diskname))


def check_cstor_disk_exist(pool, diskname):
    if is_cstor_disk_exist(pool, diskname):
        error_print(210, "cstor disk %s is in pool %s" % (diskname, pool))


def check_cstor_disk_not_exist(pool, diskname):
    if not is_cstor_disk_exist(pool, diskname):
        error_print(212, "cstor disk %s is not in pool %s" % (diskname, pool))


def check_virsh_disk_size(pool, vol, size):
    if get_volume_size(pool, vol) >= int(size):
        error_print(213, "new disk size must larger than the old size.")


def check_cstor_snapshot_exist(pool, vol, snapshot):
    op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
    ssInfo = op.execute()
    if ssInfo['result']['code'] == 0:
        error_print(214, "snapshot %s exists." % snapshot)


def check_cstor_snapshot_not_exist(pool, vol, snapshot):
    op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
    ssInfo = op.execute()
    if ssInfo['result']['code'] != 0:
        error_print(216, "snapshot %s not exists." % snapshot)


def createPoolParser(args):
    if args.type != "uus":
        if args.content is None:
            error_print(100, "less arg, content must be set")
        if args.content not in ["vmd", "vmdi", "iso"]:
            error_print(100, "less arg, content just can be vmd, vmdi, iso")

    execute('createPool', args)


def deletePoolParser(args):
    execute('deletePool', args)


def startPoolParser(args):
    if args.type == "uus":
        error_print(500, "not support operation for uus or vdiskfs")
    execute('startPool', args)


def autoStartPoolParser(args):
    if args.type == "uus":
        error_print(500, "not support operation for uus or vdiskfs")

    execute('autoStartPool', args)


def stopPoolParser(args):
    if args.type == "uus":
        error_print(500, "not support operation for uus or vdiskfs")

    execute('stopPool', args)


def showPoolParser(args):
    execute('showPool', args)


def createDiskParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type != "uus":
        if args.format is None:
            error_print(100, "less arg, format must be set")
        check_pool_active(pool_info)
        check_virsh_disk_exist(pool, args.vol)

    execute('createDisk', args)


def deleteDiskParser(args):
    try:
        helper = K8sHelper("VirtualMachineDisk")
        disk_info = helper.get_data(args.vol, "volume")
        if disk_info is None:
            helper.delete(args.vol)
            success_print("delete disk %s successful." % args.vol, {})
    except ExecuteException, e:
        error_print(400, e.message)
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(pool, args.vol)
    else:
        check_pool_active(pool_info)
        check_virsh_disk_not_exist(pool, args.vol)
    execute('deleteDisk', args)


def resizeDiskParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(pool, args.vol)
    else:
        check_pool_active(pool_info)
        check_virsh_disk_not_exist(pool, args.vol)
        check_virsh_disk_size(pool, args.vol, args.capacity)

    execute('resizeDisk', args)


def cloneDiskParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    # pool = pool_info['poolname']
    try:
        disk_info = get_vol_info_from_k8s(args.newname)
        error_print(500, "vol %s has exist in k8s." % args.newname)
    except ExecuteException:
        pass

    # check cstor disk
    # check_cstor_disk_not_exist(pool, args.vol)
    # if args.type != "uus":
    #     check_pool_active(pool_info)
        # check_virsh_disk_not_exist(pool, args.vol)
        # check_virsh_disk_exist(pool, args.newname)

    execute('cloneDisk', args)

def registerDiskToK8sParser(args):
    execute('registerDiskToK8s', args)

def rebaseDiskSnapshotParser(args):
    execute('rebaseDiskSnapshot', args)

def showDiskParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    # check cstor disk
    check_cstor_disk_not_exist(pool, args.vol)
    if args.type != "uus":
        check_virsh_disk_not_exist(pool, args.vol)

    execute('showDisk', args)


def prepareDiskParser(args):
    execute('prepareDisk', args)


def releaseDiskParser(args):
    execute('releaseDisk', args)


def showDiskSnapshotParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    # check cstor disk
    check_cstor_disk_not_exist(pool, args.vol)
    if args.type != "uus":
        check_virsh_disk_snapshot_not_exist(pool, args.vol, args.name)

    execute('showDiskSnapshot', args)


def createExternalSnapshotParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type == "uus":
        pass
    else:
        if args.format is None:
            error_print(100, "less arg, format must be set")
        check_pool_active(pool_info)
        check_virsh_disk_snapshot_exist(pool, args.vol, args.name)

        disk_dir = '%s/%s' % (get_pool_info(pool)['path'], args.vol)
        config_path = '%s/config.json' % disk_dir
        with open(config_path, "r") as f:
            config = load(f)
        if not os.path.isfile(config['current']):
            error_print(100, "can not find vol current %s." % config['current'])
        if os.path.isfile('%s/snapshots/%s' % (disk_dir, args.name)):
            error_print(100, "snapshot file has exist")

    execute('createExternalSnapshot', args)


def revertExternalSnapshotParser(args):
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type == "uus":
        pass
    else:
        if args.format is None:
            error_print(100, "less arg, format must be set")

        check_pool_active(pool_info)
        check_virsh_disk_snapshot_not_exist(pool, args.vol, args.name)

        disk_dir = '%s/%s' % (get_pool_info(pool)['path'], args.vol)
        config_path = '%s/config.json' % disk_dir
        with open(config_path, "r") as f:
            config = load(f)

        if not os.path.isfile(config['current']):
            error_print(100, "can not find current file")
    execute('revertExternalSnapshot', args)


def deleteExternalSnapshotParser(args):
    try:
        helper = K8sHelper("VirtualMachineDiskSnapshot")
        ss_info = helper.get_data(args.name, "volume")
        if ss_info is None:
            helper.delete(args.name)
            success_print("delete snapshot %s successful." % args.name, {})
    except ExecuteException, e:
        error_print(400, e.message)
    pool_info = get_pool_info_from_k8s(args.pool)
    pool = pool_info['poolname']
    if args.type == "uus":
        pass
    else:
        check_pool_active(pool_info)
        check_virsh_disk_snapshot_not_exist(pool, args.vol, args.name)

        disk_dir = '%s/%s' % (get_pool_info(pool)['path'], args.vol)
        ss_path = '%s/snapshots/%s' % (disk_dir, args.name)
        if not os.path.isfile(ss_path):
            error_print(100, "snapshot file not exist")

    execute('deleteExternalSnapshot', args)


def updateDiskCurrentParser(args):
    if args.type == "uus":
        pass
    else:
        for current in args.current:
            if not os.path.isfile(current):
                error_print(100, "disk current path %s not exists!" % current)

    execute('updateDiskCurrent', args)


def customizeParser(args):
    execute('customize', args)


def createDiskFromImageParser(args):
    pool_info = get_pool_info_from_k8s(args.targetPool)
    pool = pool_info['poolname']
    check_pool_active(pool_info)

    execute('createDiskFromImage', args)


def migrateParser(args):
    if not re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$', args.ip):
        error_print(100, "ip is not right")
    execute('migrate', args)

def migrateDiskParser(args):
    execute('migrateDisk', args)

def migrateVMDiskParser(args):
    execute('migrateVMDisk', args)

def modifyVMParser(args):
    execute('modifyVM', args)

def exportVMParser(args):
    execute('exportVM', args)

def backupVMParser(args):
    execute('backupVM', args)

def restoreVMParser(args):
    execute('restoreVM', args)

# --------------------------- cmd line parser ---------------------------------------
parser = argparse.ArgumentParser(prog="kubesds-adm", description="All storage adaptation tools")

subparsers = parser.add_subparsers(help="sub-command help")

# -------------------- add createPool cmd ----------------------------------
parser_create_pool = subparsers.add_parser("createPool", help="createPool help")
parser_create_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")

parser_create_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                help="storage pool name to delete")

# localfs, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--url", required=True, metavar="[URL]", type=str,
                                help="storage pool create location, only for uus")

# set autostart
parser_create_pool.add_argument("--autostart", metavar="[AUTOSTART]", type=bool, nargs='?', const=True,
                                help="if autostart, pool will set autostart yes after create pool")

# set content
parser_create_pool.add_argument("--content", metavar="[CONTENT]", type=str,
                                help="pool content")

# uus and nfs only
parser_create_pool.add_argument("--opt", metavar="[OPT]", type=str,
                                help="uus require or nfs mount options, only for uus and nfs")

# nfs and glusterfs only
parser_create_pool.add_argument("--uuid", metavar="[UUID]", type=str,
                                help="nfs or glusterfs poolname when use cstor-cli")

# set default func
parser_create_pool.set_defaults(func=createPoolParser)

# -------------------- add deletePool cmd ----------------------------------
parser_delete_pool = subparsers.add_parser("deletePool", help="deletePool help")
parser_delete_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")

parser_delete_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                help="storage pool name to delete")
# set default func
parser_delete_pool.set_defaults(func=deletePoolParser)

# -------------------- add startPool cmd ----------------------------------
parser_start_pool = subparsers.add_parser("startPool", help="startPool help")
parser_start_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")

parser_start_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool name to delete")
# set default func
parser_start_pool.set_defaults(func=startPoolParser)

# -------------------- add autoStartPool cmd ----------------------------------
parser_autostart_pool = subparsers.add_parser("autoStartPool", help="autoStartPool help")
parser_autostart_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                   help="storage pool type to use")

parser_autostart_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                   help="storage pool name to autostart")
parser_autostart_pool.add_argument("--disable", metavar="[DISABLE]", type=bool, nargs='?', const=True,
                                   help="disable autostart")

# set default func
parser_autostart_pool.set_defaults(func=autoStartPoolParser)

# -------------------- add stopPool cmd ----------------------------------
parser_stop_pool = subparsers.add_parser("stopPool", help="stopPool help")
parser_stop_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")

parser_stop_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                              help="storage pool name to stop")
# set default func
parser_stop_pool.set_defaults(func=stopPoolParser)

# -------------------- add showPool cmd ----------------------------------
parser_show_pool = subparsers.add_parser("showPool", help="showPool help")
parser_show_pool.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")

parser_show_pool.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                              help="storage pool name to show")
# set default func
parser_show_pool.set_defaults(func=showPoolParser)

# -------------------- add createDisk cmd ----------------------------------
parser_create_disk = subparsers.add_parser("createDisk", help="createDisk help")
parser_create_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="disk type to use")
parser_create_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                help="storage pool to use")

parser_create_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                help="volume name to use")

# will transfer to --size when type in uus, nfs or glusterfs
parser_create_disk.add_argument("--capacity", required=True, metavar="[CAPACITY]", type=str,
                                help="capacity is the size of the volume to be created, as a scaled integer (see NOTES above), defaulting to bytes")
parser_create_disk.add_argument("--format", metavar="[raw|bochs|qcow|qcow2|vmdk|qed]", type=str,
                                help="format is used in file based storage pools to specify the volume file format to use; raw, bochs, qcow, qcow2, vmdk, qed.")

# set default func
parser_create_disk.set_defaults(func=createDiskParser)

# -------------------- add deleteDisk cmd ----------------------------------
parser_delete_disk = subparsers.add_parser("deleteDisk", help="deleteDisk help")
parser_delete_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")
parser_delete_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_delete_disk.set_defaults(func=deleteDiskParser)

# -------------------- add resizeDisk cmd ----------------------------------
parser_resize_disk = subparsers.add_parser("resizeDisk", help="resizeDisk help")
parser_resize_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")
parser_resize_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_resize_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                help="volume name to use")
parser_resize_disk.add_argument("--capacity", required=True, metavar="[CAPACITY]", type=str,
                                help="new volume capacity to use")
parser_resize_disk.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="new volume capacity to use")
# set default func
parser_resize_disk.set_defaults(func=resizeDiskParser)

# -------------------- add cloneDisk cmd ----------------------------------
parser_clone_disk = subparsers.add_parser("cloneDisk", help="cloneDisk help")
parser_clone_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")
parser_clone_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_clone_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="volume name to use")
parser_clone_disk.add_argument("--newname", required=True, metavar="[NEWNAME]", type=str,
                               help="new volume name to use")
parser_clone_disk.add_argument("--format", required=True, metavar="[FORMAT]", type=str,
                               help="format to use")
# set default func
parser_clone_disk.set_defaults(func=cloneDiskParser)

# -------------------- add registerDiskToK8s cmd ----------------------------------
parser_register_disk = subparsers.add_parser("registerDiskToK8s", help="register disk to k8s help")
parser_register_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_register_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="volume name to use")
# set default func
parser_register_disk.set_defaults(func=registerDiskToK8sParser)

# -------------------- add rebaseDiskSnapshot cmd ----------------------------------
parser_rebase_snapshot = subparsers.add_parser("rebaseDiskSnapshot", help="rebase disk snapshot help")
parser_rebase_snapshot.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_rebase_snapshot.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="volume name to use")
# set default func
parser_rebase_snapshot.set_defaults(func=rebaseDiskSnapshotParser)

# -------------------- add prepareDisk cmd ----------------------------------
parser_prepare_disk = subparsers.add_parser("prepareDisk", help="prepareDisk help")
parser_prepare_disk.add_argument("--domain", metavar="[DOMAIN]", type=str,
                                 help="storage pool to use")
parser_prepare_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                 help="volume name to use")
parser_prepare_disk.add_argument("--path", metavar="[PATH]", type=str,
                                 help="volume uni to use")
# set default func
parser_prepare_disk.set_defaults(func=prepareDiskParser)

# -------------------- add releaseDisk cmd ----------------------------------
parser_release_disk = subparsers.add_parser("releaseDisk", help="releaseDisk help")
parser_release_disk.add_argument("--domain", metavar="[DOMAIN]", type=str,
                                 help="domain to use")
parser_release_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                 help="volume name to use")
parser_release_disk.add_argument("--path", metavar="[PATH]", type=str,
                                 help="volume path to use")
# set default func
parser_release_disk.set_defaults(func=releaseDiskParser)

# -------------------- add showDisk cmd ----------------------------------
parser_show_disk = subparsers.add_parser("showDisk", help="showDisk help")
parser_show_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")
parser_show_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                              help="storage pool to use")
parser_show_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                              help="volume name to use")
# set default func
parser_show_disk.set_defaults(func=showDiskParser)

# -------------------- add showDiskSnapshot cmd ----------------------------------
parser_show_disk_snapshot = subparsers.add_parser("showDiskSnapshot", help="showDiskSnapshot help")
parser_show_disk_snapshot.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                       help="storage pool type to use")
parser_show_disk_snapshot.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                       help="storage pool to use")
parser_show_disk_snapshot.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                       help="volume name to use")
parser_show_disk_snapshot.add_argument("--name", required=True, metavar="[NAME]", type=str,
                                       help="volume snapshot name")
# set default func
parser_show_disk_snapshot.set_defaults(func=showDiskSnapshotParser)

# -------------------- add createExternalSnapshot cmd ----------------------------------
parser_create_ess = subparsers.add_parser("createExternalSnapshot", help="createExternalSnapshot help")
parser_create_ess.add_argument("--type", required=True, metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")
parser_create_ess.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_create_ess.add_argument("--name", required=True, metavar="[NAME]", type=str,
                               help="volume snapshot name to use")
parser_create_ess.add_argument("--format", required=True, metavar="[FORMAT]", type=str,
                               help="disk format to use")
parser_create_ess.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="disk current file to use")
parser_create_ess.add_argument("--domain", metavar="[domain]", type=str,
                               help="domain")
# set default func
parser_create_ess.set_defaults(func=createExternalSnapshotParser)

# -------------------- add revertExternalSnapshot cmd ----------------------------------
parser_revert_ess = subparsers.add_parser("revertExternalSnapshot", help="revertExternalSnapshot help")
parser_revert_ess.add_argument("--type", required=True, metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")
parser_revert_ess.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_revert_ess.add_argument("--name", required=True, metavar="[NAME]", type=str,
                               help="volume snapshot name to use")
parser_revert_ess.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="disk current file to use")
parser_revert_ess.add_argument("--format", required=True, metavar="[FORMAT]", type=str,
                               help="disk format to use")
parser_revert_ess.add_argument("--domain", metavar="[domain]", type=str,
                               help="domain")
# set default func
parser_revert_ess.set_defaults(func=revertExternalSnapshotParser)

# -------------------- add deleteExternalSnapshot cmd ----------------------------------
parser_delete_ess = subparsers.add_parser("deleteExternalSnapshot", help="deleteExternalSnapshot help")
parser_delete_ess.add_argument("--type", required=True, metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")
parser_delete_ess.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                               help="storage pool to use")
parser_delete_ess.add_argument("--name", required=True, metavar="[NAME]", type=str,
                               help="volume snapshot name to use")
parser_delete_ess.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                               help="disk current file to use")
parser_delete_ess.add_argument("--domain", metavar="[domain]", type=str,
                               help="domain")
# set default func
parser_delete_ess.set_defaults(func=deleteExternalSnapshotParser)

# -------------------- add updateDiskCurrent cmd ----------------------------------
parser_upodate_current = subparsers.add_parser("updateDiskCurrent", help="updateDiskCurrent help")
parser_upodate_current.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                    help="storage pool type to use")
parser_upodate_current.add_argument("--current", required=True, metavar="[CURRENT]", type=str, nargs='*',
                                    help="disk current file to use")
# set default func
parser_upodate_current.set_defaults(func=updateDiskCurrentParser)

# -------------------- add customize cmd ----------------------------------
parser_customize = subparsers.add_parser("customize", help="customize help")
parser_customize.add_argument("--add", required=True, metavar="[ADD]", type=str,
                              help="storage pool type to use")
parser_customize.add_argument("--user", required=False, metavar="[USER]", type=str,
                              help="disk current file to use")
parser_customize.add_argument("--password", required=False, metavar="[PASSWORD]", type=str,
                              help="disk current file to use")
parser_customize.add_argument("--ssh_inject", required=False, metavar="[SSH_INJECT]", type=str,
                              help="disk ssh-inject")
# set default func
parser_customize.set_defaults(func=customizeParser)

# -------------------- add createDiskFromImage cmd ----------------------------------
parser_create_disk_from_image = subparsers.add_parser("createDiskFromImage", help="createDiskFromImage help")
parser_create_disk_from_image.add_argument("--type", required=True, metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
                                           help="storage pool type to use")
parser_create_disk_from_image.add_argument("--name", required=True, metavar="[name]", type=str,
                                           help="new disk name to use")
parser_create_disk_from_image.add_argument("--targetPool", required=True, metavar="[targetPool]", type=str,
                                           help="storage pool to use")
parser_create_disk_from_image.add_argument("--source", required=True, metavar="[source]", type=str,
                                           help="disk source to use")
parser_create_disk_from_image.add_argument("--full_copy", metavar="[full_copy]", type=bool, nargs='?', const=True,
                                           help="if full_copy, new disk will be created by snapshot")
# set default func
parser_create_disk_from_image.set_defaults(func=createDiskFromImageParser)

# -------------------- add migrate cmd ----------------------------------
parser_migrate = subparsers.add_parser("migrate", help="migrate help")
parser_migrate.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to migrate")
parser_migrate.add_argument("--ip", required=True, metavar="[IP]", type=str,
                            help="storage pool type to use")
parser_migrate.add_argument("--offline", metavar="[OFFLINE]", type=bool, nargs='?', const=True,
                            help="support migrate offline")
# set default func
parser_migrate.set_defaults(func=migrateParser)

# -------------------- add migrateDisk cmd ----------------------------------
parser_migrate_disk = subparsers.add_parser("migrateDisk", help="migrate disk help")
parser_migrate_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                            help="vol to migrate")
parser_migrate_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                            help="target storage pool to use")
# set default func
parser_migrate_disk.set_defaults(func=migrateDiskParser)

# -------------------- add migrateVMDisk cmd ----------------------------------
parser_migrate_vm_disk = subparsers.add_parser("migrateVMDisk", help="migrateVMDisk help")
parser_migrate_vm_disk.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to migrate")
parser_migrate_vm_disk.add_argument("--ip", required=True, metavar="[IP]", type=str,
                            help="storage pool type to use")
parser_migrate_vm_disk.add_argument("--migratedisks", required=True, metavar="[MIGRATEDISKS]", type=str,
                            help="vol opt to migrate")
# parser_migrate_vm_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
#                             help="target storage pool to use")
# set default func
parser_migrate_vm_disk.set_defaults(func=migrateVMDiskParser)

# -------------------- add migrateVMDisk cmd ----------------------------------
parser_modify_vm = subparsers.add_parser("modifyVM", help="modifyVM help")
parser_modify_vm.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to migrate")
# parser_migrate_vm_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
#                             help="target storage pool to use")
# set default func
parser_modify_vm.set_defaults(func=modifyVMParser)


# -------------------- add exportVM cmd ----------------------------------
parser_export_vm = subparsers.add_parser("exportVM", help="exportVM help")
parser_export_vm.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to export")
parser_export_vm.add_argument("--path", required=True, metavar="[PATH]", type=str,
                            help="vm disk file to export")
# set default func
parser_export_vm.set_defaults(func=exportVMParser)


# -------------------- add backupVM cmd ----------------------------------
parser_backup_vm = subparsers.add_parser("backupVM", help="backupVM help")
parser_backup_vm.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to export")
parser_backup_vm.add_argument("--remote", required=True, metavar="[REMOTE]", type=str,
                            help="backup vm to remote server.")
# set default func
parser_backup_vm.set_defaults(func=backupVMParser)


# -------------------- add restoreVM cmd ----------------------------------
parser_restore_vm = subparsers.add_parser("restoreVM", help="restoreVM help")
parser_restore_vm.add_argument("--domain", required=True, metavar="[DOMAIN]", type=str,
                            help="vm domain to export")
# set default func
parser_restore_vm.set_defaults(func=restoreVMParser)

try:
    args = parser.parse_args()
    args.func(args)
except TypeError:
    # print"argument number not enough"
    logger.debug(traceback.format_exc())

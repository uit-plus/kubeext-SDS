import argparse
from operation import *

from utils import logger
from utils.exception import ConditionException

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

SUPPORT_STORAGE_TYPE = ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]


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
        print dumps({"result": {"code": 400, "msg": "error occur while %s. %s" % (f_name, e.message)},
                     "data": {}})
        exit(1)
    except Exception:
        logger.debug(f_name)
        logger.debug(params)
        logger.debug(traceback.format_exc())
        print dumps(
            {"result": {"code": 300, "msg": "error occur while %s. traceback: %s" % (f_name, traceback.format_exc())},
             "data": {}})
        exit(1)


def check(f_name, args):
    check_storage_type(args)
    check_pool(f_name, args)


def check_storage_type(args):
    if hasattr(args, 'type') and args.type not in SUPPORT_STORAGE_TYPE:
        print dumps(
            {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)


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
        pool_info = get_pool_info(args.pool)
        uuid = os.path.basename(pool_info['path'])
        poolInfo = get_cstor_pool_info(uuid)
        if type == "localfs":
            if poolInfo['result']['code'] == 0 and poolInfo['data']['proto'] != 'localfs':
                print dumps({"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}})
                exit(3)
        else:
            if poolInfo['result']['code'] == 0:  # is cstor pool, and check pool type
                # check pool type, if pool type not match, stop delete pool
                if 'proto' not in poolInfo['data'].keys():
                    print dumps(
                        {"result": {"code": 221, "msg": "can not get pool proto, cstor-cli cmd bug"}, "data": {}})
                    exit(3)

                if poolInfo['data']['proto'] != type:
                    print dumps({"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}})
                    exit(3)
            else:  # not is cstor pool, exit
                print dumps({"result": {"code": 221,
                                        "msg": "can not get pool %s info, not exist the pool or type is not match" % args.pool},
                             "data": {}})
                exit(3)
    except ExecuteException, e:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 202, "msg": "check_pool_type, cant get pool info. %s" % e.message}, "data": {}})
        exit(2)


def check_pool(f_name, args):
    try:
        if not hasattr(args, 'type'):
            return
        if not hasattr(args, 'pool'):
            return
        if f_name == 'createPool':
            if args.type != 'uus':
                if is_pool_exists(args.pool):
                    raise ConditionException(201, "virsh pool %s has exist" % args.pool)
        else:
            if not is_cstor_pool_exist(args):
                raise ConditionException(204, "cstor pool %s not exist" % args.pool)
            if args.type != 'uus':
                if not is_pool_exists(args.pool):
                    raise ConditionException(203, "virsh pool %s not exist" % args.pool)

    except ExecuteException, e1:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 202, "msg": "check_pool, cant get pool info. %s" % e1.message}, "data": {}})
        exit(2)
    except ConditionException, e2:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": e2.code, "msg": e2.msg}, "data": {}})
        exit(2)


def is_cstor_pool_exist(args):
    if args.type in ['nfs', 'glusterfs']:
        pool = get_cstor_real_poolname(args.pool)
    else:
        pool = args.pool
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
        print dumps(
            {"result": {"code": 207, "msg": "virsh disk " + diskname + " has exist in pool " + pool}, "data": {}})


def check_virsh_disk_not_exist(pool, diskname):
    pool_info = get_pool_info(pool)
    if not os.path.isdir(pool_info['path'] + '/' + diskname):
        print dumps(
            {"result": {"code": 209, "msg": "virsh disk " + diskname + " not exist in pool " + pool}, "data": {}})
        exit(5)


def check_virsh_disk_snapshot_exist(pool, diskname, snapshot):
    pool_info = get_pool_info(pool)
    if os.path.exists(pool_info['path'] + '/' + diskname + '/snapshots/' + snapshot) and \
            not os.path.exists(pool_info['path'] + '/' + diskname + '/' + snapshot):
        print dumps({
            "result": {"code": 209, "msg": "virsh disk snapshot " + snapshot + " has exist in volume " + diskname},
            "data": {}})
        exit(1)


def check_virsh_disk_snapshot_not_exist(pool, diskname, snapshot):
    pool_info = get_pool_info(pool)
    if not os.path.exists('%s/%s/snapshots/%s' % (pool_info['path'], diskname, snapshot)) and \
            not os.path.exists('%s/%s/%s' % (pool_info['path'], diskname, snapshot)):
        print dumps({
            "result": {"code": 209, "msg": "virsh disk snapshot " + snapshot + " not exist in volume " + diskname},
            "data": {}})
        exit(1)


def check_cstor_disk_exist(pool, diskname):
    if is_cstor_disk_exist(pool, diskname):
        print dumps(
            {"result": {"code": 210, "msg": "cstor disk " + diskname + " has exist in pool " + pool}, "data": {}})
        exit(15)


def check_cstor_disk_not_exist(pool, diskname):
    if not is_cstor_disk_exist(pool, diskname):
        print dumps({"result": {"code": 212, "msg": "cstor disk " + pool + " not exist in pool " + pool}, "data": {}})
        exit(15)


def check_virsh_disk_size(pool, vol, size):
    if get_volume_size(pool, vol) >= int(size):
        print dumps({"result": {"code": 213, "msg": "new disk size must larger than the old size."}, "data": {}})
        exit(4)


def check_cstor_snapshot_exist(pool, vol, snapshot):
    op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
    ssInfo = op.execute()
    if ssInfo['result']['code'] == 0:
        print dumps({"result": {"code": 214, "msg": "snapshot " + snapshot + " has exist."}, "data": {}})
        exit(4)


def check_cstor_snapshot_not_exist(pool, vol, snapshot):
    op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
    ssInfo = op.execute()
    if ssInfo['result']['code'] != 0:
        print dumps({"result": {"code": 216, "msg": "snapshot " + snapshot + " not exist."}, "data": {}})
        exit(4)


def createPoolParser(args):
    if args.type == "uus" or args.type == "nfs":
        if args.opt is None:
            print dumps({"result": {"code": 100, "msg": "less arg, opt must be set"}, "data": {}})
            exit(9)

    if args.type == "nfs" or args.type == "glusterfs":
        if args.uuid is None:
            print dumps({"result": {"code": 100, "msg": "less arg, uuid must be set"}, "data": {}})
            exit(9)

    if args.type != "uus":
        if args.content is None:
            print dumps({"result": {"code": 100, "msg": "less arg, content must be set"}, "data": {}})
            exit(9)
        if args.content not in ["vmd", "vmdi", "iso"]:
            print dumps({"result": {"code": 100, "msg": "less arg, content just can be vmd, vmdi, iso"}, "data": {}})
            exit(9)

    execute('createPool', args)


def deletePoolParser(args):
    execute('deletePool', args)


def startPoolParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)
    execute('startPool', args)


def autoStartPoolParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    execute('autoStartPool', args)


def stopPoolParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    execute('stopPool', args)


def showPoolParser(args):
    execute('showPool', args)


def createDiskParser(args):
    if args.type == "uus":
        check_cstor_disk_exist(args.pool, args.vol)
    else:
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(4)
        check_virsh_disk_exist(args.pool, args.vol)

    execute('createDisk', args)


def deleteDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('deleteDisk', args)


def resizeDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_size(args.pool, args.vol, args.capacity)

    execute('resizeDisk', args)


def cloneDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
        check_cstor_disk_exist(args.pool, args.newname)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_exist(args.pool, args.newname)

    execute('cloneDisk', args)


def showDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('showDisk', args)


def prepareDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('prepareDisk', args)


def releaseDiskParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('releaseDisk', args)


def showDiskSnapshotParser(args):
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

    execute('showDiskSnapshot', args)


def createExternalSnapshotParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(1)
    else:
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(3)
        check_virsh_disk_snapshot_exist(args.pool, args.vol, args.name)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        config_path = disk_dir + '/config.json'
        with open(config_path, "r") as f:
            config = load(f)
        if not os.path.isfile(config['current']):
            print dumps(
                {"result": {"code": 100, "msg": "can not find vol current %s." % config['current']}, "data": {}})
            exit(3)
        if os.path.isfile(disk_dir + '/snapshots/' + args.name):
            print dumps({"result": {"code": 100, "msg": "snapshot file has exist"}, "data": {}})
            exit(3)

    execute('createExternalSnapshot', args)


def revertExternalSnapshotParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)
    else:
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(3)

        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        config_path = disk_dir + '/config.json'
        with open(config_path, "r") as f:
            config = load(f)

        if args.backing_file == config['current']:
            print dumps({"result": {"code": 100, "msg": "can not revert disk to itself"}, "data": {}})
            exit(3)
        if not os.path.isfile(config['current']):
            print dumps({"result": {"code": 100, "msg": "can not find current file"}, "data": {}})
            exit(3)
        if not os.path.isfile(args.backing_file):
            print dumps({"result": {"code": 100, "msg": "snapshot file %s not exist" % args.backing_file}, "data": {}})
            exit(3)

    execute('revertExternalSnapshot', args)


def deleteExternalSnapshotParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)
    else:
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        ss_path = disk_dir + '/snapshots/' + args.name
        if not os.path.isfile(ss_path):
            print dumps({"result": {"code": 100, "msg": "snapshot file not exist"}, "data": {}})
            exit(3)

    execute('deleteExternalSnapshot', args)


def updateDiskCurrentParser(args):
    if args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)
    else:
        for current in args.current:
            if not os.path.isfile(current):
                print dumps({"result": {"code": 100, "msg": "current" + current + " file not exist"}, "data": {}})
                exit(3)

    execute('updateDiskCurrent', args)


def customizeParser(args):
    execute('customize', args)


def createDiskFromImageParser(args):
    execute('createDiskFromImage', args)


def migrateParser(args):
    if not re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$', args.ip):
        print dumps({"result": {"code": 100, "msg": "ip is not right"}, "data": {}})
        exit(3)
    execute('migrate', args)


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

# nfs and glusterfs only
parser_create_pool.add_argument("--path", metavar="[PATH]", type=str,
                                help="nfs or glusterfs mount path")

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

# -------------------- add prepareDisk cmd ----------------------------------
parser_prepare_disk = subparsers.add_parser("prepareDisk", help="prepareDisk help")
parser_prepare_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                 help="storage pool type to use")
parser_prepare_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                 help="storage pool to use")
parser_prepare_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                 help="volume name to use")
parser_prepare_disk.add_argument("--uni", required=True, metavar="[UNI]", type=str,
                                 help="volume uni to use")
# set default func
parser_prepare_disk.set_defaults(func=prepareDiskParser)

# -------------------- add releaseDisk cmd ----------------------------------
parser_release_disk = subparsers.add_parser("releaseDisk", help="releaseDisk help")
parser_release_disk.add_argument("--type", required=True, metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                 help="storage pool type to use")
parser_release_disk.add_argument("--pool", required=True, metavar="[POOL]", type=str,
                                 help="storage pool to use")
parser_release_disk.add_argument("--vol", required=True, metavar="[VOL]", type=str,
                                 help="volume name to use")
parser_release_disk.add_argument("--uni", required=True, metavar="[UNI]", type=str,
                                 help="volume uni to use")
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
parser_revert_ess.add_argument("--backing_file", required=True, metavar="[backing_file]", type=str,
                               help="backing_file from k8s")
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
parser_delete_ess.add_argument("--backing_file", required=True, metavar="[backing_file]", type=str,
                               help="backing_file from k8s")
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
parser_customize.add_argument("--user", required=True, metavar="[USER]", type=str,
                              help="disk current file to use")
parser_customize.add_argument("--password", required=True, metavar="[PASSWORD]", type=str,
                              help="disk current file to use")
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

test_args = []

dir1 = parser.parse_args(["createPool", "--type", "localfs", "--pool", "pooldir", "--url", "/mnt/localfs/pooldir", "--content", "vmd"])
dir2 = parser.parse_args(["createDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "1073741824", "--format", "qcow2"])
dir3= parser.parse_args(["prepareDisk", "--type", "nfs", "--pool", "pooldir", "--vol", "diskdir", "--uni", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir4 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--format", "qcow2"])
dir5 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.2", "--format", "qcow2"])
dir6 = parser.parse_args(["revertExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--format", "qcow2", "--backing_file", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir7 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.3", "--format", "qcow2"])
dir8 = parser.parse_args(["deleteExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--backing_file", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir9 = parser.parse_args(["resizeDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "2147483648"])
dir10 = parser.parse_args(["cloneDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--newname", "diskdirclone", "--format", "qcow2"])
dir11 = parser.parse_args(["deleteDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdirclone"])
dir12 = parser.parse_args(["releaseDisk", "--type", "nfs", "--pool", "pooldir", "--vol", "diskdir", "--uni", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir13 = parser.parse_args(["deleteDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir"])
dir14 = parser.parse_args(["stopPool", "--type", "localfs", "--pool", "pooldir"])
dir15 = parser.parse_args(["deletePool", "--type", "localfs", "--pool", "pooldir"])

uus1 = parser.parse_args(["createPool", "--type", "uus", "--pool", "pooldev", "--url", "192.168.3.100:p1", "--opt", "iscsi,username=admin,password=admin,port=7000"])
uus2 = parser.parse_args(["createDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "1073741824"])
# uus3 = parser.parse_args(["resizeDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "2147483648"])
# uus4 = parser.parse_args(["cloneDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--newname", "diskdevclone"])
uus5 = parser.parse_args(["deleteDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev"])
uus6 = parser.parse_args(["deletePool", "--type", "uus", "--pool", "pooldev"])


vdiskfs1 = parser.parse_args(["createPool", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--url", "local", "--content", "vmd"])
vdiskfs2 = parser.parse_args(["createDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--capacity", "1073741824", "--format", "qcow2"])
vdiskfs3 = parser.parse_args(["prepareDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--uni", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs4 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--format", "qcow2"])
vdiskfs5 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.2", "--format", "qcow2"])
vdiskfs6 = parser.parse_args(["revertExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--format", "qcow2", "--backing_file", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs7 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.3", "--format", "qcow2"])
vdiskfs8 = parser.parse_args(["deleteExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--backing_file", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs9 = parser.parse_args(["resizeDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--capacity", "2147483648"])
vdiskfs10 = parser.parse_args(["cloneDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--newname", "diskvdiskfsclone", "--format", "qcow2"])
vdiskfs11 = parser.parse_args(["deleteDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfsclone"])
vdiskfs12 = parser.parse_args(["releaseDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--uni", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs13 = parser.parse_args(["deleteDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs"])
vdiskfs14 = parser.parse_args(["stopPool", "--type", "vdiskfs", "--pool", "poolvdiskfs"])
vdiskfs15 = parser.parse_args(["deletePool", "--type", "vdiskfs", "--pool", "poolvdiskfs"])

nfs1 = parser.parse_args(["createPool", "--type", "nfs", "--pool", "poolnfs", "--url", "133.133.135.30:/home/nfs", "--opt", "nolock", "--content", "vmd", "--uuid", "07098ca5fd174fccafee76b0d7fccde4"])
nfs2 = parser.parse_args(["createDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "1073741824", "--format", "qcow2"])
nfs3 = parser.parse_args(["prepareDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--uni", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/disknfs/disknfs"])
nfs4 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--format", "qcow2"])
nfs5 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.2", "--format", "qcow2"])
nfs6 = parser.parse_args(["revertExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--format", "qcow2", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/disknfs/disknfs"])
nfs7 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.3", "--format", "qcow2"])
nfs8 = parser.parse_args(["deleteExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/disknfs/disknfs"])
nfs9 = parser.parse_args(["resizeDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "2147483648"])
nfs10 = parser.parse_args(["cloneDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--newname", "disknfsclone", "--format", "qcow2"])
nfs11 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfsclone"])
nfs12 = parser.parse_args(["releaseDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--uni", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/disknfs/disknfs"])
nfs13 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs"])
nfs14 = parser.parse_args(["stopPool", "--type", "nfs", "--pool", "poolnfs"])
nfs15 = parser.parse_args(["deletePool", "--type", "nfs", "--pool", "poolnfs"])

gfs1 = parser.parse_args(["createPool", "--type", "glusterfs", "--pool", "poolglusterfs", "--url", "192.168.3.100:nfsvol", "--content", "vmd", "--uuid", "07098ca5fd174fccafee76b0d7fccde4"])
gfs2 = parser.parse_args(["createDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "1073741824", "--format", "qcow2"])
gfs3 = parser.parse_args(["prepareDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--uni", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/diskglusterfs/diskglusterfs"])
gfs4 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--format", "qcow2"])
gfs5 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.2", "--format", "qcow2"])
gfs6 = parser.parse_args(["revertExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--format", "qcow2", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/diskglusterfs/diskglusterfs"])
gfs7 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.3", "--format", "qcow2"])
gfs8 = parser.parse_args(["deleteExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--backing_file", "/var/lib/libvirt/cstor/poolglusterfs/abc/07098ca5fd174fccafee76b0d7fccde4/diskglusterfs"])
gfs9 = parser.parse_args(["resizeDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "2147483648"])
gfs10 = parser.parse_args(["cloneDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--newname", "diskglusterfsclone", "--format", "qcow2"])
gfs11 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfsclone"])
gfs12 = parser.parse_args(["releaseDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--uni", "/var/lib/libvirt/cstor/abc/07098ca5fd174fccafee76b0d7fccde4/diskglusterfs/diskglusterfs"])
gfs13 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs"])
gfs14 = parser.parse_args(["stopPool", "--type", "glusterfs", "--pool", "poolglusterfs"])
gfs15 = parser.parse_args(["deletePool", "--type", "glusterfs", "--pool", "poolglusterfs"])

# test_args.append(dir1)
# test_args.append(dir2)
# test_args.append(dir3)
# test_args.append(dir4)
# test_args.append(dir5)
# test_args.append(dir6)
# test_args.append(dir7)
# test_args.append(dir8)
# test_args.append(dir9)
# test_args.append(dir10)
# test_args.append(dir11)
# test_args.append(dir12)
# test_args.append(dir13)
# test_args.append(dir14)
# test_args.append(dir15)

# test_args.append(uus1)
# test_args.append(uus2)
# # test_args.append(uus3)
# # test_args.append(uus4)
# test_args.append(uus5)
# test_args.append(uus6)
#
# test_args.append(vdiskfs1)
# test_args.append(vdiskfs2)
# test_args.append(vdiskfs3)
# test_args.append(vdiskfs4)
# test_args.append(vdiskfs5)
# test_args.append(vdiskfs6)
# test_args.append(vdiskfs7)
# test_args.append(vdiskfs8)
# test_args.append(vdiskfs9)
# test_args.append(vdiskfs10)
# test_args.append(vdiskfs11)
# test_args.append(vdiskfs12)
# test_args.append(vdiskfs13)
# test_args.append(vdiskfs14)
# test_args.append(vdiskfs15)
#
test_args.append(nfs1)
test_args.append(nfs2)
test_args.append(nfs3)
test_args.append(nfs4)
test_args.append(nfs5)
test_args.append(nfs6)
test_args.append(nfs7)
test_args.append(nfs8)
test_args.append(nfs9)
test_args.append(nfs10)
test_args.append(nfs11)
test_args.append(nfs12)
test_args.append(nfs13)
test_args.append(nfs14)
test_args.append(nfs15)
#
# test_args.append(gfs1)
# test_args.append(gfs2)
# test_args.append(gfs3)
# test_args.append(gfs4)
# test_args.append(gfs5)
# test_args.append(gfs6)
# test_args.append(gfs7)
# test_args.append(gfs8)
# test_args.append(gfs9)
# test_args.append(gfs10)
# test_args.append(gfs11)
# test_args.append(gfs12)
# test_args.append(gfs13)
# test_args.append(gfs14)
# test_args.append(gfs15)


for args in test_args:
    try:
        args.func(args)
    except TypeError:
        print traceback.format_exc()
        logger.debug(traceback.format_exc())


# try:
#     args = parser.parse_args()
#     args.func(args)
# except TypeError:
#     # print "argument number not enough"
#     logger.debug(traceback.format_exc())

# try:
#     args = parser.parse_args(
#         ["migrate", "--domain", "vm006", "--ip", "133.133.135.22"])
#     args.func(args)
    # args = parser.parse_args(["createPool", "--type", "localfs", "--pool", "vmdi", "--url", "/mnt/localfs/sdb", "--content", "vmdi"])
    # args.func(args)
    #
    # args = parser.parse_args(
    #     ["createDisk", "--type", "localfs", "--pool", "vmdi", "--vol", "vm006", "--capacity", "10737418240", "--format", "qcow2"])
    # args.func(args)
    # args = parser.parse_args(
    #     ["createDiskFromImage", "--type", "localfs", "--targetPool", "vmdi", "--name", "vm006copyrw", "--source", "/var/lib/libvirt/cstor/07098ca5fd174fccafed76b0d7fccde9/07098ca5-fd17-4fcc-afed-76b0d7fccde9/wyw222/wyw222", "--full_copy"])
    # args.func(args)

    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "localfs", "--pool", "vmdi", "--format", "qcow2", "--name", "vm006.1", "--vol", "vm006"])
    # args.func(args)
    # #
    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "localfs", "--pool", "pooltest", "--format", "qcow2", "--name", "ss2",
    #      "--vol", "disktest"])
    # args.func(args)
    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "localfs", "--pool", "pooltest", "--format", "qcow2", "--name", "ss3",
    #      "--vol", "disktest"])
    # args.func(args)
    #
    # args = parser.parse_args(
    #     ["revertExternalSnapshot", "--type", "localfs", "--pool", "pooluittest", "--name", "datadisk.1",
    #      "--vol", "datadisk", "--format", "qcow2", "--backing_file", "/uit/pooluittest/datadisk/datadisk", "--domain", "vm010"])
    # args.func(args)
    # #
    # args = parser.parse_args(
    #     ["deleteExternalSnapshot", "--type", "localfs", "--pool", "pooluittest", "--name", "pooluittest.1",
    #      "--vol", "pooluittest", "--domain", "vm010", "--backing_file", "77525c7142144967831099b7626a0cb5"])
    # args.func(args)
    # args = parser.parse_args(
    #     ["updateDiskCurrent", "--type", "localfs", "--current", "/var/lib/libvirt/pooltest/disktest/ss2"])
    # args.func(args)
# except TypeError:
#     print dumps({"result": {"code": 1, "msg": "script error, plz check log file."}, "data": {}})
#     print traceback.format_exc()
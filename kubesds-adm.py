import argparse
from operation import *

from utils import logger

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

SUPPORT_STORAGE_TYPE = ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]

def execute(f_name, params):
    moudle = __import__('operation')
    func = getattr(moudle, f_name)
    try:
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
        print dumps({"result": {"code": 300, "msg": "error occur while %s. traceback: %s" % (f_name, traceback.format_exc())}, "data": {}})
        exit(1)

def check_storage_type(args):
    if args.type not in SUPPORT_STORAGE_TYPE:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)

def get_cstor_pool_info(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    result = op.execute()
    return result


def check_pool_type(pool, type):
    pool_info = get_pool_info(pool)
    if type == 'nfs' or type == 'glusterfs':
        uuid = os.path.basename(pool_info['path'])
        poolInfo = get_cstor_pool_info(uuid)
    else:
        poolInfo = get_cstor_pool_info(pool)
    if type == "localfs":
        if poolInfo['result']['code'] == 0 and poolInfo['data']['proto'] != 'localfs':
            print dumps({"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}})
            exit(3)
    else:
        if poolInfo['result']['code'] == 0:  # is cstor pool, and check pool type
            # check pool type, if pool type not match, stop delete pool
            if 'proto' not in poolInfo['data'].keys():
                print dumps({"result": {"code": 221, "msg": "can not get pool proto, cstor-cli cmd bug"}, "data": {}})
                exit(3)

            if poolInfo['data']['proto'] != type:
                print dumps({"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}})
                exit(3)
        else:  # not is cstor pool, exit
            print dumps({"result": {"code": 221,
                              "msg": "can not get pool " + pool + " info, not exist the pool or type is not match"},
                   "data": {}})
            exit(3)


def is_cstor_pool_exist(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    result = op.execute()
    if result["result"]["code"] == 0:
        return True
    else:
        return False


def is_cstor_disk_exist(pool, diskname):
    op = Operation("cstor-cli vdisk-show", {"poolname": pool, "name": diskname}, with_result=True)
    result = op.execute()
    if result["result"]["code"] == 0:
        return True
    else:
        return False


def check_virsh_pool_exist(pool):
    try:
        if is_pool_exists(pool):
            print dumps({"result": {"code": 201, "msg": "virsh pool " + pool + " has exist"}, "data": {}})
            exit(1)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 202, "msg": "cant get virsh pool info"}, "data": {}})
        exit(2)


def check_virsh_pool_not_exist(pool):
    try:
        if not is_pool_exists(pool):
            print dumps({"result": {"code": 203, "msg": "virsh pool " + pool + " not exist"}, "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 202, "msg": "cant get virsh pool info"}, "data": {}})
        exit(6)


def check_cstor_pool_exist(pool):
    try:
        if is_cstor_pool_exist(pool):
            print dumps({"result": {"code": 204, "msg": "cstor pool " + pool + " has exist"}, "data": {}})
            exit(7)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 205, "msg": "cant get cstor pool info"}, "data": {}})
        exit(8)


def check_cstor_pool_not_exist(pool):
    try:
        pool_info = get_pool_info(pool)
        uuid = os.path.basename(pool_info['path'])
        if not is_cstor_pool_exist(uuid):
            print dumps({"result": {"code": 206, "msg": "cstor pool " + uuid + " not exist"}, "data": {}})
            exit(11)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 205, "msg": "cant get cstor pool info"}, "data": {}})
        exit(8)


def check_virsh_disk_exist(pool, diskname):
    try:
        pool_info = get_pool_info(pool)
        if os.path.isdir(pool_info['path'] + '/' + diskname):
            print dumps({"result": {"code": 207, "msg": "virsh disk " + diskname + " has exist in pool " + pool}, "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 208, "msg": "cant get virsh disk info, please check pool and disk is match or not"},
               "data": {}})
        exit(6)


def check_virsh_disk_not_exist(pool, diskname):
    try:
        pool_info = get_pool_info(pool)
        if not os.path.isdir(pool_info['path'] + '/' + diskname):
            print dumps({"result": {"code": 209, "msg": "virsh disk " + diskname + " not exist in pool " + pool}, "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}})
        exit(6)


def check_virsh_disk_snapshot_exist(pool, diskname, snapshot):
    try:
        pool_info = get_pool_info(pool)
        if os.path.exists(pool_info['path'] + '/' + diskname + '/snapshots/' + snapshot) and \
                not os.path.exists(pool_info['path'] + '/' + diskname + '/' + snapshot):
            print dumps({
                "result": {"code": 209, "msg": "virsh disk snapshot " + snapshot + " has exist in volume " + diskname},
                "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}})
        exit(6)


def check_virsh_disk_snapshot_not_exist(pool, diskname, snapshot):
    try:
        pool_info = get_pool_info(pool)
        if not os.path.exists(pool_info['path'] + '/' + diskname + '/snapshots/' + snapshot) and \
                not os.path.exists(pool_info['path'] + '/' + diskname + '/' + snapshot):
            print dumps({
                "result": {"code": 209, "msg": "virsh disk snapshot " + snapshot + " not exist in volume " + diskname},
                "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}})
        exit(6)


def check_cstor_disk_exist(pool, diskname):
    try:
        if is_cstor_disk_exist(pool, diskname):
            print dumps({"result": {"code": 210, "msg": "cstor disk " + diskname + " has exist in pool " + pool}, "data": {}})
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 211, "msg": "cant get cstor disk info"}, "data": {}})
        exit(8)


def check_cstor_disk_not_exist(pool, diskname):
    try:
        if not is_cstor_disk_exist(pool, diskname):
            print dumps({"result": {"code": 212, "msg": "cstor disk " + pool + " not exist in pool " + pool}, "data": {}})
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 211, "msg": "cant get cstor disk info"}, "data": {}})
        exit(9)


def check_virsh_disk_size(pool, vol, size):
    try:
        if get_volume_size(pool, vol) >= int(size):
            print dumps({"result": {"code": 213, "msg": "new disk size must larger than the old size."}, "data": {}})
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}})
        exit(9)


def check_cstor_snapshot_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] == 0:
            print dumps({"result": {"code": 214, "msg": "snapshot " + snapshot + " has exist."}, "data": {}})
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 215, "msg": "cant get cstor snapshot info"}, "data": {}})
        exit(9)


def check_cstor_snapshot_not_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] != 0:
            print dumps({"result": {"code": 216, "msg": "snapshot " + snapshot + " not exist."}, "data": {}})
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print dumps({"result": {"code": 215, "msg": "cant get cstor snapshot info"}, "data": {}})
        exit(9)


def createPoolParser(args):
    check_storage_type(args)
    if args.type == "uus" or args.type == "nfs":
        if args.opt is None:
            print dumps({"result": {"code": 100, "msg": "less arg, opt must be set"}, "data": {}})
            exit(9)

    if args.type == "nfs" or args.type == "glusterfs":
        if args.uuid is None:
            print dumps({"result": {"code": 100, "msg": "less arg, uuid must be set"}, "data": {}})
            exit(9)

    if args.type == "uus":
        # check cstor pool
        check_cstor_pool_exist(args.pool)
    else:
        if args.content is None:
            print dumps({"result": {"code": 100, "msg": "less arg, content must be set"}, "data": {}})
            exit(9)
        if args.content not in ["vmd", "vmdi", "iso"]:
            print dumps({"result": {"code": 100, "msg": "less arg, content just can be vmd, vmdi, iso"}, "data": {}})
            exit(9)
        # check cstor pool
        check_cstor_pool_exist(args.pool)
        # check virsh pool, only for nfs, glusterfs and vdiskfs
        check_virsh_pool_exist(args.pool)

    execute('createPool', args)


def deletePoolParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        # check pool type, if pool type not match, stop delete pool
        check_pool_type(args.pool, args.type)

        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    execute('deletePool', args)


def startPoolParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)
    execute('startPool', args)


def autoStartPoolParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    execute('autoStartPool', args)


def stopPoolParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    execute('stopPool', args)


def showPoolParser(args):
    check_storage_type(args)
    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        # check virsh pool, only for localfs, vdiskfs, nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    execute('showPool', args)

def createDiskParser(args):
    check_storage_type(args)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(4)
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_exist(args.pool, args.vol)
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_pool_not_exist(args.pool)
        check_cstor_disk_exist(args.pool, args.vol)

    execute('createDisk', args)


def deleteDiskParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    execute('deleteDisk', args)


def resizeDiskParser(args):
    check_storage_type(args)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_size(args.pool, args.vol, args.capacity)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    execute('resizeDisk', args)


def cloneDiskParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_exist(args.pool, args.newname)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
        check_cstor_disk_exist(args.pool, args.newname)

    execute('cloneDisk', args)

def showDiskParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    execute('showDisk', args)

def prepareDiskParser(args):
    check_storage_type(args)

    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('prepareDisk', args)

def releaseDiskParser(args):
    check_storage_type(args)
    if args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
    else:
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)

    execute('releaseDisk', args)

def showDiskSnapshotParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    execute('showDiskSnapshot', args)


def createExternalSnapshotParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(3)
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_snapshot_exist(args.pool, args.vol, args.name)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        config_path = disk_dir + '/config.json'
        with open(config_path, "r") as f:
            config = load(f)
        if not os.path.isfile(config['current']):
            print dumps({"result": {"code": 100, "msg": "can not find vol current %s." % config['current']}, "data": {}})
            exit(3)
        if os.path.isfile(disk_dir + '/snapshots/' + args.name):
            print dumps({"result": {"code": 100, "msg": "snapshot file has exist"}, "data": {}})
            exit(3)
    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(1)

    execute('createExternalSnapshot', args)


def revertExternalSnapshotParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        if args.format is None:
            print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
            exit(3)

        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
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

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    execute('revertExternalSnapshot', args)


def deleteExternalSnapshotParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        ss_path = disk_dir + '/snapshots/' + args.name
        if not os.path.isfile(ss_path):
            print dumps({"result": {"code": 100, "msg": "snapshot file not exist"}, "data": {}})
            exit(3)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    execute('deleteExternalSnapshot', args)


def updateDiskCurrentParser(args):
    check_storage_type(args)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        for current in args.current:
            if not os.path.isfile(current):
                print dumps({"result": {"code": 100, "msg": "current" + current + " file not exist"}, "data": {}})
                exit(3)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    execute('updateDiskCurrent', args)


def customizeParser(args):
    execute('customize', args)

def createDiskFromImageParser(args):
    check_storage_type(args)

    execute('createDiskFromImage', args)
def migrateParser(args):
    if args.ip is None:
        print dumps({"result": {"code": 100, "msg": "less arg, ip must be set"}, "data": {}})
        exit(3)
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

# vdiskfs only
parser_create_pool.add_argument("--force", metavar="[FORCE]", type=str,
                                help="vdiskfs only, force add vdiskfs pool do not check mount")

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
parser_migrate.add_argument("--domain", metavar="[DOMAIN]", type=str,
                            help="vm domain to migrate")
parser_migrate.add_argument("--ip", metavar="[IP]", type=str,
                            help="storage pool type to use")
parser_migrate.add_argument("--offline", metavar="[OFFLINE]", type=bool, nargs='?', const=True,
                            help="support migrate offline")
# set default func
parser_migrate.set_defaults(func=migrateParser)

try:
    args = parser.parse_args()
    args.func(args)
except TypeError:
    # print "argument number not enough"
    logger.debug(traceback.format_exc())

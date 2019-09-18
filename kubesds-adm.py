import argparse
import os
import traceback
from sys import exit

from operation import *
from utils import logger
from utils.exception import *
from utils.exception import ExecuteException
from utils.libvirt_util import is_pool_exists, is_volume_exists, get_volume_path, get_volume_snapshots

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)


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
            print dumps({"result": {"code": 5, "msg": "virsh pool " + pool + " has exist"}, "data": {}})
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 6, "msg": "cant get virsh pool info"}, "data": {}}
        exit(6)

def check_virsh_pool_not_exist(pool):
    try:
        if not is_pool_exists(pool):
            print {"result": {"code": 5, "msg": "virsh pool " + pool + " not exist"}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 6, "msg": "cant get virsh pool info"}, "data": {}}
        exit(6)

def check_cstor_pool_exist(pool):
    try:
        if is_cstor_pool_exist(pool):
            print {"result": {"code": 7, "msg": "cstor pool " + pool + " has exist"}, "data": {}}
            exit(7)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 8, "msg": "cant get cstor pool info"}, "data": {}}
        exit(8)

def check_cstor_pool_not_exist(pool):
    try:
        if not is_cstor_pool_exist(pool):
            print {"result": {"code": 11, "msg": "cstor pool " + pool + " not exist"}, "data": {}}
            exit(11)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 8, "msg": "cant get cstor pool info"}, "data": {}}
        exit(8)


def check_virsh_disk_exist(pool, diskname):
    try:
        if is_volume_exists(diskname, pool):
            print {"result": {"code": 13, "msg": "virsh disk " + diskname + " has exist in pool "+pool}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 6, "msg": "cant get virsh disk info, does exist the pool "+pool}, "data": {}}
        exit(6)

def check_virsh_disk_not_exist(pool, diskname):
    try:
        if not is_volume_exists(diskname, pool):
            print {"result": {"code": 14, "msg": "virsh disk " + diskname + " not exist in pool "+pool}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 6, "msg": "cant get virsh disk info"}, "data": {}}
        exit(6)

def check_cstor_disk_exist(pool, diskname):
    try:
        if is_cstor_disk_exist(pool, diskname):
            print {"result": {"code": 15, "msg": "cstor disk " + pool + " has exist in pool "+pool}, "data": {}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 8, "msg": "cant get cstor disk info"}, "data": {}}
        exit(8)

def check_cstor_disk_not_exist(pool, diskname):
    try:
        if not is_cstor_disk_exist(pool, diskname):
            print {"result": {"code": 16, "msg": "cstor disk " + pool + " not exist in pool "+pool}, "data": {}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get cstor disk info"}, "data": {}}
        exit(9)

def check_virsh_disk_size(pool, diskname, size):
    try:
        vol_xml = get_volume_xml(pool, diskname)
        result = loads(xmlToJson(vol_xml))
        if int(result["volume"]["capacity"]["text"]) >= int(size):
            print {"result": {"code": 4, "msg": "new cstor disk size must larger than the old size."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get virsh disk info"}, "data": {}}
        exit(9)

def check_virsh_snapshot_exist(pool, vol, snapshot):
    try:
        vol_path = get_volume_path(pool, vol)
        snapshots = get_volume_snapshots(vol_path)['snapshot']
        for sn in snapshots:
            if sn.get('name') == snapshot:
                print {"result": {"code": 4, "msg": "snapshot " + snapshot + " has exist."}, "data": {}}
                exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get virsh snapshot info"}, "data": {}}
        exit(9)
def check_cstor_snapshot_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"pool": pool, "vol": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] == 0:
            print {"result": {"code": 4, "msg": "snapshot " + snapshot + " has exist."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get cstor snapshot info"}, "data": {}}
        exit(9)

def check_virsh_snapshot_not_exist(pool, vol, snapshot):
    try:
        vol_path = get_volume_path(pool, vol)
        snapshots = get_volume_snapshots(vol_path)['snapshot']
        for sn in snapshots:
            if sn.get('name') == snapshot:
                return
        print {"result": {"code": 4, "msg": "snapshot " + snapshot + " not exist."}, "data": {}}
        exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get virsh snapshot info"}, "data": {}}
        exit(9)

def check_cstor_snapshot_not_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"pool": pool, "vol": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] != 0:
            print {"result": {"code": 4, "msg": "snapshot " + snapshot + " not exist."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 9, "msg": "cant get cstor snapshot info"}, "data": {}}
        exit(9)

def createPoolParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type "+args.type+" not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.target is None:
            print {"result": {"code": 9, "msg": "less arg, target must be set"}, "data": {}}
            exit(9)
    if args.type == "uus" or args.type == "nfs" or args.type == "glusterfs":
        if args.url is None:
            print {"result": {"code": 9, "msg": "less arg, url must be set"}, "data": {}}
            exit(9)

    if args.type == "dir":
        check_virsh_pool_exist(args.pool)

    elif args.type == "uus":
        # check cstor pool
        check_cstor_pool_exist(args.pool)

    elif args.type == "nfs":
        # check cstor pool
        check_cstor_pool_exist(args.pool)
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_exist(args.pool)

    elif args.type == "glusterfs":
        # check cstor pool
        check_cstor_pool_exist(args.pool)
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_exist(args.pool)

    createPool(args)


def deletePoolParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.type == "dir":
        check_virsh_pool_not_exist(args.pool)

    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "nfs" or args.type == "glusterfs":
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    deletePool(args)

def showPoolParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.type == "dir":
        check_virsh_pool_not_exist(args.pool)

    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "nfs" or args.type == "glusterfs":
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    showPool(args)

def createDiskParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.capacity is None:
            print {"result": {"code": 4, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(4)
        if args.format is None:
            print {"result": {"code": 4, "msg": "less arg, format must be set"}, "data": {}}
            exit(4)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_exist(args.pool, args.vol)
    elif args.type == "uus":
        if args.capacity is None:
            print {"result": {"code": 4, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(4)
        # check cstor disk
        check_cstor_pool_not_exist(args.pool)
        check_cstor_disk_exist(args.pool, args.vol)

    createDisk(args)

def deleteDiskParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    deleteDisk(args)

def resizeDiskParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        print
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        print
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.capacity is None:
        print {"result": {"code": 3, "msg": "less arg, capacity must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_size(args.pool, args.vol, args.capacity)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    resizeDisk(args)

def cloneDiskParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.newname is None:
        print {"result": {"code": 3, "msg": "less arg, newname must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_exist(args.pool, args.newname)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
        check_cstor_disk_exist(args.pool, args.newname)

    cloneDisk(args)

def showDiskParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    showDisk(args)


def createSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.snapshot is None:
        print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.capacity is None:
            print {"result": {"code": 3, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(3)
        if args.snapshot_format is None:
            print {"result": {"code": 3, "msg": "less arg, snapshot_format must be set"}, "data": {}}
            exit(3)
        if args.format is None:
            print {"result": {"code": 3, "msg": "less arg, format must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_exist(args.pool, args.snapshot)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_snapshot_exist(args.pool, args.vol, args.snapshot)

    createSnapshot(args)


def deleteSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.snapshot is None:
            print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.snapshot)
    elif args.type == "uus":
        if args.vol is None:
            print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.vol, args.snapshot)

    deleteSnapshot(args)

def recoverySnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        print {"result": {"code": 3, "msg": "not support operation"}, "data": {}}
        exit(3)
    elif args.type == "uus":
        if args.pool is None:
            print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
            exit(3)
        if args.vol is None:
            print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.vol, args.snapshot)

    recoverySnapshot(args)

def showSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 1, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 2, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 3, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.snapshot is None:
            print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.snapshot)
    elif args.type == "uus":
        if args.vol is None:
            print {"result": {"code": 3, "msg": "less arg, name must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 3, "msg": "less arg, sname must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.vol, args.snapshot)

    showSnapshot(args)

# --------------------------- cmd line parser ---------------------------------------
parser = argparse.ArgumentParser(prog="kubeovs-adm", description="All storage adaptation tools")

subparsers = parser.add_subparsers(help="sub-command help")

# -------------------- add createPool cmd ----------------------------------
parser_create_pool = subparsers.add_parser("createPool", help="createPool help")
parser_create_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")

parser_create_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--url", metavar="[URL]", type=str,
                                help="storage pool create location, only for uus")

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--target", metavar="[TARGET]", type=str,
                                help="storage pool create location, only for dir, nfs and glusterfs")

# nfs only
parser_create_pool.add_argument("--opt", metavar="[OPT]", type=str,
                                help="nfs mount options, only for nfs")

# set default func
parser_create_pool.set_defaults(func=createPoolParser)

# -------------------- add deletePool cmd ----------------------------------
parser_delete_pool = subparsers.add_parser("deletePool", help="deletePool help")
parser_delete_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")

parser_delete_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")
# set default func
parser_delete_pool.set_defaults(func=deletePoolParser)

# -------------------- add showPool cmd ----------------------------------
parser_show_pool = subparsers.add_parser("showPool", help="showPool help")
parser_show_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")

parser_show_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to show")
# set default func
parser_show_pool.set_defaults(func=showPoolParser)

# -------------------- add createDisk cmd ----------------------------------
parser_create_disk = subparsers.add_parser("createDisk", help="createDisk help")
parser_create_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="disk type to use")
parser_create_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")

parser_create_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")

# will transfer to --size when type in uus, nfs or glusterfs
parser_create_disk.add_argument("--capacity", metavar="[CAPACITY]", type=str,
                                help="capacity is the size of the volume to be created, as a scaled integer (see NOTES above), defaulting to bytes")
parser_create_disk.add_argument("--format", metavar="[raw|bochs|qcow|qcow2|vmdk|qed]", type=str,
                                help="format is used in file based storage pools to specify the volume file format to use; raw, bochs, qcow, qcow2, vmdk, qed.")

# set default func
parser_create_disk.set_defaults(func=createDiskParser)

# -------------------- add deleteDisk cmd ----------------------------------
parser_delete_disk = subparsers.add_parser("deleteDisk", help="deleteDisk help")
parser_delete_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_delete_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_delete_disk.set_defaults(func=deleteDiskParser)


# -------------------- add resizeDisk cmd ----------------------------------
parser_resize_disk = subparsers.add_parser("resizeDisk", help="resizeDisk help")
parser_resize_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_resize_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_resize_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_resize_disk.add_argument("--capacity", metavar="[CAPACITY]", type=str,
                                help="new volume capacity to use")
parser_resize_disk.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="new volume capacity to use")
# set default func
parser_resize_disk.set_defaults(func=resizeDiskParser)


# -------------------- add cloneDisk cmd ----------------------------------
parser_clone_disk = subparsers.add_parser("cloneDisk", help="cloneDisk help")
parser_clone_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_clone_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_clone_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_clone_disk.add_argument("--newname", metavar="[NEWNAME]", type=str,
                                help="new volume name to use")
# set default func
parser_clone_disk.set_defaults(func=cloneDiskParser)

# -------------------- add showDisk cmd ----------------------------------
parser_show_disk = subparsers.add_parser("showDisk", help="showDisk help")
parser_show_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_show_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_show_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_show_disk.set_defaults(func=showDiskParser)


# -------------------- add createSnapshot cmd ----------------------------------
parser_create_ss = subparsers.add_parser("createSnapshot", help="createSnapshot help")
parser_create_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_create_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_create_ss.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_create_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_create_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
parser_create_ss.add_argument("--capacity", metavar="[CAPACITY]", type=str,
                                help="disk capacity to use")
parser_create_ss.add_argument("--snapshot_format", metavar="[SNAPSHOT_FORMAT]", type=str,
                                help="disk backing vol format to use")
parser_create_ss.add_argument("--format", metavar="[FORMAT]", type=str,
                                help="disk format to use")
# set default func
parser_create_ss.set_defaults(func=createSnapshotParser)

# -------------------- add deleteSnapshot cmd ----------------------------------
parser_delete_ss = subparsers.add_parser("deleteSnapshot", help="deleteSnapshot help")
parser_delete_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_delete_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_ss.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_delete_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_delete_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_delete_ss.set_defaults(func=deleteSnapshotParser)


# -------------------- add recoverySnapshot cmd ----------------------------------
parser_recovery_ss = subparsers.add_parser("recoverySnapshot", help="recoverySnapshot help")
parser_recovery_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_recovery_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_recovery_ss.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_recovery_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_recovery_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_recovery_ss.set_defaults(func=recoverySnapshotParser)

# -------------------- add showSnapshot cmd ----------------------------------
parser_show_ss = subparsers.add_parser("showSnapshot", help="showSnapshot help")
parser_show_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs]", type=str,
                                help="storage pool type to use")
parser_show_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_show_ss.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_show_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_show_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_show_ss.set_defaults(func=showSnapshotParser)

# test_args = []
#
# dir1 = parser.parse_args(["createPool", "--type", "dir", "--pool", "pooldir", "--target", "/var/lib/libvirt/pooldir"])
# dir2 = parser.parse_args(["createDisk", "--type", "dir", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "1073741824", "--format", "qcow2"])
# dir3 = parser.parse_args(["resizeDisk", "--type", "dir", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "2147483648"])
# dir4 = parser.parse_args(["cloneDisk", "--type", "dir", "--pool", "pooldir", "--vol", "diskdir", "--newname", "diskdirclone"])
# dir5 = parser.parse_args(["deleteDisk", "--type", "dir", "--pool", "pooldir", "--vol", "diskdirclone"])
# dir6 = parser.parse_args(["deleteDisk", "--type", "dir", "--pool", "pooldir", "--vol", "diskdir"])
# dir7 = parser.parse_args(["deletePool", "--type", "dir", "--pool", "pooldir"])
# #
# uus1 = parser.parse_args(["createPool", "--type", "uus", "--pool", "pooldev", "--url", "uus-iscsi-independent://admin:admin@192.168.3.10:7000/p1/4/2/0/32/0/3"])
# uus2 = parser.parse_args(["createDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "1073741824"])
# # uus3 = parser.parse_args(["resizeDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "2147483648"])
# # uus4 = parser.parse_args(["cloneDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--newname", "diskdevclone"])
# uus5 = parser.parse_args(["deleteDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev"])
# uus6 = parser.parse_args(["deletePool", "--type", "uus", "--pool", "pooldev"])
#
# nfs1 = parser.parse_args(["createPool", "--type", "nfs", "--pool", "poolnfs", "--url", "nfs://192.168.3.99:/nfs/nfs", "--target", "poolnfs", "--opt", "nolock"])
# nfs2 = parser.parse_args(["createDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "1073741824", "--format", "qcow2"])
# nfs3 = parser.parse_args(["resizeDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "2147483648"])
# nfs4 = parser.parse_args(["cloneDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--newname", "disknfsclone"])
# nfs5 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfsclone"])
# nfs6 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs"])
# nfs7 = parser.parse_args(["deletePool", "--type", "nfs", "--pool", "poolnfs"])
#
# gfs1 = parser.parse_args(["createPool", "--type", "glusterfs", "--pool", "poolglusterfs", "--url", "glusterfs://192.168.3.93:nfsvol", "--target", "poolglusterfs"])
# gfs2 = parser.parse_args(["createDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "1073741824", "--format", "qcow2"])
# gfs3 = parser.parse_args(["resizeDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "2147483648"])
# gfs4 = parser.parse_args(["cloneDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--newname", "diskglusterfsclone"])
# gfs5 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfsclone"])
# gfs6 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs"])
# gfs7 = parser.parse_args(["deletePool", "--type", "glusterfs", "--pool", "poolglusterfs"])
#
#
# test_args.append(dir1)
# test_args.append(dir2)
# test_args.append(dir3)
# test_args.append(dir4)
# test_args.append(dir5)
# test_args.append(dir6)
# test_args.append(dir7)
#
# test_args.append(uus1)
# test_args.append(uus2)
# # test_args.append(uus3)
# # test_args.append(uus4)
# test_args.append(uus5)
# test_args.append(uus6)
#
# test_args.append(nfs1)
# test_args.append(nfs2)
# test_args.append(nfs3)
# test_args.append(nfs4)
# test_args.append(nfs5)
# test_args.append(nfs6)
# test_args.append(nfs7)
#
# test_args.append(gfs1)
# test_args.append(gfs2)
# test_args.append(gfs3)
# test_args.append(gfs4)
# test_args.append(gfs5)
# test_args.append(gfs6)
# test_args.append(gfs7)
#
#
# for args in test_args:
#     try:
#         args.func(args)
#     except TypeError:
#         logger.debug(traceback.format_exc())


try:
    args = parser.parse_args()
    args.func(args)
except TypeError:
    # print "argument number not enough"
    logger.debug(traceback.format_exc())
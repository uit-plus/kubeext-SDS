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


def get_cstor_pool_info(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    result = op.execute()
    return result

def check_pool_type(pool, type):
    poolInfo = get_cstor_pool_info(pool)
    if type == "dir":
        if poolInfo['result']['code'] == 0:
            print {"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}}
            exit(3)
    else:
        if poolInfo['result']['code'] == 0:  # is cstor pool, and check pool type
            # check pool type, if pool type not match, stop delete pool
            if 'proto' not in poolInfo['data'].keys():
                print {"result": {"code": 221, "msg": "can not get pool proto, cstor-cli cmd bug"}, "data": {}}
                exit(3)

            if poolInfo['data']['proto'] != type:
                print {"result": {"code": 221, "msg": "type is not match, plz check"}, "data": {}}
                exit(3)
        else:  # not is cstor pool, exit
            print {"result": {"code": 221, "msg": "can not get pool "+pool+" info, not exist the pool or type is not match"}, "data": {}}
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
        print {"result": {"code": 202, "msg": "cant get virsh pool info"}, "data": {}}
        exit(2)

def check_virsh_pool_not_exist(pool):
    try:
        if not is_pool_exists(pool):
            print {"result": {"code": 203, "msg": "virsh pool " + pool + " not exist"}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 202, "msg": "cant get virsh pool info"}, "data": {}}
        exit(6)

def check_cstor_pool_exist(pool):
    try:
        if is_cstor_pool_exist(pool):
            print {"result": {"code": 204, "msg": "cstor pool " + pool + " has exist"}, "data": {}}
            exit(7)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 205, "msg": "cant get cstor pool info"}, "data": {}}
        exit(8)

def check_cstor_pool_not_exist(pool):
    try:
        if not is_cstor_pool_exist(pool):
            print {"result": {"code": 206, "msg": "cstor pool " + pool + " not exist"}, "data": {}}
            exit(11)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 205, "msg": "cant get cstor pool info"}, "data": {}}
        exit(8)

def check_virsh_disk_exist(pool, diskname):
    try:
        pool_info = get_pool_info(pool)
        if os.path.isdir(pool_info['path'] + '/' + diskname):
            print {"result": {"code": 207, "msg": "virsh disk " + diskname + " has exist in pool "+pool}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 208, "msg": "cant get virsh disk info, please check pool and disk is match or not"}, "data": {}}
        exit(6)

def check_virsh_disk_not_exist(pool, diskname):
    try:
        pool_info = get_pool_info(pool)
        if not os.path.isdir(pool_info['path']+'/'+diskname):
            print {"result": {"code": 209, "msg": "virsh disk " + diskname + " not exist in pool "+pool}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}}
        exit(6)

def check_virsh_disk_snapshot_not_exist(pool, diskname, snapshot):
    try:
        pool_info = get_pool_info(pool)
        if not os.path.exists(pool_info['path'] + '/' + diskname + '/snapshots/' + snapshot) and \
                not os.path.exists(pool_info['path'] + '/' + diskname + '/' + snapshot):
            print {"result": {"code": 209, "msg": "virsh disk snapshot " + snapshot + " not exist in volume "+diskname}, "data": {}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}}
        exit(6)

def check_cstor_disk_exist(pool, diskname):
    try:
        if is_cstor_disk_exist(pool, diskname):
            print {"result": {"code": 210, "msg": "cstor disk " + pool + " has exist in pool "+pool}, "data": {}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 211, "msg": "cant get cstor disk info"}, "data": {}}
        exit(8)

def check_cstor_disk_not_exist(pool, diskname):
    try:
        if not is_cstor_disk_exist(pool, diskname):
            print {"result": {"code": 212, "msg": "cstor disk " + pool + " not exist in pool "+pool}, "data": {}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 211, "msg": "cant get cstor disk info"}, "data": {}}
        exit(9)

def check_virsh_disk_size(pool, diskname, size):
    try:
        vol_xml = get_volume_xml(pool, diskname)
        result = loads(xmlToJson(vol_xml))
        if int(result["volume"]["capacity"]["text"]) >= int(size):
            print {"result": {"code": 213, "msg": "new disk size must larger than the old size."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 208, "msg": "cant get virsh disk info"}, "data": {}}
        exit(9)

def check_cstor_snapshot_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] == 0:
            print {"result": {"code": 214, "msg": "snapshot " + snapshot + " has exist."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 215, "msg": "cant get cstor snapshot info"}, "data": {}}
        exit(9)

def check_cstor_snapshot_not_exist(pool, vol, snapshot):
    try:
        op = Operation("cstor-cli vdisk-show-ss", {"poolname": pool, "name": vol, "sname": snapshot}, True)
        ssInfo = op.execute()
        if ssInfo['result']['code'] != 0:
            print {"result": {"code": 216, "msg": "snapshot " + snapshot + " not exist."}, "data": {}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {"result": {"code": 215, "msg": "cant get cstor snapshot info"}, "data": {}}
        exit(9)

def createPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type "+args.type+" not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.target is None:
            print {"result": {"code": 100, "msg": "less arg, target must be set"}, "data": {}}
            exit(9)
        if args.content is None:
            print {"result": {"code": 100, "msg": "less arg, content must be set"}, "data": {}}
            exit(9)
        if args.content not in ["vmd", "vmdi", "iso"]:
            print {"result": {"code": 100, "msg": "less arg, content just can be vmd, vmdi, iso"}, "data": {}}
            exit(9)
    if args.type == "uus" or args.type == "nfs" or args.type == "glusterfs":
        if args.url is None:
            print {"result": {"code": 100, "msg": "less arg, url must be set"}, "data": {}}
            exit(9)

    if args.type == "dir":
        check_virsh_pool_exist(args.pool)

    elif args.type == "uus" or args.type == "uraid":
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
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)


    if args.type == "dir":
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "nfs" or args.type == "glusterfs":
        # check pool type, if pool type not match, stop delete pool
        check_pool_type(args.pool, args.type)

        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    deletePool(args)

def startPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        print {"result": {"code": 500, "msg": "not support operation for uus or uraid"}, "data": {}}
        exit(3)

    startPool(args)

def autoStartPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        print {"result": {"code": 500, "msg": "not support operation for uus or uraid"}, "data": {}}
        exit(3)

    autoStartPool(args)

def unregisterPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        print {"result": {"code": 500, "msg": "not support operation for uus or uraid"}, "data": {}}
        exit(3)

    unregisterPool(args)

def stopPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        print {"result": {"code": 500, "msg": "not support operation for uus or uraid"}, "data": {}}
        exit(3)

    stopPool(args)

def showPoolParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.type == "dir":
        check_virsh_pool_not_exist(args.pool)

    elif args.type == "uus" or args.type == "uraid":
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "nfs" or args.type == "glusterfs":
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    showPool(args)

def createDiskParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.capacity is None:
            print {"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(4)
        if args.format is None:
            print {"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}}
            exit(4)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_exist(args.pool, args.vol)
        check_pool_type(args.pool, args.type)

    elif args.type == "uus" or args.type == "uraid":
        if args.capacity is None:
            print {"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(4)
        # check cstor disk
        check_cstor_pool_not_exist(args.pool)
        check_cstor_disk_exist(args.pool, args.vol)

    createDisk(args)

def deleteDiskParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    deleteDisk(args)

def resizeDiskParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        print
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        print
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.capacity is None:
        print {"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_size(args.pool, args.vol, args.capacity)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus" or args.type == "uraid":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    resizeDisk(args)

def cloneDiskParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.newname is None:
        print {"result": {"code": 100, "msg": "less arg, newname must be set"}, "data": {}}
        exit(3)
    if args.format is None:
        print {"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_virsh_disk_exist(args.pool, args.newname)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus" or args.type == "uraid":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)
        check_cstor_disk_exist(args.pool, args.newname)

    cloneDisk(args)

def showDiskParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_not_exist(args.pool, args.vol)

    elif args.type == "uus" or args.type == "uraid":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    showDisk(args)
    
def showDiskSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)
    if args.name is None:
        print {"result": {"code": 100, "msg": "less arg, name of snapshot must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

    elif args.type == "uus" or args.type == "uraid":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    showDiskSnapshot(args)

def createSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.backing_vol is None:
        print {"result": {"code": 100, "msg": "less arg, backing_vol must be set"}, "data": {}}
        exit(3)
    if args.snapshot is None:
        print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.capacity is None:
            print {"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}}
            exit(3)
        if args.backing_vol_format is None:
            print {"result": {"code": 100, "msg": "less arg, backing_vol_format must be set"}, "data": {}}
            exit(3)
        if args.format is None:
            print {"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.snapshot)
        check_virsh_disk_exist(args.pool, args.backing_vol)
    elif args.type == "uus" or args.type == "uraid":
        # check cstor disk
        check_cstor_snapshot_exist(args.pool, args.backing_vol, args.snapshot)

    createSnapshot(args)

def deleteSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 100, "msg": "not support value type, " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.snapshot is None:
            print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.snapshot)
    elif args.type == "uus":
        if args.backing_vol is None:
            print {"result": {"code": 100, "msg": "less arg, backing_vol must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.backing_vol, args.snapshot)

    deleteSnapshot(args)

def revertSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        print {"result": {"code": 100, "msg": "not support operation"}, "data": {}}
        exit(3)
    elif args.type == "uus":
        if args.pool is None:
            print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
            exit(3)
        if args.backing_vol is None:
            print {"result": {"code": 100, "msg": "less arg, backing_vol must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.backing_vol, args.snapshot)

    revertSnapshot(args)

def showSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.snapshot is None:
            print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
            exit(3)
        check_virsh_disk_not_exist(args.pool, args.snapshot)
    elif args.type == "uus":
        if args.backing_vol is None:
            print {"result": {"code": 100, "msg": "less arg, backing_vol must be set"}, "data": {}}
            exit(3)
        if args.snapshot is None:
            print {"result": {"code": 100, "msg": "less arg, snapshot must be set"}, "data": {}}
            exit(3)
        # check cstor disk
        check_cstor_snapshot_not_exist(args.pool, args.backing_vol, args.snapshot)

    showSnapshot(args)

def createExternalSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)
    if args.name is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.format is None:
            print {"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}}
            exit(3)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        config_path = disk_dir + '/config.json'
        with open(config_path, "r") as f:
            config = load(f)
        if not os.path.isfile(config['current']):
            print {"result": {"code": 100, "msg": "can not find vol"}, "data": {}}
            exit(3)
        if os.path.isfile(disk_dir + '/snapshots/' + args.name):
            print {"result": {"code": 100, "msg": "snapshot file has exist"}, "data": {}}
            exit(3)
    elif args.type == "uus" or args.type == "uraid":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or uraid"}, "data": {}})
        exit(1)

    createExternalSnapshot(args)

def revertExternalSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)
    if args.name is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.backing_file is None:
        print {"result": {"code": 100, "msg": "less arg, backing_file must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        if args.format is None:
            print {"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}}
            exit(3)

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        config_path = disk_dir + '/config.json'
        with open(config_path, "r") as f:
            config = load(f)

        if args.backing_file == config['current']:
            print {"result": {"code": 100, "msg": "can not revert disk to itself"}, "data": {}}
            exit(3)
        if not os.path.isfile(config['current']):
            print {"result": {"code": 100, "msg": "can not find current file"}, "data": {}}
            exit(3)
        if not os.path.isfile(args.backing_file):
            print {"result": {"code": 100, "msg": "snapshot file not exist"}, "data": {}}
            exit(3)

    elif args.type == "uus" or args.type == "uraid":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    revertExternalSnapshot(args)

def deleteExternalSnapshotParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.pool is None:
        print {"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}}
        exit(3)
    if args.vol is None:
        print {"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}}
        exit(3)
    if args.name is None:
        print {"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}}
        exit(3)
    if args.backing_file is None:
        print {"result": {"code": 100, "msg": "less arg, backing_file must be set"}, "data": {}}
        exit(3)

    if args.type == "nfs" or args.type == "glusterfs":
        check_cstor_pool_not_exist(args.pool)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":

        disk_dir = get_pool_info(args.pool)['path'] + '/' + args.vol
        ss_path = disk_dir + '/snapshots/' + args.name
        if not os.path.isfile(ss_path):
            print {"result": {"code": 100, "msg": "snapshot file not exist"}, "data": {}}
            exit(3)

    elif args.type == "uus" or args.type == "uraid":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    deleteExternalSnapshot(args)

def updateDiskCurrentParser(args):
    if args.type is None:
        print {"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}}
        exit(1)
    if args.type not in ["dir", "uus", "nfs", "glusterfs", "uraid"]:
        print {"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}}
        exit(2)
    if args.current is None:
        print {"result": {"code": 100, "msg": "less arg, current must be set"}, "data": {}}
        exit(3)

    if args.type == "dir" or args.type == "nfs" or args.type == "glusterfs":
        for current in args.current:
            if not os.path.isfile(current):
                print {"result": {"code": 100, "msg": "current" + current + " file not exist"}, "data": {}}
                exit(3)

    elif args.type == "uus" or args.type == "uraid":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    updateDiskCurrent(args)

def customizeParser(args):
    if args.add is None:
        print {"result": {"code": 100, "msg": "less arg, add must be set"}, "data": {}}
        exit(3)
    if args.user is None:
        print {"result": {"code": 100, "msg": "less arg, user must be set"}, "data": {}}
        exit(3)
    if args.password is None:
        print {"result": {"code": 100, "msg": "less arg, password must be set"}, "data": {}}
        exit(3)

    customize(args)

# --------------------------- cmd line parser ---------------------------------------
parser = argparse.ArgumentParser(prog="kubesds-adm", description="All storage adaptation tools")

subparsers = parser.add_subparsers(help="sub-command help")

# -------------------- add createPool cmd ----------------------------------
parser_create_pool = subparsers.add_parser("createPool", help="createPool help")
parser_create_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_create_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--url", metavar="[URL]", type=str,
                                help="storage pool create location, only for uus")

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--target", metavar="[TARGET]", type=str,
                                help="storage pool create location, only for dir, nfs and glusterfs")
# set autostart
parser_create_pool.add_argument("--autostart", metavar="[AUTOSTART]", type=bool, nargs='?', const=True,
                                help="if autostart, pool will set autostart yes after create pool")

# set content
parser_create_pool.add_argument("--content", metavar="[CONTENT]", type=str,
                                help="pool content")

# nfs only
parser_create_pool.add_argument("--opt", metavar="[OPT]", type=str,
                                help="nfs mount options, only for nfs")


# set default func
parser_create_pool.set_defaults(func=createPoolParser)

# -------------------- add deletePool cmd ----------------------------------
parser_delete_pool = subparsers.add_parser("deletePool", help="deletePool help")
parser_delete_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_delete_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")
# set default func
parser_delete_pool.set_defaults(func=deletePoolParser)

# -------------------- add startPool cmd ----------------------------------
parser_start_pool = subparsers.add_parser("startPool", help="startPool help")
parser_start_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_start_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")
# set default func
parser_start_pool.set_defaults(func=startPoolParser)

# -------------------- add autoStartPool cmd ----------------------------------
parser_autostart_pool = subparsers.add_parser("autoStartPool", help="autoStartPool help")
parser_autostart_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_autostart_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to autostart")
parser_autostart_pool.add_argument("--disable", metavar="[DISABLE]", type=bool, nargs='?',  const=True,
                                help="disable autostart")

# set default func
parser_autostart_pool.set_defaults(func=autoStartPoolParser)

# -------------------- add unregisterPool cmd ----------------------------------
parser_unregister_pool = subparsers.add_parser("unregisterPool", help="unregisterPool help")
parser_unregister_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_unregister_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to unregister")
# set default func
parser_unregister_pool.set_defaults(func=unregisterPoolParser)

# -------------------- add stopPool cmd ----------------------------------
parser_stop_pool = subparsers.add_parser("stopPool", help="stopPool help")
parser_stop_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_stop_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to stop")
# set default func
parser_stop_pool.set_defaults(func=stopPoolParser)

# -------------------- add showPool cmd ----------------------------------
parser_show_pool = subparsers.add_parser("showPool", help="showPool help")
parser_show_pool.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")

parser_show_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to show")
# set default func
parser_show_pool.set_defaults(func=showPoolParser)

# -------------------- add createDisk cmd ----------------------------------
parser_create_disk = subparsers.add_parser("createDisk", help="createDisk help")
parser_create_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
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

# parser_create_disk.add_argument("--backing_vol", metavar="[BACKING_VOL]", type=str,
#                                 help="disk backing vol to use")
# parser_create_disk.add_argument("--backing_vol_format", metavar="[BSCKING_VOL_FORMAT]", type=str,
#                                 help="disk backing vol format to use")

# set default func
parser_create_disk.set_defaults(func=createDiskParser)

# -------------------- add deleteDisk cmd ----------------------------------
parser_delete_disk = subparsers.add_parser("deleteDisk", help="deleteDisk help")
parser_delete_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_delete_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_delete_disk.set_defaults(func=deleteDiskParser)


# -------------------- add resizeDisk cmd ----------------------------------
parser_resize_disk = subparsers.add_parser("resizeDisk", help="resizeDisk help")
parser_resize_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
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
parser_clone_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_clone_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_clone_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_clone_disk.add_argument("--newname", metavar="[NEWNAME]", type=str,
                                help="new volume name to use")
parser_clone_disk.add_argument("--format", metavar="[FORMAT]", type=str,
                                help="format to use")
# set default func
parser_clone_disk.set_defaults(func=cloneDiskParser)

# -------------------- add showDisk cmd ----------------------------------
parser_show_disk = subparsers.add_parser("showDisk", help="showDisk help")
parser_show_disk.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_show_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_show_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_show_disk.set_defaults(func=showDiskParser)

# -------------------- add showDiskSnapshot cmd ----------------------------------
parser_show_disk_snapshot = subparsers.add_parser("showDiskSnapshot", help="showDiskSnapshot help")
parser_show_disk_snapshot.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_show_disk_snapshot.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_show_disk_snapshot.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
parser_show_disk_snapshot.add_argument("--name", metavar="[NAME]", type=str,
                                help="volume snapshot name")
# set default func
parser_show_disk_snapshot.set_defaults(func=showDiskSnapshotParser)


# -------------------- add createSnapshot cmd ----------------------------------
parser_create_ss = subparsers.add_parser("createSnapshot", help="createSnapshot help")
parser_create_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_create_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_create_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_create_ss.add_argument("--capacity", metavar="[CAPACITY]", type=str,
                                help="disk capacity to use")
parser_create_ss.add_argument("--format", metavar="[FORMAT]", type=str,
                                help="disk format to use")
parser_create_ss.add_argument("--backing_vol", metavar="[BACKING_VOL]", type=str,
                                help="disk backing vol to use")
parser_create_ss.add_argument("--backing_vol_format", metavar="[BSCKING_VOL_FORMAT]", type=str,
                                help="disk backing vol format to use")
parser_create_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_create_ss.set_defaults(func=createSnapshotParser)

# -------------------- add deleteSnapshot cmd ----------------------------------
parser_delete_ss = subparsers.add_parser("deleteSnapshot", help="deleteSnapshot help")
parser_delete_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_delete_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_ss.add_argument("--backing_vol", metavar="[BACKING_VOL]", type=str,
                                help="volume name to use")
parser_delete_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_delete_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_delete_ss.set_defaults(func=deleteSnapshotParser)


# -------------------- add recoverySnapshot cmd ----------------------------------
parser_revert_ss = subparsers.add_parser("recoverySnapshot", help="recoverySnapshot help")
parser_revert_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_revert_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_revert_ss.add_argument("--backing_vol", metavar="[BACKING_VOL]", type=str,
                                help="volume name to use")
parser_revert_ss.add_argument("--snapshot", metavar="[SNAPSHOT]", type=str,
                                help="volume snapshot name to use")
parser_revert_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_revert_ss.set_defaults(func=revertSnapshotParser)

# -------------------- add showSnapshot cmd ----------------------------------
parser_show_ss = subparsers.add_parser("showSnapshot", help="showSnapshot help")
parser_show_ss.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_show_ss.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_show_ss.add_argument("--backing_vol", metavar="[BACKING_VOL]", type=str,
                                help="volume name to use")
parser_show_ss.add_argument("--name", metavar="[NAME]", type=str,
                                help="volume snapshot name to use")
parser_show_ss.add_argument("--vmname", metavar="[VMNAME]", type=str,
                                help="virtual machine name to use")
# set default func
parser_show_ss.set_defaults(func=showSnapshotParser)


# -------------------- add createExternalSnapshot cmd ----------------------------------
parser_create_ess = subparsers.add_parser("createExternalSnapshot", help="createExternalSnapshot help")
parser_create_ess.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_create_ess.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_create_ess.add_argument("--name", metavar="[NAME]", type=str,
                                help="volume snapshot name to use")
parser_create_ess.add_argument("--format", metavar="[FORMAT]", type=str,
                                help="disk format to use")
parser_create_ess.add_argument("--vol", metavar="[VOL]", type=str,
                                help="disk current file to use")
parser_create_ess.add_argument("--domain", metavar="[domain]", type=str,
                                help="domain")
# set default func
parser_create_ess.set_defaults(func=createExternalSnapshotParser)

# -------------------- add revertExternalSnapshot cmd ----------------------------------
parser_revert_ess = subparsers.add_parser("revertExternalSnapshot", help="revertExternalSnapshot help")
parser_revert_ess.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_revert_ess.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_revert_ess.add_argument("--name", metavar="[NAME]", type=str,
                                help="volume snapshot name to use")
parser_revert_ess.add_argument("--vol", metavar="[VOL]", type=str,
                                help="disk current file to use")
parser_revert_ess.add_argument("--backing_file", metavar="[backing_file]", type=str,
                                help="backing_file from k8s")
parser_revert_ess.add_argument("--format", metavar="[FORMAT]", type=str,
                                help="disk format to use")
# set default func
parser_revert_ess.set_defaults(func=revertExternalSnapshotParser)

# -------------------- add deleteExternalSnapshot cmd ----------------------------------
parser_delete_ess = subparsers.add_parser("deleteExternalSnapshot", help="deleteExternalSnapshot help")
parser_delete_ess.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_delete_ess.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_ess.add_argument("--name", metavar="[NAME]", type=str,
                                help="volume snapshot name to use")
parser_delete_ess.add_argument("--vol", metavar="[VOL]", type=str,
                                help="disk current file to use")
parser_delete_ess.add_argument("--backing_file", metavar="[backing_file]", type=str,
                                help="backing_file from k8s")
parser_delete_ess.add_argument("--domain", metavar="[domain]", type=str,
                                help="domain")
# set default func
parser_delete_ess.set_defaults(func=deleteExternalSnapshotParser)

# -------------------- add updateDiskCurrent cmd ----------------------------------
parser_upodate_current = subparsers.add_parser("updateDiskCurrent", help="updateDiskCurrent help")
parser_upodate_current.add_argument("--type", metavar="[dir|uus|nfs|glusterfs|uraid]", type=str,
                                help="storage pool type to use")
parser_upodate_current.add_argument("--current", metavar="[CURRENT]", type=str, nargs='*',
                                help="disk current file to use")
# set default func
parser_upodate_current.set_defaults(func=updateDiskCurrentParser)

# -------------------- add customize cmd ----------------------------------
parser_customize_current = subparsers.add_parser("customize", help="customize help")
parser_customize_current.add_argument("--add", metavar="[ADD]", type=str,
                                help="storage pool type to use")
parser_customize_current.add_argument("--user", metavar="[USER]", type=str,
                                help="disk current file to use")
parser_customize_current.add_argument("--password", metavar="[PASSWORD]", type=str,
                                help="disk current file to use")
# set default func
parser_customize_current.set_defaults(func=customizeParser)

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


# try:
    # args = parser.parse_args(["createPool", "--type", "dir", "--pool", "pooldir", "--target", "/var/lib/libvirt/pooldir"])
    # args.func(args)

    # args = parser.parse_args(
    #     ["createDisk", "--type", "dir", "--pool", "pooltest", "--vol", "disktest", "--capacity", "10737418240", "--format", "qcow2"])
    # args.func(args)
    #
    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "dir", "--pool", "pooltest", "--format", "qcow2", "--name", "ss1", "--vol", "disktest"])
    # args.func(args)
    #
    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "dir", "--pool", "pooltest", "--format", "qcow2", "--name", "ss2",
    #      "--vol", "disktest"])
    # args.func(args)
    # args = parser.parse_args(
    #     ["createExternalSnapshot", "--type", "dir", "--pool", "pooltest", "--format", "qcow2", "--name", "ss3",
    #      "--vol", "disktest"])
    # args.func(args)
    #
    # args = parser.parse_args(
    #     ["revertExternalSnapshot", "--type", "dir", "--pool", "pooltest", "--name", "ss1",
    #      "--vol", "disktest", "--format", "qcow2"])
    # args.func(args)

    # args = parser.parse_args(
    #     ["deleteExternalSnapshot", "--type", "dir", "--pool", "pooltest", "--name", "ss1",
    #      "--vol", "disktest"])
    # args.func(args)
    # args = parser.parse_args(
    #     ["updateDiskCurrent", "--type", "dir", "--current", "/var/lib/libvirt/pooltest/disktest/ss2"])
    # args.func(args)
# except TypeError:
#     print dumps({"result": {"code": 1, "msg": "script error, plz check log file."}, "data": {}})
#     logger.debug(traceback.format_exc())
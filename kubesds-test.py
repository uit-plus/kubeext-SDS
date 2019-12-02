import argparse

from operation import *
from utils import logger

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)


def get_cstor_pool_info(pool):
    op = Operation("cstor-cli pool-show", {"poolname": pool}, with_result=True)
    result = op.execute()
    return result


def check_pool_type(pool, type):
    pool_info = get_pool_info(pool)
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
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.url is None:
        print dumps({"result": {"code": 100, "msg": "less arg, url must be set"}, "data": {}})
        exit(9)

    if args.type == "uus" or args.type == "nfs":
        if args.opt is None:
            print dumps({"result": {"code": 100, "msg": "less arg, opt must be set"}, "data": {}})
            exit(9)

    if args.type == "nfs" or args.type == "glusterfs":
        if args.uuid is None:
            print dumps({"result": {"code": 100, "msg": "less arg, uuid must be set"}, "data": {}})
            exit(9)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        # if args.target is None:
        #     print dumps({"result": {"code": 100, "msg": "less arg, target must be set"}, "data": {}}
        #     exit(9)
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

    elif args.type == "uus":
        # check cstor pool
        check_cstor_pool_exist(args.pool)

    createPool(args)


def deletePoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        # check pool type, if pool type not match, stop delete pool
        check_pool_type(args.pool, args.type)

        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)
    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    deletePool(args)


def startPoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    startPool(args)


def autoStartPoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    autoStartPool(args)


def unregisterPoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    unregisterPool(args)


def stopPoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        # if pool type is nfs or gluster, maybe cause virsh pool delete but cstor pool still exist
        check_pool_type(args.pool, args.type)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus or vdiskfs"}, "data": {}})
        exit(3)

    stopPool(args)


def showPoolParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        # check virsh pool, only for localfs, vdiskfs, nfs and glusterfs
        check_virsh_pool_not_exist(args.pool)
        # check cstor pool
        check_cstor_pool_not_exist(args.pool)

    elif args.type == "uus":
        check_cstor_pool_not_exist(args.pool)

    showPool(args)


def createDiskParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}})
        exit(3)
    if args.capacity is None:
        print dumps({"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}})
        exit(4)

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

    createDisk(args)


def deleteDiskParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)
        check_pool_type(args.pool, args.type)
    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    deleteDisk(args)


def resizeDiskParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        print
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        print
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)
    if args.capacity is None:
        print dumps({"result": {"code": 100, "msg": "less arg, capacity must be set"}, "data": {}})
        exit(3)

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

    resizeDisk(args)


def cloneDiskParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)
    if args.newname is None:
        print dumps({"result": {"code": 100, "msg": "less arg, newname must be set"}, "data": {}})
        exit(3)
    if args.format is None:
        print dumps({"result": {"code": 100, "msg": "less arg, format must be set"}, "data": {}})
        exit(3)

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

    cloneDisk(args)


def showDiskParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_not_exist(args.pool, args.vol)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    showDisk(args)


def showDiskSnapshotParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}})
        exit(3)
    if args.name is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name of snapshot must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        check_cstor_pool_not_exist(args.pool)
        check_virsh_pool_not_exist(args.pool)
        check_virsh_disk_snapshot_not_exist(args.pool, args.vol, args.name)

    elif args.type == "uus":
        # check cstor disk
        check_cstor_disk_not_exist(args.pool, args.vol)

    showDiskSnapshot(args)


def createExternalSnapshotParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}})
        exit(3)
    if args.name is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)

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

    createExternalSnapshot(args)


def revertExternalSnapshotParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}})
        exit(3)
    if args.name is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)
    if args.backing_file is None:
        print dumps({"result": {"code": 100, "msg": "less arg, backing_file must be set"}, "data": {}})
        exit(3)

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

    revertExternalSnapshot(args)


def deleteExternalSnapshotParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.pool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, pool must be set"}, "data": {}})
        exit(3)
    if args.vol is None:
        print dumps({"result": {"code": 100, "msg": "less arg, vol must be set"}, "data": {}})
        exit(3)
    if args.name is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)
    if args.backing_file is None:
        print dumps({"result": {"code": 100, "msg": "less arg, backing_file must be set"}, "data": {}})
        exit(3)

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

    deleteExternalSnapshot(args)


def updateDiskCurrentParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.current is None:
        print dumps({"result": {"code": 100, "msg": "less arg, current must be set"}, "data": {}})
        exit(3)

    if args.type == "localfs" or args.type == "nfs" or args.type == "glusterfs" or args.type == "vdiskfs":
        for current in args.current:
            if not os.path.isfile(current):
                print dumps({"result": {"code": 100, "msg": "current" + current + " file not exist"}, "data": {}})
                exit(3)

    elif args.type == "uus":
        print dumps({"result": {"code": 500, "msg": "not support operation for uus"}, "data": {}})
        exit(1)

    updateDiskCurrent(args)


def customizeParser(args):
    if args.add is None:
        print dumps({"result": {"code": 100, "msg": "less arg, add must be set"}, "data": {}})
        exit(3)
    if args.user is None:
        print dumps({"result": {"code": 100, "msg": "less arg, user must be set"}, "data": {}})
        exit(3)
    if args.password is None:
        print dumps({"result": {"code": 100, "msg": "less arg, password must be set"}, "data": {}})
        exit(3)

    customize(args)


def createDiskFromImageParser(args):
    if args.type is None:
        print dumps({"result": {"code": 100, "msg": "less arg type must be set"}, "data": {}})
        exit(1)
    if args.type not in ["localfs", "uus", "nfs", "glusterfs", "vdiskfs"]:
        print dumps({"result": {"code": 100, "msg": "not support value type " + args.type + " not support"}, "data": {}})
        exit(2)
    if args.targetPool is None:
        print dumps({"result": {"code": 100, "msg": "less arg, targetPool must be set"}, "data": {}})
        exit(3)
    if args.name is None:
        print dumps({"result": {"code": 100, "msg": "less arg, name must be set"}, "data": {}})
        exit(3)
    if args.source is None:
        print dumps({"result": {"code": 100, "msg": "less arg, source must be set"}, "data": {}})
        exit(3)


def migrateParser(args):
    if args.ip is None:
        print dumps({"result": {"code": 100, "msg": "less arg, ip must be set"}, "data": {}})
        exit(3)
    if not re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$', args.ip):
        print dumps({"result": {"code": 100, "msg": "ip is not right"}, "data": {}})
        exit(3)
    migrate(args)


# --------------------------- cmd line parser ---------------------------------------
parser = argparse.ArgumentParser(prog="kubesds-adm", description="All storage adaptation tools")

subparsers = parser.add_subparsers(help="sub-command help")

# -------------------- add createPool cmd ----------------------------------
parser_create_pool = subparsers.add_parser("createPool", help="createPool help")
parser_create_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")

parser_create_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")

# localfs, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument("--url", metavar="[URL]", type=str,
                                help="storage pool create location, only for uus")

# # localfs, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
# parser_create_pool.add_argument("--target", metavar="[TARGET]", type=str,
#                                 help="storage pool create location, only for localfs, nfs and glusterfs")
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
parser_delete_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")

parser_delete_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool name to delete")
# set default func
parser_delete_pool.set_defaults(func=deletePoolParser)

# -------------------- add startPool cmd ----------------------------------
parser_start_pool = subparsers.add_parser("startPool", help="startPool help")
parser_start_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                               help="storage pool type to use")

parser_start_pool.add_argument("--pool", metavar="[POOL]", type=str,
                               help="storage pool name to delete")
# set default func
parser_start_pool.set_defaults(func=startPoolParser)

# -------------------- add autoStartPool cmd ----------------------------------
parser_autostart_pool = subparsers.add_parser("autoStartPool", help="autoStartPool help")
parser_autostart_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                   help="storage pool type to use")

parser_autostart_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                   help="storage pool name to autostart")
parser_autostart_pool.add_argument("--disable", metavar="[DISABLE]", type=bool, nargs='?', const=True,
                                   help="disable autostart")

# set default func
parser_autostart_pool.set_defaults(func=autoStartPoolParser)

# -------------------- add unregisterPool cmd ----------------------------------
parser_unregister_pool = subparsers.add_parser("unregisterPool", help="unregisterPool help")
parser_unregister_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                    help="storage pool type to use")

parser_unregister_pool.add_argument("--pool", metavar="[POOL]", type=str,
                                    help="storage pool name to unregister")
# set default func
parser_unregister_pool.set_defaults(func=unregisterPoolParser)

# -------------------- add stopPool cmd ----------------------------------
parser_stop_pool = subparsers.add_parser("stopPool", help="stopPool help")
parser_stop_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")

parser_stop_pool.add_argument("--pool", metavar="[POOL]", type=str,
                              help="storage pool name to stop")
# set default func
parser_stop_pool.set_defaults(func=stopPoolParser)

# -------------------- add showPool cmd ----------------------------------
parser_show_pool = subparsers.add_parser("showPool", help="showPool help")
parser_show_pool.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")

parser_show_pool.add_argument("--pool", metavar="[POOL]", type=str,
                              help="storage pool name to show")
# set default func
parser_show_pool.set_defaults(func=showPoolParser)

# -------------------- add createDisk cmd ----------------------------------
parser_create_disk = subparsers.add_parser("createDisk", help="createDisk help")
parser_create_disk.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
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
parser_delete_disk.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                help="storage pool type to use")
parser_delete_disk.add_argument("--pool", metavar="[POOL]", type=str,
                                help="storage pool to use")
parser_delete_disk.add_argument("--vol", metavar="[VOL]", type=str,
                                help="volume name to use")
# set default func
parser_delete_disk.set_defaults(func=deleteDiskParser)

# -------------------- add resizeDisk cmd ----------------------------------
parser_resize_disk = subparsers.add_parser("resizeDisk", help="resizeDisk help")
parser_resize_disk.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
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
parser_clone_disk.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
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
parser_show_disk.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                              help="storage pool type to use")
parser_show_disk.add_argument("--pool", metavar="[POOL]", type=str,
                              help="storage pool to use")
parser_show_disk.add_argument("--vol", metavar="[VOL]", type=str,
                              help="volume name to use")
# set default func
parser_show_disk.set_defaults(func=showDiskParser)

# -------------------- add showDiskSnapshot cmd ----------------------------------
parser_show_disk_snapshot = subparsers.add_parser("showDiskSnapshot", help="showDiskSnapshot help")
parser_show_disk_snapshot.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                       help="storage pool type to use")
parser_show_disk_snapshot.add_argument("--pool", metavar="[POOL]", type=str,
                                       help="storage pool to use")
parser_show_disk_snapshot.add_argument("--vol", metavar="[VOL]", type=str,
                                       help="volume name to use")
parser_show_disk_snapshot.add_argument("--name", metavar="[NAME]", type=str,
                                       help="volume snapshot name")
# set default func
parser_show_disk_snapshot.set_defaults(func=showDiskSnapshotParser)

# -------------------- add createExternalSnapshot cmd ----------------------------------
parser_create_ess = subparsers.add_parser("createExternalSnapshot", help="createExternalSnapshot help")
parser_create_ess.add_argument("--type", metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
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
parser_revert_ess.add_argument("--type", metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
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
parser_revert_ess.add_argument("--domain", metavar="[domain]", type=str,
                               help="domain")
# set default func
parser_revert_ess.set_defaults(func=revertExternalSnapshotParser)

# -------------------- add deleteExternalSnapshot cmd ----------------------------------
parser_delete_ess = subparsers.add_parser("deleteExternalSnapshot", help="deleteExternalSnapshot help")
parser_delete_ess.add_argument("--type", metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
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
parser_upodate_current.add_argument("--type", metavar="[localfs|uus|nfs|glusterfs|vdiskfs]", type=str,
                                    help="storage pool type to use")
parser_upodate_current.add_argument("--current", metavar="[CURRENT]", type=str, nargs='*',
                                    help="disk current file to use")
# set default func
parser_upodate_current.set_defaults(func=updateDiskCurrentParser)

# -------------------- add customize cmd ----------------------------------
parser_customize = subparsers.add_parser("customize", help="customize help")
parser_customize.add_argument("--add", metavar="[ADD]", type=str,
                              help="storage pool type to use")
parser_customize.add_argument("--user", metavar="[USER]", type=str,
                              help="disk current file to use")
parser_customize.add_argument("--password", metavar="[PASSWORD]", type=str,
                              help="disk current file to use")
# set default func
parser_customize.set_defaults(func=customizeParser)

# -------------------- add createDiskFromImage cmd ----------------------------------
parser_create_disk_from_image = subparsers.add_parser("createDiskFromImage", help="createDiskFromImage help")
parser_create_disk_from_image.add_argument("--type", metavar="[localfs|nfs|glusterfs|vdiskfs]", type=str,
                                           help="storage pool type to use")
parser_create_disk_from_image.add_argument("--name", metavar="[name]", type=str,
                                           help="new disk name to use")
parser_create_disk_from_image.add_argument("--targetPool", metavar="[targetPool]", type=str,
                                           help="storage pool to use")
parser_create_disk_from_image.add_argument("--source", metavar="[source]", type=str,
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

test_args = []

dir1 = parser.parse_args(["createPool", "--type", "localfs", "--pool", "pooldir", "--url", "/mnt/localfs/pooldir", "--content", "vmd"])
dir2 = parser.parse_args(["createDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "1073741824", "--format", "qcow2"])
dir3 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--format", "qcow2"])
dir4 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.2", "--format", "qcow2"])
dir5 = parser.parse_args(["revertExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--format", "qcow2", "--backing_file", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir6 = parser.parse_args(["createExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.3", "--format", "qcow2"])
dir7 = parser.parse_args(["deleteExternalSnapshot", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--name", "diskdir.1", "--backing_file", "/mnt/localfs/pooldir/pooldir/diskdir/diskdir"])
dir8 = parser.parse_args(["resizeDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--capacity", "2147483648"])
dir9 = parser.parse_args(["cloneDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir", "--newname", "diskdirclone", "--format", "qcow2"])
dir10 = parser.parse_args(["deleteDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdirclone"])
dir11 = parser.parse_args(["deleteDisk", "--type", "localfs", "--pool", "pooldir", "--vol", "diskdir"])
dir12 = parser.parse_args(["stopPool", "--type", "localfs", "--pool", "pooldir"])
dir13 = parser.parse_args(["deletePool", "--type", "localfs", "--pool", "pooldir"])

uus1 = parser.parse_args(["createPool", "--type", "uus", "--pool", "pooldev", "--url", "192.168.3.100:p1", "--opt", "iscsi,username=admin,password=admin,port=7000"])
uus2 = parser.parse_args(["createDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "1073741824"])
# uus3 = parser.parse_args(["resizeDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--capacity", "2147483648"])
# uus4 = parser.parse_args(["cloneDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev", "--newname", "diskdevclone"])
uus5 = parser.parse_args(["deleteDisk", "--type", "uus", "--pool", "pooldev", "--vol", "diskdev"])
uus6 = parser.parse_args(["deletePool", "--type", "uus", "--pool", "pooldev"])


vdiskfs1 = parser.parse_args(["createPool", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--url", "local", "--content", "vmd"])
vdiskfs2 = parser.parse_args(["createDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--capacity", "1073741824", "--format", "qcow2"])
vdiskfs3 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--format", "qcow2"])
vdiskfs4 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.2", "--format", "qcow2"])
vdiskfs5 = parser.parse_args(["revertExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--format", "qcow2", "--backing_file", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs6 = parser.parse_args(["createExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.3", "--format", "qcow2"])
vdiskfs7 = parser.parse_args(["deleteExternalSnapshot", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--name", "diskvdiskfs.1", "--backing_file", "/mnt/usb/local/poolvdiskfs/diskvdiskfs/diskvdiskfs"])
vdiskfs8 = parser.parse_args(["resizeDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--capacity", "2147483648"])
vdiskfs9 = parser.parse_args(["cloneDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs", "--newname", "diskvdiskfsclone", "--format", "qcow2"])
vdiskfs10 = parser.parse_args(["deleteDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfsclone"])
vdiskfs11 = parser.parse_args(["deleteDisk", "--type", "vdiskfs", "--pool", "poolvdiskfs", "--vol", "diskvdiskfs"])
vdiskfs12 = parser.parse_args(["stopPool", "--type", "vdiskfs", "--pool", "poolvdiskfs"])
vdiskfs13 = parser.parse_args(["deletePool", "--type", "vdiskfs", "--pool", "poolvdiskfs"])

nfs1 = parser.parse_args(["createPool", "--type", "nfs", "--pool", "poolnfs", "--url", "133.133.135.30:/home/nfs", "--opt", "nolock", "--content", "vmd", "--path", "abc", "--uuid", "07098ca5-fd17-4fcc-afee-76b0d7fccde4"])
nfs2 = parser.parse_args(["createDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "1073741824", "--format", "qcow2"])
nfs3 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--format", "qcow2"])
nfs4 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.2", "--format", "qcow2"])
nfs5 = parser.parse_args(["revertExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--format", "qcow2", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5-fd17-4fcc-afee-76b0d7fccde4/disknfs/disknfs"])
nfs6 = parser.parse_args(["createExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.3", "--format", "qcow2"])
nfs7 = parser.parse_args(["deleteExternalSnapshot", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--name", "disknfs.1", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5-fd17-4fcc-afee-76b0d7fccde4/disknfs/disknfs"])
nfs8 = parser.parse_args(["resizeDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--capacity", "2147483648"])
nfs9 = parser.parse_args(["cloneDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs", "--newname", "disknfsclone", "--format", "qcow2"])
nfs10 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfsclone"])
nfs11 = parser.parse_args(["deleteDisk", "--type", "nfs", "--pool", "poolnfs", "--vol", "disknfs"])
nfs12 = parser.parse_args(["stopPool", "--type", "nfs", "--pool", "poolnfs"])
nfs13 = parser.parse_args(["deletePool", "--type", "nfs", "--pool", "poolnfs"])

gfs1 = parser.parse_args(["createPool", "--type", "glusterfs", "--pool", "poolglusterfs", "--url", "192.168.3.100:nfsvol", "--content", "vmd", "--path", "abc", "--uuid", "07098ca5-fd17-4fcc-afee-76b0d7fccde4"])
gfs2 = parser.parse_args(["createDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "1073741824", "--format", "qcow2"])
gfs3 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--format", "qcow2"])
gfs4 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.2", "--format", "qcow2"])
gfs5 = parser.parse_args(["revertExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--format", "qcow2", "--backing_file", "/var/lib/libvirt/cstor/abc/07098ca5-fd17-4fcc-afee-76b0d7fccde4/diskglusterfs/diskglusterfs"])
gfs6 = parser.parse_args(["createExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.3", "--format", "qcow2"])
gfs7 = parser.parse_args(["deleteExternalSnapshot", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--name", "diskglusterfs.1", "--backing_file", "/var/lib/libvirt/cstor/poolglusterfs/abc/07098ca5-fd17-4fcc-afee-76b0d7fccde4/diskglusterfs"])
gfs8 = parser.parse_args(["resizeDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--capacity", "2147483648"])
gfs9 = parser.parse_args(["cloneDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs", "--newname", "diskglusterfsclone", "--format", "qcow2"])
gfs10 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfsclone"])
gfs11 = parser.parse_args(["deleteDisk", "--type", "glusterfs", "--pool", "poolglusterfs", "--vol", "diskglusterfs"])
gfs12 = parser.parse_args(["stopPool", "--type", "glusterfs", "--pool", "poolglusterfs"])
gfs13 = parser.parse_args(["deletePool", "--type", "glusterfs", "--pool", "poolglusterfs"])

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


for args in test_args:
    try:
        args.func(args)
    except TypeError:
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
    #     ["createDiskFromImage", "--type", "localfs", "--targetPool", "vmdi", "--name", "vm006copy", "--source", "/mnt/localfs/sdb/vmdi/vm006/vm006", "--full_copy"])
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
#     logger.debug(traceback.format_exc())
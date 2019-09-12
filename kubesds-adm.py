import argparse
import os
import traceback
from sys import exit

from operation import *
from utils import logger
from utils.exception import *
from utils.exception import ExecuteException
from utils.libvirt_util import is_pool_exists, is_volume_exists

LOG = 'kubesds.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)


def is_cstor_pool_exist(poolname):
    op = Operation('cstor-cli pool-show', {'poolname': poolname}, with_result=True)
    result = op.execute()
    if result['result']['code'] == 0:
        return True
    else:
        return False

def is_cstor_disk_exist(poolname, diskname):
    op = Operation('cstor-cli vdisk-show', {'poolname': poolname, 'name': diskname}, with_result=True)
    result = op.execute()
    if result['result']['code'] == 0:
        return True
    else:
        return False

def check_virsh_pool_exist(poolname):
    try:
        if is_pool_exists(poolname):
            print {'result': {'code': 5, 'msg': 'virsh pool ' + poolname + ' has exist'}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 6, 'msg': 'cant get cstor pool info'}}
        exit(6)

def check_virsh_pool_not_exist(poolname):
    try:
        if not is_pool_exists(poolname):
            print {'result': {'code': 5, 'msg': 'virsh pool ' + poolname + ' not exist'}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 6, 'msg': 'cant get cstor pool info'}}
        exit(6)

def check_cstor_pool_exist(poolname):
    try:
        if is_cstor_pool_exist(poolname):
            print {'result': {'code': 7, 'msg': 'cstor pool ' + poolname + ' has exist'}}
            exit(7)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 8, 'msg': 'cant get cstor pool info'}}
        exit(8)

def check_cstor_pool_not_exist(poolname):
    try:
        if not is_cstor_pool_exist(poolname):
            print {'result': {'code': 11, 'msg': 'cstor pool ' + poolname + ' not exist'}}
            exit(11)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 8, 'msg': 'cant get cstor pool info'}}
        exit(8)


def check_virsh_disk_exist(poolname, diskname):
    try:
        if is_volume_exists(diskname, poolname):
            print {'result': {'code': 13, 'msg': 'virsh disk ' + diskname + ' has exist in pool '+poolname}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 6, 'msg': 'cant get virsh disk info, does exist the pool '+poolname}}
        exit(6)

def check_virsh_disk_not_exist(poolname, diskname):
    try:
        if not is_volume_exists(diskname, poolname):
            print {'result': {'code': 14, 'msg': 'virsh disk ' + diskname + ' not exist in pool '+poolname}}
            exit(5)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 6, 'msg': 'cant get virsh disk info'}}
        exit(6)

def check_cstor_disk_exist(poolname, diskname):
    try:
        if is_cstor_disk_exist(poolname, diskname):
            print {'result': {'code': 15, 'msg': 'cstor disk ' + poolname + ' has exist in pool '+poolname}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 8, 'msg': 'cant get cstor disk info'}}
        exit(8)

def check_cstor_disk_not_exist(poolname, diskname):
    try:
        if not is_cstor_disk_exist(poolname, diskname):
            print {'result': {'code': 16, 'msg': 'cstor disk ' + poolname + ' not exist in pool '+poolname}}
            exit(15)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 9, 'msg': 'cant get cstor disk info'}}
        exit(9)

def check_virsh_disk_size(poolname, diskname, size):
    try:
        vol_xml = get_volume_xml(poolname, diskname)
        result = loads(xmlToJson(vol_xml))
        if int(result['volume']['capacity']['text']) >= int(size):
            print {'result': {'code': 4, 'msg': 'new cstor disk size must larger than the old size.'}}
            exit(4)
    except Exception:
        logger.debug(traceback.format_exc())
        print {'result': {'code': 9, 'msg': 'cant get virsh disk info'}}
        exit(9)

def createPoolParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type '+args.type+' not support'}}
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        exit(3)
    if args.type == 'dir' or args.type == 'nfs' or args.type == 'glusterfs':
        if args.target is None:
            print {'result': {'code': 9, 'msg': 'less arg, target must be set'}}
            exit(9)
    if args.type == 'uus' or args.type == 'nfs' or args.type == 'glusterfs':
        if args.url is None:
            print {'result': {'code': 9, 'msg': 'less arg, url must be set'}}
            exit(9)

    if args.type == 'dir':
        check_virsh_pool_exist(args.poolname)
        createPool('dir', {'poolname': args.poolname, 'type': 'dir', 'target': args.target})
    elif args.type == 'uus':
        # check cstor pool
        check_cstor_pool_exist(args.poolname)
        createPool(args.type, {'poolname': args.poolname, 'url': args.url})
    elif args.type == 'nfs':
        # check cstor pool
        check_cstor_pool_exist(args.poolname)
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_exist(args.poolname)
        if args.opt is None:
            createPool(args.type, {'poolname': args.poolname, 'url': args.url, 'path': args.target})
        else:
            createPool(args.type, {'poolname': args.poolname, 'url': args.url, 'path': args.target, 'opt': args.opt})
    elif args.type == 'glusterfs':
        # check cstor pool
        check_cstor_pool_exist(args.poolname)
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_exist(args.poolname)

        createPool(args.type, {'poolname': args.poolname, 'url': args.url, 'path': args.target})


def deletePoolParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type ' + args.type + ' not support'}}
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        exit(3)
    if args.type == 'dir':
        check_virsh_pool_not_exist(args.poolname)

        deletePool('dir', {'poolname': args.poolname})
    elif args.type == 'uus':
        check_cstor_pool_not_exist(args.poolname)
        deletePool(args.type, {'poolname': args.poolname})
    elif args.type == 'nfs' or args.type == 'glusterfs':
        # check virsh pool, only for nfs and glusterfs
        check_virsh_pool_not_exist(args.poolname)
        # check cstor pool
        check_cstor_pool_not_exist(args.poolname)
        deletePool(args.type, {'poolname': args.poolname})

def createDiskParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type ' + args.type + ' not support'}}
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        exit(3)
    if args.name is None:
        print {'result': {'code': 3, 'msg': 'less arg, name must be set'}}
        exit(3)

    if args.type == 'dir' or args.type == 'nfs' or args.type == 'glusterfs':
        if args.capacity is None:
            print {'result': {'code': 4, 'msg': 'less arg, capacity must be set'}}
            exit(4)
        if args.format is None:
            print {'result': {'code': 4, 'msg': 'less arg, format must be set'}}
            exit(4)
        check_virsh_pool_not_exist(args.poolname)
        check_virsh_disk_exist(args.poolname, args.name)

        createDisk('dir', {'poolname': args.poolname, 'name': args.name, 'capacity': args.capacity, 'format': args.format})
    elif args.type == 'uus':
        if args.capacity is None:
            print {'result': {'code': 4, 'msg': 'less arg, capacity must be set'}}
            exit(4)
        # check cstor disk
        check_cstor_pool_not_exist(args.poolname)
        check_cstor_disk_exist(args.poolname, args.name)
        createDisk(args.type, {'poolname': args.poolname, 'name': args.name, 'capacity': args.capacity})

def deleteDiskParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type ' + args.type + ' not support'}}
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        exit(3)
    if args.name is None:
        print {'result': {'code': 3, 'msg': 'less arg, name must be set'}}
        exit(3)

    if args.type == 'dir' or args.type == 'nfs' or args.type == 'glusterfs':
        check_virsh_disk_not_exist(args.poolname, args.name)

        deleteDisk('dir', {'poolname': args.poolname, 'name': args.name})
    elif args.type == 'uus':
        # check cstor disk
        check_cstor_disk_not_exist(args.poolname, args.name)
        deleteDisk(args.type, {'poolname': args.poolname, 'name': args.name})

def resizeDiskParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type ' + args.type + ' not support'}}
        print
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        print
        exit(3)
    if args.name is None:
        print {'result': {'code': 3, 'msg': 'less arg, name must be set'}}
        exit(3)
    if args.capacity is None:
        print {'result': {'code': 3, 'msg': 'less arg, capacity must be set'}}
        exit(3)

    if args.type == 'dir' or args.type == 'nfs' or args.type == 'glusterfs':
        check_virsh_disk_not_exist(args.poolname, args.name)
        check_virsh_disk_size(args.poolname, args.name, args.capacity)

        resizeDisk('dir', {'poolname': args.poolname, 'name': args.name, 'capacity': args.capacity})
    elif args.type == 'uus':
        # check cstor disk
        check_cstor_disk_not_exist(args.poolname, args.name)
        if args.vmname is None:
            resizeDisk(args.type, {'poolname': args.poolname, 'name': args.name, 'size': args.capacity})
        else:
            resizeDisk(args.type, {'poolname': args.poolname, 'name': args.name, 'size': args.capacity, 'vmname': args.vmname})

def cloneDiskParser(args):
    if args.type is None:
        print {'result': {'code': 1, 'msg': 'less arg type must be set'}}
        exit(1)
    if args.type not in ['dir', 'uus', 'nfs', 'glusterfs']:
        print {'result': {'code': 2, 'msg': 'not support value type ' + args.type + ' not support'}}
        exit(2)
    if args.poolname is None:
        print {'result': {'code': 3, 'msg': 'less arg, poolname must be set'}}
        exit(3)
    if args.name is None:
        print {'result': {'code': 3, 'msg': 'less arg, name must be set'}}
        exit(3)
    if args.newname is None:
        print {'result': {'code': 3, 'msg': 'less arg, newname must be set'}}
        exit(3)

    if args.type == 'dir' or args.type == 'nfs' or args.type == 'glusterfs':
        check_virsh_disk_not_exist(args.poolname, args.name)
        check_virsh_disk_exist(args.poolname, args.newname)

        cloneDisk('dir', {'poolname': args.poolname, 'name': args.name, 'newname': args.newname})
    elif args.type == 'uus':
        # check cstor disk
        check_cstor_disk_not_exist(args.poolname, args.name)
        check_cstor_disk_exist(args.poolname, args.newname)
        if args.newname is None:
            cloneDisk(args.type, {'poolname': args.poolname, 'name': args.name, 'clonename': args.newname})
        else:
            cloneDisk(args.type, {'poolname': args.poolname, 'name': args.name, 'clonename': args.newname, 'newname': args.newname})


# --------------------------- cmd line parser ---------------------------------------
parser = argparse.ArgumentParser(prog='kubeovs-adm', description='All storage adaptation tools')

subparsers = parser.add_subparsers(help='sub-command help')

# -------------------- add createPool cmd ----------------------------------
parser_create_pool = subparsers.add_parser('createPool', help='createPool help')
parser_create_pool.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='storage pool type to use')

parser_create_pool.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool name to delete')

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument('--url', metavar='[URL]', type=str,
                                help='storage pool create location, only for uus')

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument('--tag', metavar='[TAG]', type=str,
                                help='storage pool tag, only for uus')

# dir, nfs and glusterfs only, target will transfer to path in nfs and glusterfs
parser_create_pool.add_argument('--target', metavar='[TARGET]', type=str,
                                help='storage pool create location, only for dir, nfs and glusterfs')

# nfs only
parser_create_pool.add_argument('--opt', metavar='[OPT]', type=str,
                                help='nfs mount options, only for nfs')

# set default func
parser_create_pool.set_defaults(func=createPoolParser)

# -------------------- add deletePool cmd ----------------------------------
parser_delete_pool = subparsers.add_parser('deletePool', help='deletePool help')
parser_delete_pool.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='storage pool type to use')

parser_delete_pool.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool name to delete')
# set default func
parser_delete_pool.set_defaults(func=deletePoolParser)

# -------------------- add createDisk cmd ----------------------------------
parser_create_disk = subparsers.add_parser('createDisk', help='createDisk help')
parser_create_disk.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='disk type to use')
parser_create_disk.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool to use')

parser_create_disk.add_argument('--name', metavar='[NAME]', type=str,
                                help='volume name to use')

# will transfer to --size when type in uus, nfs or glusterfs
parser_create_disk.add_argument('--capacity', metavar='[CAPACITY]', type=str,
                                help='capacity is the size of the volume to be created, as a scaled integer (see NOTES above), defaulting to bytes')
parser_create_disk.add_argument('--format', metavar='[raw|bochs|qcow|qcow2|vmdk|qed]', type=str,
                                help='format is used in file based storage pools to specify the volume file format to use; raw, bochs, qcow, qcow2, vmdk, qed.')

# set default func
parser_create_disk.set_defaults(func=createDiskParser)

# -------------------- add deleteDisk cmd ----------------------------------
parser_delete_disk = subparsers.add_parser('deleteDisk', help='deleteDisk help')
parser_delete_disk.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='storage pool type to use')
parser_delete_disk.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool to use')
parser_delete_disk.add_argument('--name', metavar='[NAME]', type=str,
                                help='volume name to use')
# set default func
parser_delete_disk.set_defaults(func=deleteDiskParser)


# -------------------- add resizeDisk cmd ----------------------------------
parser_resize_disk = subparsers.add_parser('resizeDisk', help='resizeDisk help')
parser_resize_disk.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='storage pool type to use')
parser_resize_disk.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool to use')
parser_resize_disk.add_argument('--name', metavar='[NAME]', type=str,
                                help='volume name to use')
parser_resize_disk.add_argument('--capacity', metavar='[CAPACITY]', type=str,
                                help='new volume capacity to use')
parser_resize_disk.add_argument('--vmname', metavar='[VMNAME]', type=str,
                                help='new volume capacity to use')
# set default func
parser_resize_disk.set_defaults(func=resizeDiskParser)


# -------------------- add cloneDisk cmd ----------------------------------
parser_clone_disk = subparsers.add_parser('cloneDisk', help='cloneDisk help')
parser_clone_disk.add_argument('--type', metavar='[dir|uus|nfs|glusterfs]', type=str,
                                help='storage pool type to use')
parser_clone_disk.add_argument('--poolname', metavar='[POOLNAME]', type=str,
                                help='storage pool to use')
parser_clone_disk.add_argument('--name', metavar='[NAME]', type=str,
                                help='volume name to use')
parser_clone_disk.add_argument('--newname', metavar='[NEWNAME]', type=str,
                                help='new volume name to use')
# set default func
parser_clone_disk.set_defaults(func=cloneDiskParser)


# test_args = []
#
# dir1 = parser.parse_args(['createPool', '--type', 'dir', '--poolname', 'pooldir', '--target', '/var/lib/libvirt/pooldir'])
# dir2 = parser.parse_args(['createDisk', '--type', 'dir', '--poolname', 'pooldir', '--name', 'diskdir', '--capacity', '1073741824', '--format', 'qcow2'])
# dir3 = parser.parse_args(['resizeDisk', '--type', 'dir', '--poolname', 'pooldir', '--name', 'diskdir', '--capacity', '2147483648'])
# dir4 = parser.parse_args(['cloneDisk', '--type', 'dir', '--poolname', 'pooldir', '--name', 'diskdir', '--newname', 'diskdirclone'])
# dir5 = parser.parse_args(['deleteDisk', '--type', 'dir', '--poolname', 'pooldir', '--name', 'diskdirclone'])
# dir6 = parser.parse_args(['deleteDisk', '--type', 'dir', '--poolname', 'pooldir', '--name', 'diskdir'])
# dir7 = parser.parse_args(['deletePool', '--type', 'dir', '--poolname', 'pooldir'])
# #
# uus1 = parser.parse_args(['createPool', '--type', 'uus', '--poolname', 'pooldev', '--url', 'uus-iscsi-independent://admin:admin@192.168.3.10:7000/p1/4/2/0/32/0/3'])
# uus2 = parser.parse_args(['createDisk', '--type', 'uus', '--poolname', 'pooldev', '--name', 'diskdev', '--capacity', '1073741824'])
# uus3 = parser.parse_args(['resizeDisk', '--type', 'uus', '--poolname', 'pooldev', '--name', 'diskdev', '--capacity', '2147483648'])
# uus4 = parser.parse_args(['cloneDisk', '--type', 'uus', '--poolname', 'pooldev', '--name', 'diskdev', '--newname', 'diskdevclone'])
# uus5 = parser.parse_args(['deleteDisk', '--type', 'uus', '--poolname', 'pooldev', '--name', 'diskdev'])
# uus6 = parser.parse_args(['deletePool', '--type', 'uus', '--poolname', 'pooldev'])
#
# nfs1 = parser.parse_args(['createPool', '--type', 'nfs', '--poolname', 'poolnfs', '--url', 'nfs://192.168.3.99:/nfs/nfs', '--target', 'poolnfs', '--opt', 'nolock'])
# nfs2 = parser.parse_args(['createDisk', '--type', 'nfs', '--poolname', 'poolnfs', '--name', 'disknfs', '--capacity', '1073741824', '--format', 'qcow2'])
# nfs3 = parser.parse_args(['resizeDisk', '--type', 'nfs', '--poolname', 'poolnfs', '--name', 'disknfs', '--capacity', '2147483648'])
# nfs4 = parser.parse_args(['cloneDisk', '--type', 'nfs', '--poolname', 'poolnfs', '--name', 'disknfs', '--newname', 'disknfsclone'])
# nfs5 = parser.parse_args(['deleteDisk', '--type', 'nfs', '--poolname', 'poolnfs', '--name', 'disknfsclone'])
# nfs6 = parser.parse_args(['deleteDisk', '--type', 'nfs', '--poolname', 'poolnfs', '--name', 'disknfs'])
# nfs7 = parser.parse_args(['deletePool', '--type', 'nfs', '--poolname', 'poolnfs'])
#
# gfs1 = parser.parse_args(['createPool', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--url', 'glusterfs://192.168.3.93:nfsvol', '--target', 'poolglusterfs'])
# gfs2 = parser.parse_args(['createDisk', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--name', 'diskglusterfs', '--capacity', '1073741824', '--format', 'qcow2'])
# gfs3 = parser.parse_args(['resizeDisk', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--name', 'diskglusterfs', '--capacity', '2147483648'])
# gfs4 = parser.parse_args(['cloneDisk', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--name', 'diskglusterfs', '--newname', 'diskglusterfsclone'])
# gfs5 = parser.parse_args(['deleteDisk', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--name', 'diskglusterfsclone'])
# gfs6 = parser.parse_args(['deleteDisk', '--type', 'glusterfs', '--poolname', 'poolglusterfs', '--name', 'diskglusterfs'])
# gfs7 = parser.parse_args(['deletePool', '--type', 'glusterfs', '--poolname', 'poolglusterfs'])
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
# test_args.append(uus3)
# test_args.append(uus4)
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
    # print 'argument number not enough'
    logger.debug(traceback.format_exc())
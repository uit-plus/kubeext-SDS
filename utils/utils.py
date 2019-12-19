'''
Run back-end command in subprocess.
'''
import atexit
import fcntl
import os
import re
import random
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid
from functools import wraps
from json import loads, dumps, load, dump
from sys import exit

import grpc
import xmltodict

try:
    import xml.etree.CElementTree as ET
except:
    import xml.etree.ElementTree as ET

import cmdcall_pb2
import cmdcall_pb2_grpc
import logger
from exception import ExecuteException
from netutils import get_docker0_IP

LOG = '/var/log/kubesds.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)

DEFAULT_PORT = '19999'


def runCmdWithResult(cmd):
    if not cmd:
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
            msg = ''
            for index, line in enumerate(std_out):
                if not str.strip(line):
                    continue
                msg = msg + str.strip(line)
            msg = str.strip(msg)
            logger.debug(msg)
            try:
                result = loads(msg)
                if isinstance(result, dict) and 'result' in result.keys():
                    if result['result']['code'] != 0:
                        if std_err:
                            error_msg = ''
                            for index, line in enumerate(std_err):
                                if not str.strip(line):
                                    continue
                                error_msg = error_msg + str.strip(line)
                            error_msg = str.strip(error_msg).replace('"', "'")
                            result['result']['msg'] = '%s. cstor error output: %s' % (
                                result['result']['msg'], error_msg)
                return result
            except Exception:
                logger.debug(cmd)
                logger.debug(traceback.format_exc())
                error_msg = ''
                for index, line in enumerate(std_err):
                    if not str.strip(line):
                        continue
                    error_msg = error_msg + str.strip(line)
                error_msg = str.strip(error_msg)
                raise ExecuteException('RunCmdError',
                                       'can not parse cstor-cli output to json----%s. %s' % (msg, error_msg))
        if std_err:
            msg = ''
            for index, line in enumerate(std_err):
                msg = msg + line + ', '
            logger.debug(cmd)
            logger.debug(msg)
            logger.debug(traceback.format_exc())
            if msg.strip() != '':
                raise ExecuteException('RunCmdError', msg)
    finally:
        p.stdout.close()
        p.stderr.close()


def runCmdAndTransferXmlToJson(cmd):
    xml_str = runCmdAndGetOutput(cmd)
    dic = xmltodict.parse(xml_str, encoding='utf-8')
    dic = dumps(dic)
    dic = dic.replace('@', '').replace('#', '')
    return loads(dic)


def runCmdAndSplitKvToJson(cmd):
    if not cmd:
        #         logger.debug('No CMD to execute.')
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
            result = {}
            for index, line in enumerate(std_out):
                if not str.strip(line):
                    continue
                line = str.strip(line)
                kv = line.replace(':', '').split()
                if len(kv) == 2:
                    result[kv[0].lower()] = kv[1]
            return result
        if std_err:
            error_msg = ''
            for index, line in enumerate(std_err):
                if not str.strip(line):
                    continue
                else:
                    error_msg = error_msg + str.strip(line)
            error_msg = str.strip(error_msg)
            logger.debug(error_msg)
            if error_msg.strip() != '':
                raise ExecuteException('RunCmdError', error_msg)
    finally:
        p.stdout.close()
        p.stderr.close()


def runCmdAndGetOutput(cmd):
    if not cmd:
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
            msg = ''
            for line in std_out:
                msg = msg + line
            return msg
        if std_err:
            msg = ''
            for index, line in enumerate(std_err):
                if not str.strip(line):
                    continue
                if index == len(std_err) - 1:
                    msg = msg + str.strip(line) + '. ' + '***More details in %s***' % LOG
                else:
                    msg = msg + str.strip(line) + ', '
            logger.debug(cmd)
            logger.debug(msg)
            logger.debug(traceback.format_exc())
            if msg.strip() != '':
                raise ExecuteException('RunCmdError', msg)
    except Exception:
        logger.debug(traceback.format_exc())
    finally:
        p.stdout.close()
        p.stderr.close()


'''
Run back-end command in subprocess.
'''


def runCmd(cmd):
    std_err = None
    if not cmd:
        #         logger.debug('No CMD to execute.')
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
            #             msg = ''
            #             for index,line in enumerate(std_out):
            #                 if not str.strip(line):
            #                     continue
            #                 if index == len(std_out) - 1:
            #                     msg = msg + str.strip(line) + '. '
            #                 else:
            #                     msg = msg + str.strip(line) + ', '
            #             logger.debug(str.strip(msg))
            logger.debug(std_out)
        if std_err:
            msg = ''
            for index, line in enumerate(std_err):
                msg = msg + line
            if msg.strip() != '':
                raise ExecuteException('RunCmdError', msg)
        return
    finally:
        p.stdout.close()
        p.stderr.close()


def runCmdRaiseException(cmd, head='VirtctlError', use_read=False):
    logger.debug(cmd)
    std_err = None
    if not cmd:
        return
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        if use_read:
            std_out = p.stdout.read()
            std_err = p.stderr.read()
        else:
            std_out = p.stdout.readlines()
            std_err = p.stderr.readlines()
        if std_err:
            logger.error(std_err)
            raise ExecuteException(head, std_err)
        return std_out
    finally:
        p.stdout.close()
        p.stderr.close()


def rpcCall(cmd):
    logger.debug(cmd)
    try:
        host = get_docker0_IP()
        channel = grpc.insecure_channel("{0}:{1}".format(host, DEFAULT_PORT))
        client = cmdcall_pb2_grpc.CmdCallStub(channel)
        response = client.Call(cmdcall_pb2.CallRequest(cmd=cmd))
        logger.debug(response.json)
        jsondict = loads(str(response.json))
    except grpc.RpcError, e:
        logger.debug(traceback.format_exc())
        # ouch!
        # lets print the gRPC error message
        # which is "Length of `Name` cannot be more than 10 characters"
        logger.debug(e.details())
        # lets access the error code, which is `INVALID_ARGUMENT`
        # `type` of `status_code` is `grpc.StatusCode`
        status_code = e.code()
        # should print `INVALID_ARGUMENT`
        logger.debug(status_code.name)
        # should print `(3, 'invalid argument')`
        logger.debug(status_code.value)
        # want to do some specific action based on the error?
        if grpc.StatusCode.INVALID_ARGUMENT == status_code:
            # do your stuff here
            pass
        raise ExecuteException('RunCmdError', "Cmd: %s failed!" % cmd)
    except Exception:
        logger.debug(traceback.format_exc())
        raise ExecuteException('RunCmdError', "Cmd: %s failed!" % cmd)

    if jsondict['result']['code'] != 0:
        raise ExecuteException('RunCmdError', jsondict['result']['msg'])


def rpcCallWithResult(cmd):
    logger.debug(cmd)
    try:
        host = get_docker0_IP()
        channel = grpc.insecure_channel("{0}:{1}".format(host, DEFAULT_PORT))
        client = cmdcall_pb2_grpc.CmdCallStub(channel)
        # ideally, you should have try catch block here too
        response = client.CallWithResult(cmdcall_pb2.CallRequest(cmd=cmd))
        client = cmdcall_pb2_grpc.CmdCallStub(channel)
        result = loads(str(response.json))
        return result
    except grpc.RpcError, e:
        logger.debug(traceback.format_exc())
        # ouch!
        # lets print the gRPC error message
        # which is "Length of `Name` cannot be more than 10 characters"
        logger.debug(e.details())
        # lets access the error code, which is `INVALID_ARGUMENT`
        # `type` of `status_code` is `grpc.StatusCode`
        status_code = e.code()
        # should print `INVALID_ARGUMENT`
        logger.debug(status_code.name)
        # should print `(3, 'invalid argument')`
        logger.debug(status_code.value)
        # want to do some specific action based on the error?
        if grpc.StatusCode.INVALID_ARGUMENT == status_code:
            # do your stuff here
            pass
        raise ExecuteException('RunCmdError', "Cmd: %s failed!" % cmd)
    except Exception:
        logger.debug(traceback.format_exc())
        raise ExecuteException('RunCmdError', 'can not parse rpc response to json.')


def rpcCallAndTransferXmlToJson(cmd):
    logger.debug(cmd)
    try:
        host = get_docker0_IP()
        channel = grpc.insecure_channel("{0}:{1}".format(host, DEFAULT_PORT))
        client = cmdcall_pb2_grpc.CmdCallStub(channel)
        # ideally, you should have try catch block here too
        response = client.CallAndTransferXmlToJson(cmdcall_pb2.CallRequest(cmd=cmd))
        result = loads(str(response.json))
        return result
    except grpc.RpcError, e:
        logger.debug(traceback.format_exc())
        # ouch!
        # lets print the gRPC error message
        # which is "Length of `Name` cannot be more than 10 characters"
        logger.debug(e.details())
        # lets access the error code, which is `INVALID_ARGUMENT`
        # `type` of `status_code` is `grpc.StatusCode`
        status_code = e.code()
        # should print `INVALID_ARGUMENT`
        logger.debug(status_code.name)
        # should print `(3, 'invalid argument')`
        logger.debug(status_code.value)
        # want to do some specific action based on the error?
        if grpc.StatusCode.INVALID_ARGUMENT == status_code:
            # do your stuff here
            pass
        raise ExecuteException('RunCmdError', "Cmd: %s failed!" % cmd)
    except Exception:
        logger.debug(traceback.format_exc())
        raise ExecuteException('RunCmdError', 'can not parse rpc response to json.')


def rpcCallAndTransferKvToJson(cmd):
    logger.debug(cmd)
    try:
        host = get_docker0_IP()
        channel = grpc.insecure_channel("{0}:{1}".format(host, DEFAULT_PORT))
        client = cmdcall_pb2_grpc.CmdCallStub(channel)
        # ideally, you should have try catch block here too
        response = client.CallAndSplitKVToJson(cmdcall_pb2.CallRequest(cmd=cmd))
        result = loads(str(response.json))
        return result
    except grpc.RpcError, e:
        logger.debug(traceback.format_exc())
        # ouch!
        # lets print the gRPC error message
        # which is "Length of `Name` cannot be more than 10 characters"
        logger.debug(e.details())
        # lets access the error code, which is `INVALID_ARGUMENT`
        # `type` of `status_code` is `grpc.StatusCode`
        status_code = e.code()
        # should print `INVALID_ARGUMENT`
        logger.debug(status_code.name)
        # should print `(3, 'invalid argument')`
        logger.debug(status_code.value)
        # want to do some specific action based on the error?
        if grpc.StatusCode.INVALID_ARGUMENT == status_code:
            # do your stuff here
            pass
        raise ExecuteException('RunCmdError', "Cmd: %s failed!" % cmd)
    except Exception:
        logger.debug(traceback.format_exc())
        raise ExecuteException('RunCmdError', 'can not parse rpc response to json.')


def randomUUID():
    u = [random.randint(0, 255) for ignore in range(0, 16)]
    u[6] = (u[6] & 0x0F) | (4 << 4)
    u[8] = (u[8] & 0x3F) | (2 << 6)
    return "-".join(["%02x" * 4, "%02x" * 2, "%02x" * 2, "%02x" * 2,
                     "%02x" * 6]) % tuple(u)


def randomUUIDFromName(name):
    name = str(name)
    namespace = uuid.NAMESPACE_URL

    return str(uuid.uuid5(namespace, name))


def is_pool_started(pool):
    poolInfo = runCmdAndSplitKvToJson('virsh pool-info %s' % pool)
    if poolInfo['state'] == 'running':
        return True
    return False


def is_pool_exists(pool):
    poolInfo = runCmdAndSplitKvToJson('virsh pool-info %s' % pool)
    if poolInfo and pool == poolInfo['name']:
        return True
    return False


def is_pool_defined(pool):
    poolInfo = runCmdAndSplitKvToJson('virsh pool-info %s' % pool)
    if poolInfo['persistent'] == 'yes':
        return True
    return False


def is_vm_active(domain):
    output = runCmdAndGetOutput('virsh list')
    lines = output.splitlines()
    for line in lines:
        if domain in line.split():
            return True
    return False


def get_volume_size(pool, vol):
    disk_config = get_disk_config(pool, vol)
    disk_info = get_disk_info(disk_config['current'])
    return int(disk_info['virtual_size'])


def get_disks_spec(domain):
    output = runCmdAndGetOutput('virsh domblklist %s' % domain)
    lines = output.splitlines()
    spec = {}
    for i in range(2, len(lines)):
        kv = lines[i].split()
        if len(kv) == 2 and kv[0].find('hd') < 0:
            spec[kv[1]] = kv[0]
    return spec


class CDaemon:
    '''
    a generic daemon class.
    usage: subclass the CDaemon class and override the run() method
    stderr:
    verbose:
    save_path:
    '''

    def __init__(self, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022,
                 verbose=1):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = save_path
        self.home_dir = home_dir
        self.verbose = verbose
        self.umask = umask
        self.daemon_alive = True

    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

        os.chdir(self.home_dir)
        os.setsid()
        os.umask(self.umask)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

        sys.stdout.flush()
        sys.stderr.flush()

        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        if self.stderr:
            se = file(self.stderr, 'a+', 0)
        else:
            se = so

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        def sig_handler(signum, frame):
            self.daemon_alive = False

        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)

        if self.verbose >= 1:
            print 'daemon process started ...'

        atexit.register(self.del_pid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write('%s\n' % pid)

    def get_pid(self):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        except SystemExit:
            pid = None
        return pid

    def del_pid(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def start(self, *args, **kwargs):
        if self.verbose >= 1:
            print 'ready to starting ......'
        # check for a pid file to see if the daemon already runs
        pid = self.get_pid()
        if pid:
            msg = 'pid file %s already exists, is it already running?\n'
            sys.stderr.write(msg % self.pidfile)
            sys.exit(1)
        # start the daemon
        self.daemonize()
        self.run(*args, **kwargs)

    def stop(self):
        if self.verbose >= 1:
            print 'stopping ...'
        pid = self.get_pid()
        if not pid:
            msg = 'pid file [%s] does not exist. Not running?\n' % self.pidfile
            sys.stderr.write(msg)
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return
        # try to kill the daemon process
        try:
            i = 0
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                i = i + 1
                if i % 10 == 0:
                    os.kill(pid, signal.SIGHUP)
        except OSError, err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)
            if self.verbose >= 1:
                print 'Stopped!'

    def restart(self, *args, **kwargs):
        self.stop()
        self.start(*args, **kwargs)

    def is_running(self):
        pid = self.get_pid()
        # print(pid)
        return pid and os.path.exists('/proc/%d' % pid)

    def run(self, *args, **kwargs):
        'NOTE: override the method in subclass'
        print 'base class run()'


def singleton(pid_filename):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            pid = str(os.getpid())
            pidfile = open(pid_filename, 'a+')
            try:
                fcntl.flock(pidfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                return
            pidfile.seek(0)
            pidfile.truncate()
            pidfile.write(pid)
            pidfile.flush()
            pidfile.seek(0)

            ret = f(*args, **kwargs)

            try:
                pidfile.close()
            except IOError, err:
                if err.errno != 9:
                    return
            os.remove(pid_filename)
            return ret

        return decorated

    return decorator


def get_IP():
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    return myaddr


def get_cstor_pool_info(pool):
    cstor = runCmdWithResult("cstor-cli pool-show --poolname %s" % pool)
    if cstor['result']['code'] != 0:
        error_print(400, 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
    return cstor


def get_pool_info(pool_):
    if not pool_:
        raise ExecuteException('', 'missing parameter: no pool name.')
    result = runCmdAndSplitKvToJson('virsh pool-info %s' % pool_)
    # result['allocation'] = int(1024*1024*1024*float(result['allocation']))
    # result['available'] = int(1024 * 1024 * 1024 * float(result['available']))
    # result['code'] = 0
    # result['capacity'] = int(1024 * 1024 * 1024 * float(result['capacity']))
    if 'allocation' in result.keys():
        del result['allocation']
    if 'available' in result.keys():
        del result['available']

    xml_dict = runCmdAndTransferXmlToJson('virsh pool-dumpxml %s' % pool_)
    result['capacity'] = int(xml_dict['pool']['capacity']['text'])
    result['path'] = xml_dict['pool']['target']['path']
    return result


def get_pool_info_from_k8s(pool):
    if not pool:
        raise ExecuteException('', 'missing parameter: no pool name.')
    result = runCmdWithResult('kubectl get vmp -o json %s' % pool)
    if 'spec' in result.keys() and isinstance(result['spec'], dict) and 'pool' in result['spec'].keys():
        return result['spec']['pool']
    raise ExecuteException('', 'can not get pool info from k8s')


def get_vol_info_from_k8s(vol):
    if not vol:
        raise ExecuteException('', 'missing parameter: no disk name.')
    result = runCmdWithResult('kubectl get vmd -o json %s' % vol)
    if 'spec' in result.keys() and isinstance(result['spec'], dict) and 'volume' in result['spec'].keys():
        return result['spec']['volume']
    raise ExecuteException('', 'can not get vol info from k8s')


def get_snapshot_info_from_k8s(snapshot):
    result = runCmdWithResult('kubectl get vmdsn -o json %s' % snapshot)
    if 'spec' in result.keys() and isinstance(result['spec'], dict) and 'volume' in result['spec'].keys():
        return result['spec']['volume']
    raise ExecuteException('', 'can not get snapshot info from k8s')


def get_disk_config(pool, vol):
    if not pool or not vol:
        raise ExecuteException('', 'missing parameter: no pool or disk name.')
    poolInfo = get_pool_info(pool)
    pool_path = poolInfo['path']
    if not os.path.isdir(pool_path):
        raise ExecuteException('', "can not get pool %s path." % pool)
    config_path = '%s/%s/config.json' % (pool_path, vol)
    with open(config_path, "r") as f:
        config = load(f)
        return config


def get_disk_config_by_path(config_path):
    if not config_path:
        raise ExecuteException('', 'cannot find "config.json" in disk dir.')
    with open(config_path, "r") as f:
        config = load(f)
        return config


def get_disk_snapshots(ss_path):
    ss_chain = get_sn_chain(ss_path)
    snapshots = []
    for disk_info in ss_chain:
        if disk_info['filename'] != ss_path:
            snapshots.append(disk_info['filename'])
    return snapshots


def get_disk_info(ss_path):
    try:
        result = runCmdWithResult('qemu-img info -U --output json %s' % ss_path)
    except:
        try:
            result = runCmdWithResult('qemu-img info --output json %s' % ss_path)
        except:
            error_print(400, "can't get snapshot info in qemu-img.")
            exit(1)
    json_str = dumps(result)
    return loads(json_str.replace('-', '_'))


def get_sn_chain(ss_path):
    try:
        result = runCmdWithResult('qemu-img info -U --backing-chain --output json %s' % ss_path)
    except:
        try:
            result = runCmdWithResult('qemu-img info --backing-chain --output json %s' % ss_path)
        except:
            error_print(400, "can't get snapshot info in qemu-img.")
            exit(1)
    return result


def get_sn_chain_path(ss_path):
    paths = set()
    chain = get_sn_chain(ss_path)
    for info in chain:
        if 'backing-filename' in info.keys():
            paths.add(info['backing-filename'])
    return list(paths)


def get_all_snapshot_to_delete(ss_path, current):
    delete_sn = []
    chain = get_sn_chain(current)
    for info in chain:
        if 'backing-filename' in info.keys() and info['backing-filename'] == ss_path:
            delete_sn.append(info['filename'])
            delete_sn.extend(get_all_snapshot_to_delete(info['filename'], current))
            break
    return delete_sn


class DiskImageHelper(object):
    @staticmethod
    def get_backing_file(file, raise_it=False):
        """ Gets backing file for disk image """
        get_backing_file_cmd = "qemu-img info %s" % file
        try:
            out = runCmdRaiseException(get_backing_file_cmd, use_read=True)
        except Exception, e:
            if raise_it:
                raise e
            get_backing_file_cmd = "qemu-img info -U %s" % file
            out = runCmdRaiseException(get_backing_file_cmd, use_read=True)
        lines = out.decode('utf-8').split('\n')
        for line in lines:
            if re.search("backing file:", line):
                return str(line.strip().split()[2])
        return None

    @staticmethod
    def get_backing_files_tree(file):
        """ Gets all backing files (snapshot tree) for disk image """
        backing_files = []
        backing_file = DiskImageHelper.get_backing_file(file)
        while backing_file is not None:
            backing_files.append(backing_file)
            backing_file = DiskImageHelper.get_backing_file(backing_file)
        return backing_files

    @staticmethod
    def set_backing_file(backing_file, file):
        """ Sets backing file for disk image """
        set_backing_file_cmd = "qemu-img rebase -u -b %s %s" % (backing_file, file)
        runCmdRaiseException(set_backing_file_cmd)


def check_disk_in_use(disk_path):
    try:
        result = runCmdWithResult('qemu-img info --output json %s' % disk_path)
    except:
        return True
    return False


def change_vm_os_disk_file(vm, source, target):
    if not vm or not source or not target:
        raise ExecuteException('', 'missing parameter: no vm name(%s) or source path(%s) or target path(%s).' % (
        vm, source, target))
    runCmd('virsh dumpxml %s > /tmp/%s.xml' % (vm, vm))
    tree = ET.parse('/tmp/%s.xml' % vm)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                if source_element.get("file") == source:
                    source_element.set("file", target)
                    tree.write('/tmp/%s.xml' % vm)
                    runCmd('virsh define /tmp/%s.xml' % vm)
                    return True
    return False


def is_shared_storage(path):
    if not path:
        raise ExecuteException('', 'missing parameter: no path.')
    cmd = 'df %s | awk \'{print $1}\' | sed -n "2, 1p"' % path
    fs = runCmdAndGetOutput(cmd)
    fs = fs.strip()
    if re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}:.*$', fs):
        return True
    return False


def is_vm_disk_not_shared_storage(vm):
    if not vm:
        raise ExecuteException('', 'missing parameter: no vm name.')
    runCmd('virsh dumpxml %s > /tmp/%s.xml' % (vm, vm))
    tree = ET.parse('/tmp/%s.xml' % vm)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                disk_file = source_element.get("file")
                if not is_shared_storage(disk_file):
                    return False

    return True


def is_vm_disk_driver_cache_none(vm):
    if not vm:
        raise ExecuteException('', 'missing parameter: no vm name.')
    runCmd('virsh dumpxml %s > /tmp/%s.xml' % (vm, vm))
    tree = ET.parse('/tmp/%s.xml' % vm)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("driver")
                if "cache" in source_element.keys() and source_element.get("cache") == "none":
                    continue
                else:
                    return False
    return True


def success_print(msg, data):
    print dumps({"result": {"code": 0, "msg": msg}, "data": data})
    exit(0)


def error_print(code, msg, data=None):
    if data is None:
        print dumps({"result": {"code": code, "msg": msg}, "data": {}})
        exit(1)
    else:
        print dumps({"result": {"code": code, "msg": msg}, "data": data})
        exit(1)


# if __name__ == '__main__':
    # try:
    #     result = runCmdWithResult('cstor-cli pooladd-nfs --poolname abc --url /mnt/localfs/pooldir11')
    #     print result
    # except ExecuteException, e:
    #     print e.message
    # print get_snapshot_info_from_k8s('disktestd313.2')
    # print get_pool_info(' node22-poolnfs')
# print is_vm_disk_not_shared_storage('vm006')

# print change_vm_os_disk_file('vm010', '/uit/pooluittest/diskuittest/snapshots/diskuittest.2', '/uit/pooluittest/diskuittest/snapshots/diskuittest.1')
# print get_all_snapshot_to_delete('/var/lib/libvirt/pooltest/disktest/disktest', '/var/lib/libvirt/pooltest/disktest/ss3')

# print os.path.basename('/var/lib/libvirt/pooltest/disktest/disktest')

# print get_disk_snapshots('/var/lib/libvirt/pooltest/disktest/ss1')

# print get_pool_info('test1')

# print get_sn_chain_path('/var/lib/libvirt/pooltest/disktest/0e8e48d9-b6ab-4477-999d-0e57b521a51b')

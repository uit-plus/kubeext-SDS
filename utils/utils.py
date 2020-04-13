'''
Run back-end command in subprocess.
'''
import atexit
import fcntl
import hashlib
import os
import re
import random
import signal
import socket
import string
import subprocess
import sys
import time
import traceback
import uuid
from functools import wraps
from json import loads, dumps, load, dump
from sys import exit
from xml.etree.ElementTree import fromstring
from xmljson import badgerfish as bf
import grpc
import xmltodict
import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

from k8s import K8sHelper, addPowerStatusMessage, updateJsonRemoveLifecycle, get_hostname_in_lower_case, get_node_name
from arraylist import vmArray

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


def remoteRunCmdWithResult(ip, cmd):
    if not cmd:
        return
    cmd = 'ssh root@%s "%s"' % (ip, cmd)
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

def remoteRunCmd(ip, cmd):
    if not cmd:
        logger.debug('No CMD to execute.')
        return
    cmd = 'ssh root@%s "%s"' % (ip, cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        std_out = p.stdout.readlines()
        std_err = p.stderr.readlines()
        if std_out:
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

'''
Run back-end command in subprocess.
'''
def runCmd(cmd):
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

def is_vm_exist(domain):
    output = runCmdAndGetOutput('virsh list --all')
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
    if domain is None:
        raise ExecuteException('RunCmdError', 'domin is not set. Can not get domain disk spec.')
    output = runCmdAndGetOutput('virsh domblklist %s' % domain)
    lines = output.splitlines()
    spec = {}
    for i in range(2, len(lines)):
        kv = lines[i].split()
        if len(kv) == 2 and kv[0].find('hd') < 0:
            spec[kv[1]] = kv[0]
    return spec

def get_disks_spec_by_xml(xmlfile):
    if xmlfile is None:
        raise ExecuteException('RunCmdError', 'domin xml file is not set. Can not get domain disk spec.')

    tree = ET.parse(xmlfile)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    spec = {}
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                if source_element is not None:
                    target_element = disk.find("target")
                    spec[source_element.get("file")] = target_element.get('dev')
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
        error_print(400, 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))
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


def modify_disk_info_in_k8s(poolname, vol):
    helper = K8sHelper("VirtualMachineDisk")
    helper.update(vol, "volume", get_disk_info_to_k8s(poolname, vol))


def modify_snapshot_info_in_k8s(poolname, vol, name):
    helper = K8sHelper("VirtualMachineDiskSnapshot")
    helper.update(name, "volume", get_snapshot_info_to_k8s(poolname, vol, name))

def cstor_pool_active(poolname):
    cstor = runCmdWithResult('cstor-cli pool-active --poolname %s' % poolname)
    if cstor['result']['code'] != 0:
        logger.debug('can not active cstor pool %s' % poolname)
        raise ExecuteException('', 'can not active cstor pool %s' % poolname)

def get_pool_info_from_k8s(pool):
    if not pool:
        raise ExecuteException('', 'missing parameter: no pool name.')
    result = runCmdWithResult('kubectl get vmp -o json %s' % pool)
    if 'spec' in result.keys() and isinstance(result['spec'], dict) and 'pool' in result['spec'].keys():
        pool_info = result['spec']['pool']
        # # try make pool active
        # pool_helper = K8sHelper('VirtualMahcinePool')
        # this_node_name = get_hostname_in_lower_case()
        # pool_node_name = get_node_name(pool_helper.get(pool))
        # if this_node_name == pool_node_name and pool_info['state'] == 'active':
        #     cstor_pool_active(pool_info['poolname'])
        #     if pool_info['pooltype'] != 'uus':
        #         runCmd('virsh pool-start %s' % pool_info['poolname'])
        return pool_info
    raise ExecuteException('', 'can not get pool info from k8s')

def get_image_info_from_k8s(image):
    if not image:
        raise ExecuteException('', 'missing parameter: no image name.')
    result = runCmdWithResult('kubectl get vmdi -o json %s' % image)
    if 'spec' in result.keys() and isinstance(result['spec'], dict) and 'volume' in result['spec'].keys():
        return result['spec']['volume']
    raise ExecuteException('', 'can not get vol info from k8s')

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

def change_vol_current(vol, current):
    vol_info = get_vol_info_from_k8s(vol)
    pool_info = get_pool_info_from_k8s(vol_info['pool'])
    check_pool_active(pool_info)

    config_path = '%s/%s/config.json' % (pool_info['path'], vol)
    with open(config_path, 'r') as f:
        config = load(f)
    config['current'] = current
    with open(config_path, 'w') as f:
        dump(config, f)
    helper = K8sHelper("VirtualMachineDisk")
    helper.update(vol, 'volume', get_disk_info_to_k8s(pool_info['poolname'], vol))

def get_pool_info_to_k8s(type, pool, poolname, content):
    cstor = get_cstor_pool_info(poolname)
    result = get_pool_info(poolname)
    result['content'] = content
    result["pooltype"] = type
    result["pool"] = pool
    result["poolname"] = poolname
    if is_pool_started(poolname) and cstor['data']['status'] == 'active':
        result["state"] = "active"
    else:
        result["state"] = "inactive"
    return result

def write_config(vol, dir, current, pool, poolname):
    config = {}
    config['name'] = vol
    config['dir'] = dir
    config['current'] = current
    config['pool'] = pool
    config['poolname'] = poolname

    with open('%s/config.json' % dir, "w") as f:
        logger.debug(config)
        dump(config, f)

def get_disk_info_to_k8s(poolname, vol):
    config_path = '%s/%s/config.json' % (get_pool_info(poolname)['path'], vol)
    if not os.path.exists(config_path):
        return get_vol_info_from_k8s(vol)
    with open(config_path, "r") as f:
        config = load(f)
    result = get_disk_info(config['current'])
    result['disk'] = vol
    result["pool"] = config['pool']
    result["poolname"] = poolname
    result["uni"] = config['current']
    result["current"] = config['current']
    return result

def get_cstor_disk_info_to_k8s(pool, poolname, vol):
    disk_info_k8s = get_vol_info_from_k8s(vol)
    diskinfo = runCmdWithResult("cstor-cli vdisk-show --poolname %s --name %s" % (poolname, vol))
    if diskinfo['result']['code'] != 0:
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            diskinfo['result']['code'], diskinfo['result']['msg'], diskinfo['obj']))

    result = {
        "disk": vol,
        "pool": pool,
        "poolname": poolname,
        "uni": diskinfo["data"]["uni"],
        "current": disk_info_k8s["current"],
        "virtual_size": diskinfo["data"]["size"],
        "filename": disk_info_k8s["filename"]
    }
    return result


def get_snapshot_info_to_k8s(poolname, vol, name):
    config_path = '%s/%s/config.json' % (get_pool_info(poolname)['path'], vol)
    if not os.path.exists(config_path):
        return get_snapshot_info_from_k8s(name)
    with open(config_path, "r") as f:
        config = load(f)
    ss_path = '%s/snapshots/%s' % (config['dir'], name)
    result = get_disk_info(ss_path)
    result['disk'] = vol
    result["pool"] = config['pool']
    result["poolname"] = poolname
    result["uni"] = config['current']
    result['snapshot'] = name
    return result


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

def delete_vm_disk_in_xml(xmlfile, disk_file):
    tree = ET.parse(xmlfile)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                if source_element.get("file") == disk_file:
                    caption.remove(disk)
                    tree.write(xmlfile)
                    return True
    return False

def delete_vm_cdrom_file_in_xml(xmlfile):
    tree = ET.parse(xmlfile)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'cdrom' == disk.attrib['device']:
                source_element = disk.find("source")
                if source_element is not None:
                    disk.remove(source_element)
                    tree.write(xmlfile)
                    return True
    return False


def modofy_vm_disk_file(xmlfile, source, target):
    tree = ET.parse(xmlfile)

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
                    tree.write(xmlfile)
                    return True
    return False

def attach_vm_disk(vm, disk):
    disk_specs = get_disks_spec(vm)
    if not os.path.exists(disk):
        raise ExecuteException('', 'disk file %s not exist.' % disk)
    if disk in disk_specs.keys():
        raise ExecuteException('', 'disk file %s has attached in vm %s.' % (disk, vm))
    tag = None
    letter_list = list(string.ascii_lowercase)
    for i in letter_list:
        if ('vd' + i) not in disk_specs.values():
            tag = 'vd' + i
            break
    disk_info = get_disk_info(disk)
    runCmd('virsh attach-disk --domain %s --cache none  --config %s --target %s --subdriver %s' % (vm, disk, tag, disk_info['format']))

def modofy_vm_disks(vm, source_to_target):
    if not vm or not source_to_target:
        raise ExecuteException('', 'missing parameter: no vm name(%s) or source_to_target.' % vm)
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
                if source_element.get("file") in source_to_target.keys():
                    source_element.set("file", source_to_target[source_element.get("file")])
        tree.write('/tmp/%s.xml' % vm)
        runCmd('virsh define /tmp/%s.xml' % vm)
        runCmd('rm /tmp/%s.xml' % vm)
        return True
    return False

def define_and_restore_vm_disks(xmlfile, newname, source_to_target):
    if not xmlfile or not source_to_target:
        raise ExecuteException('', 'missing parameter: no vm xml file %s or source_to_target.' % xmlfile)
    uuid = randomUUID().replace('-', '')
    runCmd('cp %s /tmp/%s.xml' % (xmlfile, uuid))
    tree = ET.parse('/tmp/%s.xml' % uuid)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    nameList = root.findall("name")
    for name in nameList:
        name.text = newname
    uuidList = root.findall("uuid")
    for uuid in uuidList:
        uuid.text = newname
    captionList = root.findall("devices")
    for caption in captionList:
        disks = caption.findall("disk")
        disk_need_to_delete = []
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                if source_element.get("file") in source_to_target.keys():
                    source_element.set("file", source_to_target[source_element.get("file")])
                else:
                    disk_need_to_delete.append(disk)
        for disk in disk_need_to_delete:
            caption.remove(disk)
    tree.write('/tmp/%s.xml' % uuid)
    runCmd('virsh define /tmp/%s.xml' % uuid)
    runCmd('rm /tmp/%s.xml' % uuid)


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
    if fs:
        fs = fs.strip()
        if re.match('^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}:.*$', fs):
            return True
    return False

def get_vm_disks_from_xml(xmlfile):
    tree = ET.parse(xmlfile)

    root = tree.getroot()
    # for child in root:
    #     print(child.tag, "----", child.attrib)
    captionList = root.findall("devices")
    all_disks = []
    for caption in captionList:
        disks = caption.findall("disk")
        for disk in disks:
            if 'disk' == disk.attrib['device']:
                source_element = disk.find("source")
                disk_file = source_element.get("file")
                if not is_shared_storage(disk_file):
                    all_disks.append(disk_file)

    return all_disks

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

def poolActive(poolname):
    cstor = runCmdWithResult('cstor-cli pool-active --poolname %s' % poolname)
    if cstor['result']['code'] != 0 or cstor['data']['status'] != 'active':
        raise ExecuteException('', 'cstor raise exception: cstor error code: %d, msg: %s, obj: %s' % (
            cstor['result']['code'], cstor['result']['msg'], cstor['obj']))

    # make other vdiskfs pool inactive
    pool_path = '%s/%s' % (cstor['data']['mountpath'], poolname)
    pools = get_pools_by_path(pool_path)
    node_name = get_hostname_in_lower_case()
    poolHelper = K8sHelper('VirtualMahcinePool')
    for pool in pools:
        if pool['host'] != node_name:
            pool_info = get_pool_info_from_k8s(pool['pool'])
            pool_info['state'] = 'inactive'
            poolHelper.update(pool['pool'], 'pool', pool_info)
        else:
            pool_info = get_pool_info_from_k8s(pool['pool'])
            if pool_info['state'] != 'active':
                pool_info['state'] = 'active'
                poolHelper.update(pool['pool'], 'pool', pool_info)

    # change all disk and snapshot to this node
    all_disk = get_pool_all_disk(poolname)
    if all_disk:
        disk_helper = K8sHelper('VirtualMachineDisk')
        for disk in all_disk:
            if disk['host'] != node_name:
                disk_helper.change_node(disk['disk'], node_name)

        all_ss = get_pool_all_ss(poolname)
        if all_ss:
            ss_helper = K8sHelper('VirtualMachineDiskSnapshot')
            for ss in all_ss:
                if ss['host'] != node_name:
                    ss_helper.change_node(ss['ss'], node_name)

def get_pool_all_disk(poolname):
    output = runCmdAndGetOutput(
        'kubectl get vmd -o=jsonpath="{range .items[?(@.spec.volume.poolname==\\"%s\\")]}{.metadata.name}{\\"\\t\\"}{.metadata.labels.host}{\\"\\n\\"}{end}"' % poolname)
    disks = []
    if output:
        for line in output.splitlines():
            disk = {}
            if len(line.split()) < 2:
                continue
            disk['disk'] = line.split()[0]
            disk['host'] = line.split()[1]
            disks.append(disk)
    return disks

def get_pool_all_ss(poolname):
    output = runCmdAndGetOutput(
        'kubectl get vmdsn -o=jsonpath="{range .items[?(@.spec.volume.poolname==\\"%s\\")]}{.metadata.name}{\\"\\t\\"}{.metadata.labels.host}{\\"\\n\\"}{end}"' % poolname)
    disks = []
    if output:
        for line in output.splitlines():
            disk = {}
            if len(line.split()) < 2:
                continue
            disk['ss'] = line.split()[0]
            disk['host'] = line.split()[1]
            disks.append(disk)
    return disks

def get_pools_by_node(node_name):
    output = runCmdAndGetOutput(
        'kubectl get vmp -o=jsonpath="{range .items[?(.metadata.labels.host==\\"%s\\")]}{.metadata.name}{\\"\\t\\"}{.spec.pool.poolname}{\\"\\t\\"}{.metadata.labels.host}{\\"\\n\\"}{end}"' % node_name)
    pools = []
    for line in output.splitlines():
        pool = {}
        if len(line.split()) < 3:
            continue
        pool['pool'] = line.split()[0]
        pool['poolname'] = line.split()[1]
        pools.append(pool)
    return pools

def get_pools_by_path(path):
    output = runCmdAndGetOutput(
        'kubectl get vmp -o=jsonpath="{range .items[?(@.spec.pool.path==\\"%s\\")]}{.metadata.name}{\\"\\t\\"}{.metadata.labels.host}{\\"\\t\\"}{.spec.pool.path}{\\"\\n\\"}{end}"' % path)
    pools = []
    if output:
        for line in output.splitlines():
            pool = {}
            if len(line.split()) < 3:
                continue
            pool['pool'] = line.split()[0]
            pool['host'] = line.split()[1]
            pools.append(pool)
    return pools

def get_pools_by_poolname(poolname):
    output = runCmdAndGetOutput(
        'kubectl get vmp -o=jsonpath="{range .items[?(@.spec.pool.poolname==\\"%s\\")]}{.metadata.name}{\\"\\t\\"}{.metadata.labels.host}{\\"\\t\\"}{.spec.pool.path}{\\"\\n\\"}{end}"' % poolname)
    pools = []
    if output:
        for line in output.splitlines():
            pool = {}
            if len(line.split()) < 3:
                continue
            pool['pool'] = line.split()[0]
            pool['host'] = line.split()[1]
            pools.append(pool)
    return pools

def get_all_node_ip():
    all_node_ip = []
    try:
        jsondict = client.CoreV1Api().list_node().to_dict()
        nodes = jsondict['items']
        for node in nodes:
            node_ip = {}
            for address in node['status']['addresses']:
                if address['type'] == 'InternalIP':
                    node_ip['ip'] = address['address']
                    break
            node_ip['nodeName'] = node['metadata']['name']
            all_node_ip.append(node_ip)

    except ApiException as e:
        logger.debug("Exception when calling CoreV1Api->list_node: %s\n" % e)
    except Exception as e:
        logger.debug("Exception when calling get_all_node_ip: %s\n" % e)

    return all_node_ip

def get_spec(jsondict):
    spec = jsondict.get('spec')
    if not spec:
        raw_object = jsondict.get('raw_object')
        if raw_object:
            spec = raw_object.get('spec')
    return spec

# get disk and snapshot jsondict and change to targetPool
# def get_migrate_disk_jsondict(disk, targetPool):
#     jsondicts = []
#     # two case: 1. pool has same path 2. pool has different path
#     pool_helper = K8sHelper('VirtualMahcinePool')
#     pool_metadata = pool_helper.get(targetPool)['metadata']
#     pool_info = pool_helper.get_data(targetPool, 'pool')
#
#     # get disk jsondict
#     disk_helper = K8sHelper('VirtualMachineDisk')
#     disk_info = disk_helper.get_data(disk, 'volume')
#     disk_jsondict = disk_helper.get(disk)
#     if disk_info['poolname'] == pool_info['poolname']:  # same poolname
#         if disk_jsondict:
#             disk_jsondict['metadata']['labels']['host'] = pool_metadata['labels']['host']
#             spec = get_spec(disk_jsondict)
#             if spec:
#                 nodeName = spec.get('nodeName')
#                 if nodeName:
#                     spec['nodeName'] = pool_metadata['labels']['host']
#                 disk_info['pool'] = targetPool
#                 disk_info["poolname"] = pool_info['poolname']
#                 spec['volume'] = disk_info
#                 jsondicts.append(disk_jsondict)
#         ss_helper = K8sHelper('VirtualMachineDiskSnapshot')
#         ss_dir = '%s/%s/snapshots' % (pool_info['path'], disk)
#         for ss in os.listdir(ss_dir):
#             try:
#                 ss_jsondict = ss_helper.get(ss)
#                 if ss_jsondict:
#                     ss_jsondict['metadata']['labels']['host'] = pool_metadata['labels']['host']
#                     spec = get_spec(ss_jsondict)
#                     if spec:
#                         nodeName = spec.get('nodeName')
#                         if nodeName:
#                             spec['nodeName'] = pool_metadata['labels']['host']
#                         disk_info['pool'] = targetPool
#                         disk_info["poolname"] = pool_info['poolname']
#                         spec['volume'] = disk_info
#                         jsondicts.append(ss_jsondict)
#             except ExecuteException:
#                 pass
#
#     else:  #different poolname
#         pass
#
#
#     return jsondicts

def get_disk_jsondict(pool, disk):
    jsondicts = []
    pool_helper = K8sHelper('VirtualMahcinePool')
    pool_jsondict = pool_helper.get(pool)
    pool_node_name = pool_jsondict['metadata']['labels']['host']
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)

    # get disk jsondict
    disk_helper = K8sHelper('VirtualMachineDisk')
    # if pool_info['pooltype'] not in ['localfs', 'nfs', 'glusterfs', "vdiskfs"]:
    #     raise ExecuteException("RunCmdError", "not support pool type %s" % pool_info['pooltype'])

    if disk_helper.exist(disk):  # migrate disk or migrate vm
        if pool_info['pooltype'] in ['localfs', 'nfs', 'glusterfs', 'vdiskfs']:
            disk_jsondict = disk_helper.get(disk)
            # update disk jsondict
            logger.debug(disk_jsondict)
            disk_jsondict['metadata']['labels']['host'] = pool_node_name

            spec = get_spec(disk_jsondict)
            logger.debug(disk_jsondict)
            if spec:
                nodeName = spec.get('nodeName')
                if nodeName:
                    spec['nodeName'] = pool_node_name
                disk_info = get_disk_info_to_k8s(pool_info['poolname'], disk)
                spec['volume'] = disk_info
                logger.debug(disk_jsondict)
                jsondicts.append(disk_jsondict)
            # update snapshot jsondict
            ss_helper = K8sHelper('VirtualMachineDiskSnapshot')
            ss_dir = '%s/%s/snapshots' % (pool_info['path'], disk)
            if os.path.exists(ss_dir):
                for ss in os.listdir(ss_dir):
                    try:
                        ss_jsondict = ss_helper.get(ss)

                        if ss_jsondict and ss_helper.get_data(ss, 'volume')['disk'] == disk:
                            ss_jsondict['metadata']['labels']['host'] = pool_node_name
                            spec = get_spec(ss_jsondict)
                            if spec:
                                nodeName = spec.get('nodeName')
                                if nodeName:
                                    spec['nodeName'] = pool_node_name
                                ss_info = get_snapshot_info_to_k8s(pool_info['poolname'], disk, ss)
                                spec['volume'] = ss_info
                                jsondicts.append(ss_jsondict)
                    except ExecuteException:
                        pass
        else:
            disk_jsondict = disk_helper.get(disk)
            # update disk jsondict
            logger.debug(disk_jsondict)
            disk_jsondict['metadata']['labels']['host'] = pool_node_name

            spec = get_spec(disk_jsondict)
            logger.debug(disk_jsondict)
            if spec:
                nodeName = spec.get('nodeName')
                if nodeName:
                    spec['nodeName'] = pool_node_name
                disk_info = get_cstor_disk_info_to_k8s(pool, pool_info['poolname'], disk)
                spec['volume'] = disk_info
                logger.debug(disk_jsondict)
                jsondicts.append(disk_jsondict)
    else:  # clone disk
        disk_info = get_disk_info_to_k8s(pool_info['poolname'], disk)
        disk_jsondict = disk_helper.get_create_jsondict(disk, 'volume', disk_info)
        jsondicts.append(disk_jsondict)

        # ss_helper = K8sHelper('VirtualMachineDiskSnapshot')
        # ss_dir = '%s/%s/snapshots' % (pool_info['path'], disk)
        # for ss in os.listdir(ss_dir):
        #     try:
        #         ss_info = get_snapshot_info_to_k8s(pool_info['poolname'], disk, ss)
        #         ss_jsondict = ss_helper.get_create_jsondict(ss)
        #
        #         jsondicts.append(ss_jsondict)
        #     except ExecuteException:
        #         pass

    return jsondicts

def rebase_snapshot_with_config(pool, vol):
    pool_info = get_pool_info_from_k8s(pool)
    check_pool_active(pool_info)
    old_disk_info = get_vol_info_from_k8s(vol)
    old_pool_info = get_pool_info_from_k8s(old_disk_info['pool'])
    check_pool_active(old_pool_info)

    old_disk_dir = '%s/%s' % (old_pool_info['path'], vol)
    disk_dir = '%s/%s' % (pool_info['path'], vol)

    # change config
    old_config = get_disk_config(pool_info['poolname'], vol)
    current = old_config['current'].replace(old_pool_info['path'], pool_info['path'])
    write_config(vol, disk_dir, current, pool, pool_info['poolname'])

    # change backing file
    logger.debug('disk_dir: %s' % disk_dir)
    for ss in os.listdir(disk_dir):
        if ss == 'snapshots' or ss == 'config.json':
            continue
        ss_info = None
        ss_full_path = '%s/%s' % (disk_dir, ss)
        try:
            ss_info = get_disk_info(ss_full_path)
        except ExecuteException:
            pass
        if ss_info:
            if 'backing_filename' in ss_info.keys():
                old_backing_file = ss_info['backing_filename']
                new_backing_file = old_backing_file.replace(old_disk_dir, disk_dir)
                logger.debug('old backing file %s, new backing file %s' % (old_backing_file, new_backing_file))
                if os.path.exists(new_backing_file):
                    runCmd('qemu-img rebase -b %s %s' % (new_backing_file, ss_full_path))
    ss_dir = '%s/snapshots' % disk_dir
    logger.debug('ss_dir: %s' % ss_dir)
    if os.path.exists(ss_dir):
        for ss in os.listdir(ss_dir):
            ss_info = None
            ss_full_path = '%s/%s' % (ss_dir, ss)
            try:
                ss_info = get_disk_info(ss_full_path)
            except ExecuteException:
                pass
            if ss_info:
                if 'backing_filename' in ss_info.keys():
                    old_backing_file = ss_info['backing_filename']
                    new_backing_file = old_backing_file.replace(old_disk_dir, disk_dir)
                    logger.debug('old backing file %s, new backing file %s' % (old_backing_file, new_backing_file))
                    if os.path.exists(new_backing_file):
                        runCmd('qemu-img rebase -u -b %s %s' % (new_backing_file, ss_full_path))
    jsondicts = get_disk_jsondict(pool, vol)

    apply_all_jsondict(jsondicts)

def apply_all_jsondict(jsondicts):
    if len(jsondicts) == 0:
        return
    filename = randomUUID()
    logger.debug(filename)
    with open('/tmp/%s.yaml' % filename, 'w') as f:
        for i in range(len(jsondicts)):
            result = yaml.safe_dump(jsondicts[i])
            f.write(result)
            if i != len(jsondicts) - 1:
                f.write('---\n')
    try:
        runCmd('kubectl apply -f /tmp/%s.yaml' % filename)
    except ExecuteException, e:
        if (e.message == 'Warning: kubectl apply should be used on resource created by either kubectl create --save-config or kubectl apply\n'):
            pass
        else:
            raise e
    try:
        runCmd('rm -f /tmp/%s.yaml' % filename)
    except ExecuteException:
        pass

def create_all_jsondict(jsondicts):
    if len(jsondicts) == 0:
        return
    filename = randomUUID()
    logger.debug(filename)
    with open('/tmp/%s.yaml' % filename, 'w') as f:
        for i in range(len(jsondicts)):
            result = yaml.safe_dump(jsondicts[i])
            f.write(result)
            if i != len(jsondicts) - 1:
                f.write('---\n')
    runCmd('kubectl create -f /tmp/%s.yaml' % filename)
    try:
        runCmd('rm -f /tmp/%s.yaml' % filename)
    except ExecuteException:
        pass

def get_node_ip_by_node_name(nodeName):
    all_node_ip = get_all_node_ip()
    if all_node_ip:
        for ip in all_node_ip:
            if ip['nodeName'] == nodeName:
                return ip['ip']
    return None

def get_node_name_by_node_ip(ip):
    all_node_ip = get_all_node_ip()
    if all_node_ip:
        for node in all_node_ip:
            if node['ip'] == ip and node['nodeName'].find("vm.") >= 0:
                return node['nodeName']
    return None


def get_vm_xml(domain):
    return runCmdAndGetOutput('virsh dumpxml %s' % domain)

def xmlToJson(xmlStr):
    return dumps(bf.data(fromstring(xmlStr)), sort_keys=True, indent=4)

def toKubeJson(json):
    return json.replace('@', '_').replace('$', 'text').replace(
            'interface', '_interface').replace('transient', '_transient').replace(
                    'nested-hv', 'nested_hv').replace('suspend-to-mem', 'suspend_to_mem').replace('suspend-to-disk', 'suspend_to_disk')

def _addListToSpecificField(data):
    if isinstance(data, list):
        return data
    else:
        return [data]

'''
Cautions! Do not modify this function because it uses reflections!
'''
def _userDefinedOperationInList(field, jsondict, alist):
    jsondict = jsondict[field]
    tmp = jsondict
    do_it = False
    for index, value in enumerate(alist):
        if index == 0:
            if value != field:
                break
            continue
        tmp = tmp.get(value)
        if not tmp:
            do_it = False
            break
        do_it = True
    if do_it:
        tmp2 = None
        for index, value in enumerate(alist):
            if index == 0:
                tmp2 = 'jsondict'
            else:
                tmp2 = '{}[\'{}\']'.format(tmp2, value)
        exec('{} = {}').format(tmp2, _addListToSpecificField(tmp))
    return

def updateDomain(jsondict):
    for line in vmArray:
        alist = line.split('-')
        _userDefinedOperationInList('domain', jsondict, alist)
    return jsondict

def modifyVMOnNode(domain):
    helper = K8sHelper('VirtualMachine')
    jsonDict = helper.get(domain)
    vm_xml = get_vm_xml(domain)
    vm_json = toKubeJson(xmlToJson(vm_xml))
    vm_json = updateDomain(loads(vm_json))
    vm_json = updateJsonRemoveLifecycle(jsonDict, vm_json)
    jsonDict = addPowerStatusMessage(vm_json, 'Running', 'The VM is running.')
    helper.updateAll(domain, jsonDict)

# def checkVMDiskFileChanged():
#     p = subprocess.Popen('virt-diff ', shell=True, stdout=subprocess.PIPE)
#     try:
#         while True:
#             output = p.stdout.readline()
#             if output == '' and p.poll() is not None:
#                 break
#             if output:
#                 # print output.strip()
#                 p.terminate()
#     except Exception:
#         traceback.print_exc()
#     finally:
#         p.stdout.close()

def checksum(path, block_size=8192):
    with open(path, "rb") as f:
        file_hash = hashlib.md5()
        while True:
            data = f.read(block_size)
            if not data:
                break
            file_hash.update(data)
        return file_hash.hexdigest()


def backup_snapshots_chain(domain, disk_dir, current, backup_path):
    if not os.path.exists(disk_dir):
        raise ExecuteException('', 'not exist disk dir need to backup: %s' % disk_dir)
    result = {}
    result['current'] = get_disk_info(current)['full_backing_filename']
    backup_files = set()
    image_file = None
    chains = []
    checksums = {}

    # only backup the current chain
    old_current = result['current']
    backup_files.add(old_current)
    # back up disk image
    disk_info = get_disk_info(old_current)
    while 'full_backing_filename' in disk_info.keys():
        backup_files.add(disk_info['full_backing_filename'])
        if disk_info['full_backing_filename'].find(disk_dir) < 0:  # not in disk dir, use image
            try:
                image = os.path.basename(disk_info['full_backing_filename'])
                image_info = get_image_info_from_k8s(image)
                image_file = disk_info['full_backing_filename']
            except:
                pass

        disk_info = get_disk_info(disk_info['full_backing_filename'])

    # backup image
    if image_file:
        image_backup_path = '%s/image' % backup_path
        if not os.path.exists(image_backup_path):
            os.makedirs(image_backup_path)
        image = os.path.basename(image_file)
        if not os.path.exists('%s/%s' % (image_backup_path, image)):
            runCmd('cp -f %s %s' % (image_file, image_backup_path))
        backup_files.remove(image_file)

    # record snapshot chain
    disk_backup_dir = '%s/%s/diskbackup' % (backup_path, domain)
    for bf in backup_files:
        disk_checksum = backup_file(bf, disk_backup_dir)
        checksums[bf] = disk_checksum
    for bf in backup_files:
        disk_info = get_disk_info(bf)
        record = {}
        record['path'] = bf
        record['checksum'] = checksums[bf]
        if 'full_backing_filename' in disk_info.keys():
            record['parent'] = disk_info['full_backing_filename']
            record['parent_checksum'] = checksums[disk_info['full_backing_filename']]
        else:
            record['parent'] = ''
            record['parent_checksum'] = ''
        chains.append(record)

    if image_file:
        result['image'] = os.path.basename(image_file)
        result['image_path'] = image_file
    else:
        result['image'] = ''
        result['image_path'] = ''
    result['chains'] = chains
    return result

def backup_file(file, target_dir):
    # print file
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    file_checksum = checksum(file)
    history_file = '%s/checksum.json' % target_dir
    backupRecord = None
    if not os.path.exists(history_file):
        history = {}
    else:
        with open(history_file, 'r') as f:
            history = load(f)
            if file_checksum in history.keys():
                backupRecord = history[file_checksum]

    if not backupRecord:
        # backup file
        target = '%s/%s' % (target_dir, os.path.basename(file))
        if os.path.exists(target):
            uuid = randomUUID().replace('-', '')
            target = '%s/%s' % (target_dir, uuid)
        runCmd('cp -f %s %s' % (file, target))

        # dump hisory
        history[file_checksum] = os.path.basename(target)
        with open(history_file, 'w') as f:
            dump(history, f)
    return file_checksum

def restore_snapshots_chain(disk_back_dir, backup_disk, target_dir):
    vm_backup_path = os.path.dirname(disk_back_dir)
    backup_path = os.path.dirname(vm_backup_path)
    # disk_back_dir = '%s/diskbackup' % vm_backup_path

    checksum_file = '%s/checksum.json' % disk_back_dir
    with open(checksum_file, 'r') as f:
        checksums = load(f)

    disk_dir = target_dir

    old_to_new = {}

    # cp all file and make a chain
    if backup_disk['image']:
        image_backup_path = '%s/image/%s' % (backup_path, backup_disk['image'])
        if not os.path.exists(image_backup_path):
            raise ExecuteException('', 'can not find image backup file.')

        # check image source can use or not
        image_info = None
        try:
            image_info = get_image_info_from_k8s(backup_disk['image'])
        except:
            pass
        backup_checksum = checksum(image_backup_path)
        if image_info and os.path.exists(image_info['filename']) and checksum(image_info['filename']) == backup_checksum:  # image still can be used
            old_to_new[backup_disk['image_path']] = backup_disk['image_path']
        else:
            if os.path.exists('%s/%s' % (disk_dir, backup_disk['image'])):
                disk_dir_image_file = '%s/%s' % (disk_dir, backup_disk['image'])
                disk_dir_image_checksum = checksum(disk_dir_image_file)
                if disk_dir_image_checksum != backup_checksum:
                    uuid = randomUUID().replace('-', '')
                    new_image_path = '%s/%s' % (disk_dir, uuid)
                    runCmd('cp  %s %s' % (image_backup_path, new_image_path))
                    old_to_new[backup_disk['image_path']] = disk_dir_image_file
                else:
                    old_to_new[backup_disk['image_path']] = image_backup_path
            else:
                new_image_path = '%s/%s' % (disk_dir, backup_disk['image'])
                runCmd('cp  %s %s' % (image_backup_path, new_image_path))
                old_to_new[backup_disk['image_path']] = new_image_path

    for chain in backup_disk['chains']:
        # print chain['path']
        if chain['checksum'] not in checksums.keys():
            raise ExecuteException('', 'can not find disk file backup checksum.')
        backup_file = '%s/%s' % (disk_back_dir, checksums[chain['checksum']])
        if not os.path.exists(backup_file):
            raise ExecuteException('', 'can not find disk backup file %s.' % backup_file)
        if chain['parent']:
            uuid = randomUUID().replace('-', '')
            new_disk_file = '%s/%s' % (disk_dir, uuid)
            runCmd('cp -f %s %s' % (backup_file, new_disk_file))
            old_to_new[chain['path']] = new_disk_file
        else:
            # base image
            if chain['path'].find('snapshots') >= 0:
                base_file = '%s/snapshots/%s' % (disk_dir, os.path.basename(chain['path']))
            else:
                base_file = '%s/%s' % (disk_dir, os.path.basename(chain['path']))
            new_disk_file = '%s/%s' % (disk_dir, os.path.basename(chain['path']))
            if not os.path.exists(base_file):
                runCmd('cp -f %s %s' % (backup_file, new_disk_file))
                old_to_new[chain['path']] = new_disk_file
            else:
                old_to_new[chain['path']] = base_file
    # print dumps(old_to_new)
    # print dumps(backup_disk['chains'])
    # reconnect snapshot chain
    for chain in backup_disk['chains']:
        # print dumps(chain)
        if chain['parent']:
            # parent = '%s/%s' % (disk_dir, os.path.basename(chain['parent']))
            # print 'qemu-img rebase -f qcow2 -b %s %s' % (old_to_new[parent], old_to_new[chain['path']])
            runCmd('qemu-img rebase -f qcow2 -b %s %s' % (old_to_new[chain['parent']], old_to_new[chain['path']]))

    # if this disk has no snapshots, try to delete other file
    ss_dir = '%s/snapshots' % disk_dir
    exist_ss = False
    if os.path.exists(ss_dir):
        for ss in os.listdir(ss_dir):
            try:
                ss_info = get_snapshot_info_from_k8s(ss)
                exist_ss = True
            except:
                pass
    file_to_delete = []
    if not exist_ss:
        for ss in os.listdir(ss_dir):
            ss_file = '%s/%s' % (ss_dir, ss)
            if os.path.isfile(ss_file) and ss_file not in old_to_new.values():
                file_to_delete.append(ss_file)
        for ss in os.listdir(disk_dir):
            if ss == 'config.json':
                continue
            ss_file = '%s/%s' % (disk_dir, ss)
            if os.path.isfile(ss_file) and ss_file not in old_to_new.values():
                file_to_delete.append(ss_file)

    if backup_disk['current'].find('snapshots') >= 0:
        disk_current = '%s/snapshots/%s' % (disk_dir, os.path.basename(backup_disk['current']))
    else:
        disk_current = '%s/%s' % (disk_dir, os.path.basename(backup_disk['current']))
    return old_to_new[disk_current], file_to_delete

def check_pool_active(info):
    pool_helper = K8sHelper('VirtualMahcinePool')
    this_node_name = get_hostname_in_lower_case()
    pool_node_name = get_node_name(pool_helper.get(info['pool']))
    if this_node_name != pool_node_name:
        if info['state'] == 'inactive':
            error_print(221, 'pool %s is not active, please run "startPool" first' % info['pool'])
        else:
            return

    if this_node_name == pool_node_name and info['state'] == 'active':
        try:
            cstor_pool_active(info['poolname'])
            if info['pooltype'] != 'uus' and not is_pool_started(info['poolname']):
                runCmd('virsh pool-start %s' % info['poolname'])
        except ExecuteException, e:
            error_print(221, e.message)

    cstor = get_cstor_pool_info(info['poolname'])

    if info['pooltype'] == 'uus':
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
        if is_pool_started(info['poolname']) and cstor['data']['status'] == 'active':
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
        error_print(221, 'pool %s is not active, please run "startPool" first' % info['pool'])

def change_k8s_pool_state(pool, state):
    helper = K8sHelper("VirtualMahcinePool")
    pool_info = helper.get_data(pool, "pool")
    pool_info['state'] = state
    helper.update(pool, 'pool', pool_info)

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

if __name__ == '__main__':
    print get_all_node_ip()
    # print checksum('/var/lib/libvirt/cstor/a639873f92a24a9ab840492f0e538f2b/a639873f92a24a9ab840492f0e538f2b/vmbackuptestdisk1/vmbackuptestdisk1')
    # print get_pools_by_node('vm.node25')
    # print get_pool_info_from_k8s('7daed7737ea0480eb078567febda62ea')
    # jsondicts = get_migrate_disk_jsondict('vm006migratedisk1', 'migratepoolnode35')
    # apply_all_jsondict(jsondicts)
    # print remoteRunCmdWithResult('133.133.135.35', 'cstor-cli pool-show --poolname pooldir')
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

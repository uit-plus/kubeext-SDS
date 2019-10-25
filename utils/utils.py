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

import grpc

import cmdcall_pb2
import cmdcall_pb2_grpc
import logger
from exception import ExecuteException
from netutils import get_docker0_IP
from libvirt_util import get_pool_info

LOG = '/var/log/kubesds.log'

logger = logger.set_logger(os.path.basename(__file__), LOG)


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
                raise ExecuteException('RunCmdError', 'can not parse cstor-cli output to json----'+msg+'. '+error_msg)
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
            logger.debug(traceback.format_exc())
            raise ExecuteException('RunCmdError', msg)
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
            for index, line in enumerate(std_out):
                if not str.strip(line):
                    continue
                msg = msg + str.strip(line)
            msg = str.strip(msg)
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
            logger.debug(traceback.format_exc())
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
#             msg = ''
#             for index, line in enumerate(std_err):
#                 if not str.strip(line):
#                     continue
#                 if index == len(std_err) - 1:
#                     msg = msg + str.strip(line) + '. ' + '***More details in %s***' % LOG
#                 else:
#                     msg = msg + str.strip(line) + ', '
            logger.error(std_err)
#             raise ExecuteException('VirtctlError', str.strip(msg))
            raise ExecuteException('VirtctlError', std_err)
#         return (str.strip(std_out[0]) if std_out else '', str.strip(std_err[0]) if std_err else '')
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

host = get_docker0_IP()
port = '19999'

channel = grpc.insecure_channel("{0}:{1}".format(host, port))
client = cmdcall_pb2_grpc.CmdCallStub(channel)

def rpcCall(cmd):
    jsondict = None
    logger.debug(cmd)
    try:
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
        # ideally, you should have try catch block here too
        response = client.CallWithResult(cmdcall_pb2.CallRequest(cmd=cmd))
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

def get_vol_info(vol_path):
    return runCmdWithResult('qemu-img info -U --output json ' + vol_path)

def get_disk_config(pool, vol):
    poolInfo = get_pool_info(pool)
    pool_path = poolInfo['path']
    if not os.path.isdir(pool_path):
        raise ExecuteException('', "can not get pool " + pool + " path.")
    config_path = pool_path + '/' + vol + '/config.json'
    with open(config_path, "r") as f:
        config = load(f)
        return config
    raise ExecuteException('', 'can not get disk config by current')

def get_disk_snapshots(ss_path):
    ss_chain = get_sn_chain(ss_path)
    snapshots = []
    for disk_info in ss_chain:
        if disk_info['filename'] != ss_path:
            snapshots.append(disk_info['filename'])
    return snapshots

def get_sn_chain(ss_path):
    return runCmdWithResult('qemu-img info -U --backing-chain --output json '+ss_path)

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

# print get_all_snapshot_to_delete('/var/lib/libvirt/pooltest/disktest/disktest', '/var/lib/libvirt/pooltest/disktest/ss3')

# print os.path.basename('/var/lib/libvirt/pooltest/disktest/disktest')

# print get_disk_snapshots('/var/lib/libvirt/pooltest/disktest/ss1')
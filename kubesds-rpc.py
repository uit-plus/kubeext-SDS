# coding=utf-8
import os
import socket
import subprocess
import sys
import threading
import time
import traceback
from threading import Thread

import grpc
from json import dumps
from concurrent import futures

from netutils import get_docker0_IP

sys.path.append('%s/' % os.path.dirname(os.path.realpath(__file__)))

from utils import logger
from utils.utils import CDaemon, singleton, runCmdWithResult, runCmdAndCheckReturnCode, runCmdAndGetOutput

import cmdcall_pb2, cmdcall_pb2_grpc  # 刚刚生产的两个文件

LOG = "/var/log/cmdrpc.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

DEFAULT_PORT = '19999'


class Operation(object):
    def __init__(self, cmd, params, with_result=False):
        if cmd is None or cmd == "":
            raise Exception("plz give me right cmd.")
        if not isinstance(params, dict):
            raise Exception("plz give me right parameters.")

        self.params = params
        self.cmd = cmd
        self.params = params
        self.with_result = with_result

    def get_cmd(self):
        cmd = self.cmd
        for key in self.params.keys():
            cmd = cmd + " --" + key + " " + self.params[key] + " "
        return cmd

    def execute(self):
        cmd = self.get_cmd()
        logger.debug(cmd)

        if self.with_result:
            return runCmdWithResult(cmd)
        else:
            return runCmdAndCheckReturnCode(cmd)


class CmdCallServicer(cmdcall_pb2_grpc.CmdCallServicer):

    def Call(self, request, ctx):
        try:
            cmd = str(request.cmd)
            logger.debug(cmd)
            op = Operation(cmd, {})
            op.execute()

            logger.debug(request)
            return cmdcall_pb2.CallResponse(
                json=dumps({'result': {'code': 0, 'msg': 'call cmd ' + cmd + ' successful.'}, 'data': {}}))
        except subprocess.CalledProcessError, e:
            logger.debug(e.output)
            logger.debug(traceback.format_exc())
            return cmdcall_pb2.CallResponse(
                json=dumps({'result': {'code': 1, 'msg': 'call cmd failure ' + e.output}, 'data': {}}))
        except Exception:
            logger.debug(traceback.format_exc())
            return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 1, 'msg': 'call cmd failure'}, 'data': {}}))

    def CallWithResult(self, request, context):
        jsonstr = ''
        try:
            cmd = str(request.cmd)
            logger.debug(cmd)

            op = Operation(cmd, {}, with_result=True)
            result = op.execute()
            logger.debug(request)
            logger.debug(result)
            return cmdcall_pb2.CallResponse(json=dumps(result))
        except Exception:
            logger.debug(traceback.format_exc())
            return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 1, 'msg': 'call cmd failure'}, 'data': {}}))


def run_server():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = CmdCallServicer()
    # 注册本地服务,方法CmdCallServicer只有这个是变的
    cmdcall_pb2_grpc.add_CmdCallServicer_to_server(servicer, server)
    # 监听端口
    print get_docker0_IP() + ':' + DEFAULT_PORT
    logger.debug(get_docker0_IP() + ':' + DEFAULT_PORT)
    server.add_insecure_port(get_docker0_IP() + ':' + DEFAULT_PORT)
    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    try:
        print("rpc server running...")
        time.sleep(1000)
    except KeyboardInterrupt:
        print("rpc server stopping...")
        server.stop(0)


def keep_alive():
    t = threading.Thread(target=run_server)
    t.start()  # 启动一个线程
    while True:
        output = runCmdAndGetOutput('netstat -anp|grep 19999')
        if output is not None and output.find('19999') >= 0:
            # logger.debug("port 19999 is alive")
            pass
        else:
            nt = threading.Thread(target=run_server)
            nt.start()  # 启动一个线程
            logger.debug("restart port 19999")
        time.sleep(1)

class ClientDaemon(CDaemon):
    def __init__(self, name, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022,
                 verbose=1):
        CDaemon.__init__(self, save_path, stdin, stdout, stderr, home_dir, umask, verbose)
        self.name = name

    @singleton('/var/run/cmdrpc.pid')
    def run(self, output_fn, **kwargs):
        logger.debug("---------------------------------------------------------------------------------")
        logger.debug("------------------------Welcome to Virtlet Daemon.-------------------------------")
        logger.debug("------Copyright (2019, ) Institute of Software, Chinese Academy of Sciences------")
        logger.debug("---------author: wuyuewen@otcaix.iscas.ac.cn,liuhe18@otcaix.iscas.ac.cn----------")
        logger.debug("--------------------------------wuheng@otcaix.iscas.ac.cn------------------------")
        logger.debug("---------------------------------------------------------------------------------")

        try:
            # thread_1 = Thread(target=run_server)
            # thread_1.daemon = True
            # thread_1.name = 'run_server'
            # thread_1.start()
            thread_1 = Thread(target=keep_alive)
            thread_1.daemon = True
            thread_1.name = 'keep_alive'
            thread_1.start()
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                return
        except:
            logger.error('Oops! ', exc_info=1)

def daemonize():
    help_msg = 'Usage: python %s <start|stop|restart|status>' % sys.argv[0]
    if len(sys.argv) != 2:
        print help_msg
        sys.exit(1)
    p_name = 'kubesds-rpc'
    pid_fn = '/var/run/kubesds-rpc.pid'
    log_fn = '/var/log/kubesds-rpc.log'
    err_fn = '/var/log/kubesds-rpc.log'
    cD = ClientDaemon(p_name, pid_fn, stderr=err_fn, verbose=1)

    if sys.argv[1] == 'start':
        cD.start(log_fn)
    elif sys.argv[1] == 'stop':
        cD.stop()
    elif sys.argv[1] == 'restart':
        cD.restart(log_fn)
    elif sys.argv[1] == 'status':
        alive = cD.is_running()
        if alive:
            print 'process [%s] is running ......' % cD.get_pid()
        else:
            print 'daemon process [%s] stopped' % cD.name
    else:
        print 'invalid argument!'
        print help_msg


if __name__ == '__main__':
    daemonize()

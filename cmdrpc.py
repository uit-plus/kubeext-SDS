# coding=utf-8
import os
import socket
import subprocess
import sys
import time
import traceback
from threading import Thread

import grpc
from json import dumps
from concurrent import futures

sys.path.append('%s/' % os.path.dirname(os.path.realpath(__file__)))

from operation import Operation
from utils import logger
from utils.utils import CDaemon, singleton, get_IP


import cmdcall_pb2, cmdcall_pb2_grpc  # 刚刚生产的两个文件

LOG = "/var/log/cmdrpc.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

DEFAULT_PORT = '19999'



class CmdCallServicer(cmdcall_pb2_grpc.CmdCallServicer):

    def Call(self, request, ctx):
        try:
            cmd = str(request.cmd)
            logger.debug(cmd)
            op = Operation(cmd, {})
            op.execute()

            logger.debug(request)
            return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 0, 'msg': 'call cmd ' + cmd + ' successful.'}, 'data': {}}))
        except subprocess.CalledProcessError as e:
            logger.debug(e.output)
            logger.debug(traceback.format_exc())
            return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 1, 'msg': 'call cmd failure '+e.output}, 'data': {}}))
        except Exception:
            logger.debug(traceback.format_exc())
        return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 1, 'msg': 'call cmd failure'}, 'data': {}}))


    def CallWithResult(self, request, context):
        jsonstr= ''
        try:
            cmd = str(request.cmd)
            logger.debug(cmd)

            op = Operation(cmd, {}, with_result=True)
            result = op.execute()
            logger.debug(request)
            return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 0, 'msg': 'call cmd ' + cmd + ' successful.'}, 'data': result}))
        except Exception:
            logger.debug(traceback.format_exc())

        return cmdcall_pb2.CallResponse(json=dumps({'result': {'code': 1, 'msg': 'call cmd failure'}, 'data': {}}))


def run_server():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = CmdCallServicer()
    # 注册本地服务,方法ComputeServicer只有这个是变的
    cmdcall_pb2_grpc.add_CmdCallServicer_to_server(servicer, server)
    # 监听端口
    print get_IP()+':'+DEFAULT_PORT
    logger.debug(get_IP()+':'+DEFAULT_PORT)
    server.add_insecure_port(get_IP()+':'+DEFAULT_PORT)
    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    try:
        print("rpc server running...")
        time.sleep(1000)
    except KeyboardInterrupt:
        print("rpc server stopping...")
        server.stop(0)


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
            thread_1 = Thread(target=run_server)
            thread_1.daemon = True
            thread_1.name = 'run_server'
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
    p_name = 'virtlet'
    pid_fn = '/var/run/cmdrpc.pid'
    log_fn = '/var/log/cmdrpc.log'
    err_fn = '/var/log/cmdrpc.log'
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
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

from netutils import get_docker0_IP

sys.path.append('%s/' % os.path.dirname(os.path.realpath(__file__)))

from utils import logger
from utils.utils import CDaemon, singleton, runCmdWithResult, runCmd

import cmdcall_pb2, cmdcall_pb2_grpc  # 刚刚生产的两个文件

LOG = "/var/log/kubesds-rpc.log"

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
            return runCmd(cmd)


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
    # 注册本地服务,方法ComputeServicer只有这个是变的
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




if __name__ == '__main__':
    run_server()

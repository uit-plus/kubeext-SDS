# coding=utf-8
import time
import grpc
from concurrent import futures

import cmdcall_pb2, cmdcall_pb2_grpc  # 刚刚生产的两个文件


class CmdCallServicer(cmdcall_pb2_grpc.CmdCallServicer):
    def SayHello(self, request, ctx):
        max_len = str(len(request.helloworld))
        return cmdcall_pb2.CallResponse(result=max_len)


def main():
    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 实例化 计算len的类
    servicer = CmdCallServicer()
    # 注册本地服务,方法ComputeServicer只有这个是变的
    cmdcall_pb2_grpc.add_CmdCallServicer_to_server(servicer, server)
    # 监听端口
    server.add_insecure_port('127.0.0.1:19999')
    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    try:
        print("running...")
        time.sleep(1000)
    except KeyboardInterrupt:
        print("stopping...")
        server.stop(0)


if __name__ == '__main__':
    main()

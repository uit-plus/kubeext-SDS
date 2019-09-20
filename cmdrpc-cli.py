import os
import traceback

import grpc
import argparse

from utils import logger

import cmdcall_pb2
import cmdcall_pb2_grpc


LOG = "/var/log/cmdrpc.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)



parser = argparse.ArgumentParser(prog="kubeovs-adm", description="All storage adaptation tools")

parser.add_argument('host', help='host help')
parser.add_argument('port', help='port help')
parser.add_argument('cmd', help='cmd help')
parser.set_defaults()



def call_rpc(host, port, cmd):
    with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
        client = cmdcall_pb2_grpc.CmdCallStub(channel=channel)
        response = client.Call(cmdcall_pb2.CallRequest(cmd=cmd))
    logger.debug("received: " + response.json)
    return response.json


try:
    args = parser.parse_args()
    args.func(args)
    print call_rpc(args.host, args.port, args.cmd)
except TypeError:
    # print "argument number not enough"
    logger.debug(traceback.format_exc())

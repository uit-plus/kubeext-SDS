import os
import traceback

import grpc
import argparse

from utils import logger

import cmdcall_pb2
import cmdcall_pb2_grpc
from utils.utils import get_IP

LOG = "/var/log/cmdrpc.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

host = get_IP()
port = '19999'

cmd = 'virsh pool-create-as --type dir  --name pooldir2  --target /var/lib/libvirt/pooldir2'
with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
    client = cmdcall_pb2_grpc.CmdCallStub(channel=channel)
    response = client.Call(cmdcall_pb2.CallRequest(cmd=cmd))
    logger.debug("received: " + response.json)
    print response.json



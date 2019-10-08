import os

import grpc

from netutils import get_docker0_IP
from utils import logger

import cmdcall_pb2
import cmdcall_pb2_grpc

LOG = "/var/log/kubesds-rpc.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

host = get_docker0_IP()
port = '19999'

# cmd = 'virsh pool-define-as --type dir  --name pooldir2  --target /var/lib/libvirt/pooldir2'
# with grpc.insecure_channel("{0}:{1}".format(host, port)) as channel:
#     client = cmdcall_pb2_grpc.CmdCallStub(channel=channel)
#     response = client.Call(cmdcall_pb2.CallRequest(cmd=cmd))
#     logger.debug("received: " + response.json)
#     print response.json

channel = grpc.insecure_channel("{0}:{1}".format(host, port))
stub = cmdcall_pb2_grpc.CmdCallStub(channel)



cmd = 'virsh pool-define-as --type dir  --name pooldir2  --target /var/lib/libvirt/pooldir2'

try:
    # ideally, you should have try catch block here too
    response = stub.Call(cmdcall_pb2.CallRequest(cmd=cmd))
except grpc.RpcError, e:
    # ouch!
    # lets print the gRPC error message
    # which is "Length of `Name` cannot be more than 10 characters"
    print(e.details())
    # lets access the error code, which is `INVALID_ARGUMENT`
    # `type` of `status_code` is `grpc.StatusCode`
    status_code = e.code()
    # should print `INVALID_ARGUMENT`
    print(status_code.name)
    # should print `(3, 'invalid argument')`
    print(status_code.value)
    # want to do some specific action based on the error?
    if grpc.StatusCode.INVALID_ARGUMENT == status_code:
        # do your stuff here
        pass
else:
    logger.debug("received: " + response.json)
    print(response.json)

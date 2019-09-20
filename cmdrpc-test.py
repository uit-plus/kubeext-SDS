import grpc
import cmdcall_pb2
import cmdcall_pb2_grpc

_HOST = '127.0.0.1'
_PORT = '19999'


def main():
    with grpc.insecure_channel("{0}:{1}".format(_HOST, _PORT)) as channel:
        client = cmdcall_pb2_grpc.add_CmdCallServicer_to_server(channel=channel)
        response = client.SayHello(cmdcall_pb2.CallRequest(helloworld="123456"))
    print("received: " + response.result)


if __name__ == '__main__':
    main()

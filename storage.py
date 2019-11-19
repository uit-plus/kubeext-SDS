import os

from utils.exception import ExecuteException
from utils.libvirt_util import list_pools, list_defined_pools
from utils.utils import rpcCallAndTransferKvToJson, rpcCallAndTransferXmlToJson, rpcCallWithResult, rpcCall
from utils import logger

LOG = "/var/log/kubesds.log"

logger = logger.set_logger(os.path.basename(__file__), LOG)

class Operation(object):
    def __init__(self, cmd, params, with_result=False, xml_to_json=False, kv_to_json=False):
        if cmd is None or cmd == "":
            raise Exception("plz give me right cmd.")
        if not isinstance(params, dict):
            raise Exception("plz give me right parameters.")

        self.params = params
        self.cmd = cmd
        self.params = params
        self.with_result = with_result
        self.xml_to_json = xml_to_json
        self.kv_to_json = kv_to_json

    def get_cmd(self):
        cmd = self.cmd
        for key in self.params.keys():
            cmd = cmd + " --" + key + " " + self.params[key] + " "
        return cmd

    def execute(self):
        cmd = self.get_cmd()
        logger.debug(cmd)

        if self.with_result:
            return rpcCallWithResult(cmd)
        elif self.xml_to_json:
            return rpcCallAndTransferXmlToJson(cmd)
        elif self.kv_to_json:
            return rpcCallAndTransferKvToJson(cmd)
        else:
            return rpcCall(cmd)


class Pool(object):
    def __init__(self, name, args):
        self.name = name
        self.args = args
        self.proto = None
        self.mount_path = None
        self.path = None

    def is_virsh_active(self):
        if self.name in list_pools():
            return True
        return False

    def is_virsh_defined(self):
        if self.name in list_defined_pools():
            return True
        return False

    def is_virsh_exist(self):
        if self.is_virsh_active() or self.is_virsh_defined():
            return True
        return False

    def is_cstor_exist(self):
        try:
            self.cstor_info()
        except ExecuteException:
            return False
        return True

    def cstor_info(self):
        op = Operation("cstor-cli pool-show", {"poolname": self.name}, with_result=True)
        result = op.execute()
        if result["result"]["code"] != 0:
            raise ExecuteException('Cstor Error', 'cant get cstor pool info')
        return result

    def virsh_info(self):
        info = rpcCallAndTransferKvToJson('virsh pool-info %s' % self.name)
        # info['allocation'] = int(1024*1024*1024*float(info['allocation']))
        # info['available'] = int(1024 * 1024 * 1024 * float(info['available']))
        # info['capacity'] = int(1024 * 1024 * 1024 * float(info['capacity']))
        if 'allocation' in info.keys():
            del info['allocation']
        if 'available' in info.keys():
            del info['available']

        xml_dict = rpcCallAndTransferXmlToJson('virsh pool-dumpxml %s' % self.name)
        info['capacity'] = int(xml_dict['pool']['capacity']['text'])
        info['path'] = xml_dict['pool']['target']['path']
        return info

    def check_virsh_pool_exist(self):
        if self.is_virsh_exist():
            return True
        return False

    def check_cstor_pool_exist(self):
        if self.is_cstor_exist():
            return True
        return False

    def check_args(self):
        pass

    def check_precondition(self):
        pass

    def check(self):
        self.check_args()
        self.check_precondition()

    def operation_chain_executor(self):
        self.check()

    def create_localfs_pool(self):
        op = Operation('cstor-cli pooladd-localfs ', {'poolname': self.args.pool,
                                                      'url': self.args.url}, with_result=True)
        info = op.execute()

        self.mount_path = info['data']['mountpath']
        self.path = '%s/%s' % (self.mount_path, self.args.pool)
        if not os.path.isdir(self.path):
            raise ExecuteException('', 'cant not get pooladd-localfs mount path')




class Volume(object):
    def __init__(self, pool, name):
        self.pool = pool
        self.name = name

    def info(self):
        pass

class Snapshot(object):
    def __init__(self, pool, volume, name):
        self.pool = pool
        self.volume = volume
        self.name = name

    def info(self):
        pass


def success(self, code, msg, data):
    print {"result": {"code": code, "msg": msg}, "data": data}
    exit(0)


def error(self, code, msg):
    print {"result": {"code": code, "msg": msg}, "data": {}}
    exit(3)


def less_arg(self, arg):
    print {"result": {"code": 100, "msg": 'error: arg %s must set.' % arg}, "data": {}}
    exit(3)

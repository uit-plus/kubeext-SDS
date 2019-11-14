class Pool(object):
    def __init__(self, name):
        self.name = name

    def xml(self):
        xml_dict = rpcCallAndTransferXmlToJson('virsh pool-dumpxml %s' % self.name)
        xml_dict['capacity'] = int(xml_dict['pool']['capacity']['text'])
        xml_dict['path'] = xml_dict['pool']['target']['path']
        return xml_dict

    def info(self):
        info = rpcCallAndTransferKvToJson('virsh pool-info %s' % self.name)
        if 'allocation' in info.keys():
            del info['allocation']
        if 'available' in info.keys():
            del info['available']
        return info


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

class OutputHelper(object):
    def success(self, code, msg, data):
        print {"result": {"code": code, "msg": msg}, "data": data}
        exit(0)

    def error(self, code, msg, data):
        print {"result": {"code": code, "msg": msg}, "data": data}
        exit(3)